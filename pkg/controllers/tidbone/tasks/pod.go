// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"k8s.io/client-go/tools/record"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/features"
	"github.com/pingcap/tidb-operator/v2/pkg/image"
	"github.com/pingcap/tidb-operator/v2/pkg/overlay"
	pdapi "github.com/pingcap/tidb-operator/v2/pkg/pdapi/v1"
	"github.com/pingcap/tidb-operator/v2/pkg/reloadable"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	pdv1 "github.com/pingcap/tidb-operator/v2/pkg/timanager/apis/pd/v1"
	pdm "github.com/pingcap/tidb-operator/v2/pkg/timanager/pd"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/k8s"
	maputil "github.com/pingcap/tidb-operator/v2/pkg/utils/map"
	stringutil "github.com/pingcap/tidb-operator/v2/pkg/utils/string"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

const (
	MinGracePeriodSeconds = 30
	// Assume that approximately 200 regions are transferred for 1s
	RegionsPerSecond = 200

	metricsPath = "/metrics"
)

func TaskSuspendPod(state *ReconcileContext, c client.Client) task.Task {
	return task.NameTaskFunc("PodSuspend", func(ctx context.Context) task.Result {
		pod := state.Pod()
		if pod == nil {
			return task.Complete().With("pod has been deleted")
		}
		regionCount := 0
		if state.Store != nil {
			regionCount = state.Store.RegionCount
		}
		if err := DeletePodWithGracePeriod(ctx, c, pod, regionCount); err != nil {
			return task.Fail().With("can't delete pod of tidb-one: %v", err)
		}
		state.DeletePod(pod)

		return task.Wait().With("pod is deleting")
	})
}

func TaskPod(state *ReconcileContext, c client.Client, cm pdm.PDClientManager, recorder ...record.EventRecorder) task.Task {
	return task.NameTaskFunc("Pod", func(ctx context.Context) task.Result {
		logger := logr.FromContextOrDiscard(ctx)
		expected := newPod(state.Cluster(), state.TiDBOne(), state.Store, state.FeatureGates())

		pod := state.Pod()
		if pod == nil {
			if err := c.Apply(ctx, expected); err != nil {
				k8s.RecordFailedCreatePod(recorder, state.Object(), err)
				return task.Fail().With("can't apply pod of tidb-one: %v", err)
			}

			state.SetPod(expected)
			return task.Complete().With("pod is created")
		}

		// Now we cannot minimize the deletion grace period because of a bug
		// See https://github.com/kubernetes/kubernetes/issues/83916.
		// TODO(liubo02): support minimize the deletion grace period seconds
		if !pod.GetDeletionTimestamp().IsZero() {
			state.ShouldEvictLeader = true
			// key will be requeued after the pod is changed
			return task.Wait().With("pod is deleting")
		}

		if !reloadable.CheckTiDBOnePod(state.TiDBOne(), pod) {
			state.ShouldEvictLeader = true
			state.PodNeedsRestart = true

			pc, ok := state.GetPDClient(cm)
			if !ok {
				return task.Wait().With("wait if pd client is not registered")
			}

			if err := checkDownPeerCountIsZero(ctx, state.Store, pc); err != nil {
				return task.Retry(defaultTaskWaitDuration).With("cannot recreate pod, check down peer: %v", err)
			}

			if err := CheckTiKVLeadersEvicted(state.TiDBOne()); err != nil {
				return task.Retry(defaultTaskWaitDuration).With("cannot recreate pod, check leader count: %v", err)
			}
			if remaining := cacheTTLRemaining(state.TiDBOne(), time.Now(), v1alpha1.TiKVCondLeadersEvicted); remaining > 0 {
				return task.Retry(remaining).With("cannot recreate pod, wait for TiDB region cache to refresh")
			}
			if err := CheckRoutersDrained(state.TiDBOne()); err != nil {
				return task.Retry(time.Second).With("cannot recreate pod: %v", err)
			}

			logger.Info("will recreate the pod")
			regionCount := 0
			if state.Store != nil {
				regionCount = state.Store.RegionCount
			}
			if err := DeletePodWithGracePeriod(ctx, c, pod, regionCount); err != nil {
				return task.Fail().With("can't minimize the deletion grace period of pod of tidb-one: %v", err)
			}

			state.DeletePod(pod)
			return task.Wait().With("pod is deleting")
		}

		logger.Info("will update the pod in place")
		if err := c.Apply(ctx, expected); err != nil {
			return task.Fail().With("can't apply pod of tidb-one: %v", err)
		}

		// write apply result back to ctx
		state.SetPod(expected)

		return task.Complete().With("pod is synced")
	})
}

func checkDownPeerCountIsZero(ctx context.Context, store *pdv1.Store, pc pdm.PDClient) error {
	downPeerInfo, err := pc.Underlay().GetDownPeerRegions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get down peer info: %w", err)
	}
	downPeerCount := countNonSelfDownPeers(downPeerInfo, store)
	if downPeerCount != 0 {
		return fmt.Errorf("down peer is not zero: %v", downPeerCount)
	}

	return nil
}

func countNonSelfDownPeers(downPeerInfo *pdapi.RegionsCheckInfo, store *pdv1.Store) int {
	if downPeerInfo == nil {
		return 0
	}
	if store == nil || store.ID == "" {
		return downPeerInfo.Count
	}
	remainingDownPeerCount := downPeerInfo.Count
	for _, region := range downPeerInfo.Regions {
		if region == nil || len(region.DownPeers) == 0 {
			continue
		}
		for _, downPeer := range region.DownPeers {
			if downPeer == nil || downPeer.Peer == nil {
				continue
			}
			if fmt.Sprint(downPeer.Peer.StoreId) == store.ID {
				remainingDownPeerCount--
			}
		}
	}

	if remainingDownPeerCount < 0 {
		return 0
	}
	return remainingDownPeerCount
}

func newPod(cluster *v1alpha1.Cluster, one *v1alpha1.TiDBOne, store *pdv1.Store, _ features.Gates) *corev1.Pod {
	vols := []corev1.Volume{
		{
			Name: v1alpha1.VolumeNameConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: coreutil.PodName[scope.TiDBOne](one),
					},
				},
			},
		},
	}
	mounts := []corev1.VolumeMount{
		{
			Name:      v1alpha1.VolumeNameConfig,
			MountPath: v1alpha1.DirPathConfigTiDBOne,
		},
	}

	dataDir := v1alpha1.VolumeMountTiDBOneDataDefaultPath
	for i := range one.Spec.Volumes {
		vol := &one.Spec.Volumes[i]
		name := VolumeName(vol.Name)
		vols = append(vols, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: coreutil.PersistentVolumeClaimName[scope.TiDBOne](one, vol.Name),
				},
			},
		})
		for i := range vol.Mounts {
			mount := VolumeMount(name, &vol.Mounts[i])
			mounts = append(mounts, *mount)
			if vol.Mounts[i].Type == v1alpha1.VolumeMountTypeTiDBOneData {
				dataDir = mount.MountPath
			}
		}
	}

	clientPort := coreutil.TiDBOneClientPort(one)
	statusPort := coreutil.TiDBOneStatusPort(one)
	kvPort := coreutil.TiDBOneTiKVClientPort(one)
	kvStatusPort := coreutil.TiDBOneTiKVStatusPort(one)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: one.Namespace,
			Name:      coreutil.PodName[scope.TiDBOne](one),
			Labels: maputil.Merge(
				coreutil.PodLabels[scope.TiDBOne](one),
				k8s.LabelsK8sApp(cluster.Name, v1alpha1.LabelValComponentTiDBOne),
			),
			// The TiDB status port is the main scrape target; the embedded
			// TiKV is scraped through "tikv.prometheus.io/port".
			Annotations: maputil.Merge(
				k8s.AnnoProm(statusPort, metricsPath),
				k8s.AnnoAdditionalProm("tikv", kvStatusPort),
			),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(one, v1alpha1.SchemeGroupVersion.WithKind("TiDBOne")),
			},
		},
		Spec: corev1.PodSpec{
			//nolint:mnd // same as TiKV: the grace period is set when the pod is deleted
			TerminationGracePeriodSeconds: ptr.To[int64](65535),
			Hostname:                      coreutil.PodName[scope.TiDBOne](one),
			Subdomain:                     one.Spec.Subdomain,
			NodeSelector:                  one.Spec.Topology,
			Containers: []corev1.Container{
				{
					Name:            v1alpha1.ContainerNameTiDBOne,
					Image:           image.TiDBOne.Image(one.Spec.Image, one.Spec.Version),
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         serverCommand(cluster, one, dataDir),
					Ports: []corev1.ContainerPort{
						{Name: v1alpha1.TiDBOnePortNameClient, ContainerPort: clientPort},
						{Name: v1alpha1.TiDBOnePortNameStatus, ContainerPort: statusPort},
						{Name: v1alpha1.TiDBOnePortNameTiKVClient, ContainerPort: kvPort},
						{Name: v1alpha1.TiDBOnePortNameTiKVStatus, ContainerPort: kvStatusPort},
					},
					// TiDB answers /status with 200 once it serves SQL, and
					// with 500 during graceful shutdown.
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/status",
								Port: intstr.FromInt32(statusPort),
							},
						},
						InitialDelaySeconds: readinessInitialDelaySeconds,
						PeriodSeconds:       readinessPeriodSeconds,
					},
					VolumeMounts: mounts,
					Resources:    k8s.GetResourceRequirements(one.Spec.Resources),
				},
			},
			Volumes: vols,
		},
	}

	if cluster.Status.ID != "" {
		pod.Labels[v1alpha1.LabelKeyClusterID] = cluster.Status.ID
	}
	if store != nil && store.ID != "" {
		pod.Labels[v1alpha1.LabelKeyStoreID] = store.ID
	}

	if one.Spec.Overlay != nil {
		overlay.OverlayPod(pod, one.Spec.Overlay.Pod)
	}

	reloadable.MustEncodeLastTiDBOneTemplate(one, pod)
	return pod
}

const (
	readinessInitialDelaySeconds = 5
	readinessPeriodSeconds       = 5
)

// sqlHost is the host TiDB advertises in /info/all, which the routers use as
// the backend address.
func sqlHost(cluster *v1alpha1.Cluster, one *v1alpha1.TiDBOne) string {
	host := coreutil.InstanceHost[scope.TiDBOne](cluster, one)
	// Same as TiDB: keep the ".svc" suffix for the in-cluster DNS mode.
	if cluster.Spec.DNS == nil || cluster.Spec.DNS.Mode == "" || cluster.Spec.DNS.Mode == v1alpha1.DNSModeInCluster {
		return host + ".svc"
	}
	return host
}

// SQLAddr is the backend address of the server on the routers.
func SQLAddr(cluster *v1alpha1.Cluster, one *v1alpha1.TiDBOne) string {
	return fmt.Sprintf("%s:%d", sqlHost(cluster, one), coreutil.TiDBOneClientPort(one))
}

// serverCommand starts tidb-one-server. The operator managed settings are
// flags, which take precedence over the config files.
func serverCommand(cluster *v1alpha1.Cluster, one *v1alpha1.TiDBOne, dataDir string) []string {
	kvPort := coreutil.TiDBOneTiKVClientPort(one)
	kvStatusPort := coreutil.TiDBOneTiKVStatusPort(one)
	return []string{
		"/tidb-one-server",
		"-P", fmt.Sprint(coreutil.TiDBOneClientPort(one)),
		"--status", fmt.Sprint(coreutil.TiDBOneStatusPort(one)),
		"--host", "::",
		"--advertise-address", sqlHost(cluster, one),
		"--store", "tikv",
		"--path", stringutil.RemoveHTTPPrefix(cluster.Status.PD),
		"--config", filepath.Join(v1alpha1.DirPathConfigTiDBOne, v1alpha1.FileNameConfig),
		"--tikv.addr", coreutil.ListenAddress(kvPort),
		"--tikv.advertise-addr", coreutil.InstanceAdvertiseAddress[scope.TiDBOne](cluster, one, kvPort),
		"--tikv.status-addr", coreutil.ListenAddress(kvStatusPort),
		"--tikv.advertise-status-addr", coreutil.InstanceAdvertiseAddress[scope.TiDBOne](cluster, one, kvStatusPort),
		"--tikv.config", filepath.Join(v1alpha1.DirPathConfigTiDBOne, v1alpha1.FileNameConfigTiDBOneTiKV),
		"--tikv.data-dir", dataDir,
	}
}
