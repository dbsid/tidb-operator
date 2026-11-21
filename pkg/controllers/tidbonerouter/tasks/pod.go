// Copyright 2026 PingCAP, Inc.
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

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/image"
	"github.com/pingcap/tidb-operator/v2/pkg/overlay"
	"github.com/pingcap/tidb-operator/v2/pkg/reloadable"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/k8s"
	maputil "github.com/pingcap/tidb-operator/v2/pkg/utils/map"
	stringutil "github.com/pingcap/tidb-operator/v2/pkg/utils/string"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

const (
	metricsPath = "/metrics"
	// gracePeriodSeconds: the router closes its client connections on exit.
	gracePeriodSeconds = 30

	readinessInitialDelaySeconds = 2
	readinessPeriodSeconds       = 5
)

func TaskPod(state *ReconcileContext, c client.Client, recorder ...record.EventRecorder) task.Task {
	return task.NameTaskFunc("Pod", func(ctx context.Context) task.Result {
		logger := logr.FromContextOrDiscard(ctx)
		r := state.TiDBOneRouter()
		expected := newPod(state.Cluster(), r)
		pod := state.Pod()
		if pod == nil {
			if err := c.Apply(ctx, expected); err != nil {
				k8s.RecordFailedCreatePod(recorder, state.Object(), err)
				return task.Fail().With("can't apply pod of tidb-one-router: %v", err)
			}
			state.SetPod(expected)
			return task.Complete().With("pod is created")
		}
		if !pod.GetDeletionTimestamp().IsZero() {
			return task.Wait().With("pod is deleting")
		}
		if !reloadable.CheckTiDBOneRouterPod(r, pod) {
			logger.Info("will recreate the pod")
			if err := c.Delete(ctx, pod, client.GracePeriodSeconds(gracePeriodSeconds)); err != nil {
				return task.Fail().With("can't delete pod of tidb-one-router: %v", err)
			}
			state.DeletePod(pod)
			return task.Wait().With("pod is deleting")
		}
		if err := c.Apply(ctx, expected); err != nil {
			return task.Fail().With("can't apply pod of tidb-one-router: %v", err)
		}
		state.SetPod(expected)
		return task.Complete().With("pod is synced")
	})
}

// SeedAddr is the TiDB status service a router reads /info/all from at
// start; it is created by the router group.
func SeedAddr(r *v1alpha1.TiDBOneRouter) string {
	return fmt.Sprintf("%s-%s-seed.%s.svc:%d",
		r.Labels[v1alpha1.LabelKeyGroup], v1alpha1.LabelValComponentTiDBOneRouter, r.Namespace, v1alpha1.DefaultTiDBOnePortStatus)
}

func newPod(cluster *v1alpha1.Cluster, r *v1alpha1.TiDBOneRouter) *corev1.Pod {
	clientPort := coreutil.TiDBOneRouterClientPort(r)
	statusPort := coreutil.TiDBOneRouterStatusPort(r)
	args := []string{
		"/tiproxy-rs", "locality-router",
		"--listen", fmt.Sprintf("0.0.0.0:%d", clientPort),
		"--status", fmt.Sprintf("0.0.0.0:%d", statusPort),
		"--tidb-status", SeedAddr(r),
		"--pd", stringutil.RemoveHTTPPrefix(cluster.Status.PD),
	}
	args = append(args, r.Spec.Args...)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Namespace,
			Name:      coreutil.PodName[scope.TiDBOneRouter](r),
			Labels: maputil.Merge(
				coreutil.PodLabels[scope.TiDBOneRouter](r),
				k8s.LabelsK8sApp(cluster.Name, v1alpha1.LabelValComponentTiDBOneRouter),
			),
			Annotations: k8s.AnnoProm(statusPort, metricsPath),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r, v1alpha1.SchemeGroupVersion.WithKind("TiDBOneRouter")),
			},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: ptr.To[int64](gracePeriodSeconds),
			Hostname:                      coreutil.PodName[scope.TiDBOneRouter](r),
			Subdomain:                     r.Spec.Subdomain,
			NodeSelector:                  r.Spec.Topology,
			Containers: []corev1.Container{
				{
					Name:            v1alpha1.ContainerNameTiDBOneRouter,
					Image:           image.TiDBOneRouter.Image(r.Spec.Image, r.Spec.Version),
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         args,
					Ports: []corev1.ContainerPort{
						{Name: v1alpha1.TiDBOneRouterPortNameClient, ContainerPort: clientPort},
						{Name: v1alpha1.TiDBOneRouterPortNameStatus, ContainerPort: statusPort},
					},
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
					Resources: k8s.GetResourceRequirements(r.Spec.Resources),
				},
			},
		},
	}
	if cluster.Status.ID != "" {
		pod.Labels[v1alpha1.LabelKeyClusterID] = cluster.Status.ID
	}
	if r.Spec.Overlay != nil {
		overlay.OverlayPod(pod, r.Spec.Overlay.Pod)
	}
	reloadable.MustEncodeLastTiDBOneRouterTemplate(r, pod)
	return pod
}

// TaskDeletePod deletes the pod of a deleting router.
func TaskDeletePod(state *ReconcileContext, c client.Client) task.Task {
	return task.NameTaskFunc("DeletePod", func(ctx context.Context) task.Result {
		pod := state.Pod()
		if pod == nil {
			return task.Complete().With("pod has been deleted")
		}
		if pod.GetDeletionTimestamp().IsZero() {
			if err := c.Delete(ctx, pod, client.GracePeriodSeconds(gracePeriodSeconds)); client.IgnoreNotFound(err) != nil {
				return task.Fail().With("can't delete pod of tidb-one-router: %v", err)
			}
		}
		state.DeletePod(pod)
		return task.Wait().With("pod is deleting")
	})
}
