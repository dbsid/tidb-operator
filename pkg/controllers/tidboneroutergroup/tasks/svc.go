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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

// TaskService is the task to create sbc
// TODO: extract svc to a common task
func TaskService(state common.TiDBOneRouterGroupState, c client.Client) task.Task {
	return task.NameTaskFunc("Service", func(ctx context.Context) task.Result {
		og := state.TiDBOneRouterGroup()

		for _, svc := range []*corev1.Service{newHeadlessService(og), newClientService(og), newSeedService(og)} {
			if err := c.Apply(ctx, svc); err != nil {
				return task.Fail().With("%s", fmt.Sprintf("can't apply service %s: %v", svc.Name, err))
			}
		}

		return task.Complete().With("services of tidb-one-router have been applied")
	})
}

func newHeadlessService(og *v1alpha1.TiDBOneRouterGroup) *corev1.Service {
	ipFamilyPolicy := corev1.IPFamilyPolicyPreferDualStack
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coreutil.HeadlessServiceName[scope.TiDBOneRouterGroup](og),
			Namespace: og.Namespace,
			Labels: map[string]string{
				v1alpha1.LabelKeyManagedBy: v1alpha1.LabelValManagedByOperator,
				v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOneRouter,
				v1alpha1.LabelKeyCluster:   og.Spec.Cluster.Name,
				v1alpha1.LabelKeyGroup:     og.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(og, v1alpha1.SchemeGroupVersion.WithKind("TiDBOneRouterGroup")),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:           corev1.ServiceTypeClusterIP,
			ClusterIP:      corev1.ClusterIPNone,
			IPFamilyPolicy: &ipFamilyPolicy,
			Selector: map[string]string{
				v1alpha1.LabelKeyManagedBy: v1alpha1.LabelValManagedByOperator,
				v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOneRouter,
				v1alpha1.LabelKeyCluster:   og.Spec.Cluster.Name,
				v1alpha1.LabelKeyGroup:     og.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       v1alpha1.TiDBOneRouterPortNameClient,
					Port:       coreutil.TiDBOneRouterGroupClientPort(og),
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(v1alpha1.TiDBOneRouterPortNameClient),
				},
				{
					Name:       v1alpha1.TiDBOneRouterPortNameStatus,
					Port:       coreutil.TiDBOneRouterGroupStatusPort(og),
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(v1alpha1.TiDBOneRouterPortNameStatus),
				},
			},
			PublishNotReadyAddresses: true,
		},
	}
}

// ClientServiceName is the service clients connect to.
func ClientServiceName(groupName string) string {
	return groupName + "-" + v1alpha1.LabelValComponentTiDBOneRouter
}

// SeedServiceName is the service of the TiDB status ports of all tidb-one
// servers of the cluster. A router reads /info/all through it at start.
func SeedServiceName(groupName string) string {
	return groupName + "-" + v1alpha1.LabelValComponentTiDBOneRouter + "-seed"
}

func groupLabels(og *v1alpha1.TiDBOneRouterGroup) map[string]string {
	return map[string]string{
		v1alpha1.LabelKeyManagedBy: v1alpha1.LabelValManagedByOperator,
		v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOneRouter,
		v1alpha1.LabelKeyCluster:   og.Spec.Cluster.Name,
		v1alpha1.LabelKeyGroup:     og.Name,
	}
}

func newClientService(og *v1alpha1.TiDBOneRouterGroup) *corev1.Service {
	ipFamilyPolicy := corev1.IPFamilyPolicyPreferDualStack
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClientServiceName(og.Name),
			Namespace: og.Namespace,
			Labels:    groupLabels(og),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(og, v1alpha1.SchemeGroupVersion.WithKind("TiDBOneRouterGroup")),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:           corev1.ServiceTypeClusterIP,
			IPFamilyPolicy: &ipFamilyPolicy,
			Selector:       groupLabels(og),
			Ports: []corev1.ServicePort{
				{
					Name:       v1alpha1.TiDBOneRouterPortNameClient,
					Port:       coreutil.TiDBOneRouterGroupClientPort(og),
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(v1alpha1.TiDBOneRouterPortNameClient),
				},
				{
					Name:       v1alpha1.TiDBOneRouterPortNameStatus,
					Port:       coreutil.TiDBOneRouterGroupStatusPort(og),
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(v1alpha1.TiDBOneRouterPortNameStatus),
				},
			},
		},
	}
}

func newSeedService(og *v1alpha1.TiDBOneRouterGroup) *corev1.Service {
	ipFamilyPolicy := corev1.IPFamilyPolicyPreferDualStack
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SeedServiceName(og.Name),
			Namespace: og.Namespace,
			Labels:    groupLabels(og),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(og, v1alpha1.SchemeGroupVersion.WithKind("TiDBOneRouterGroup")),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:           corev1.ServiceTypeClusterIP,
			IPFamilyPolicy: &ipFamilyPolicy,
			// Ready tidb-one servers of the cluster, any group.
			Selector: map[string]string{
				v1alpha1.LabelKeyManagedBy: v1alpha1.LabelValManagedByOperator,
				v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOne,
				v1alpha1.LabelKeyCluster:   og.Spec.Cluster.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       v1alpha1.TiDBOnePortNameStatus,
					Port:       v1alpha1.DefaultTiDBOnePortStatus,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(v1alpha1.TiDBOnePortNameStatus),
				},
			},
		},
	}
}
