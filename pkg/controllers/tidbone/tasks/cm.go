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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
	"github.com/pelletier/go-toml/v2"
)

func TaskConfigMap(state *ReconcileContext, c client.Client) task.Task {
	return task.NameTaskFunc("ConfigMap", func(ctx context.Context) task.Result {
		one := state.TiDBOne()
		if coreutil.IsTLSClusterEnabled(state.Cluster()) {
			return task.Fail().With("TLS is not supported by tidb-one yet")
		}
		// The operator managed settings (addresses, PD, data dir) are flags
		// of tidb-one-server, so the files are the user's config as is.
		for name, cfg := range map[string]string{
			v1alpha1.FileNameConfig:            string(one.Spec.Config),
			v1alpha1.FileNameConfigTiDBOneTiKV: string(one.Spec.TiKVConfig),
		} {
			var v map[string]any
			if err := toml.Unmarshal([]byte(cfg), &v); err != nil {
				return task.Fail().With("%s cannot be decoded: %v", name, err)
			}
		}
		expected := newConfigMap(one)
		if e := c.Apply(ctx, expected); e != nil {
			return task.Fail().With("can't create/update cm of tidb-one: %v", e)
		}
		return task.Complete().With("cm is synced")
	})
}

func newConfigMap(one *v1alpha1.TiDBOne) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coreutil.PodName[scope.TiDBOne](one),
			Namespace: one.Namespace,
			Labels:    coreutil.ConfigMapLabels[scope.TiDBOne](one),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(one, v1alpha1.SchemeGroupVersion.WithKind("TiDBOne")),
			},
		},
		Data: map[string]string{
			v1alpha1.FileNameConfig:            string(one.Spec.Config),
			v1alpha1.FileNameConfigTiDBOneTiKV: string(one.Spec.TiKVConfig),
		},
	}
}
