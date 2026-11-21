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
	"path"

	"github.com/pelletier/go-toml/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

func TaskConfigMap(state *ReconcileContext, c client.Client) task.Task {
	return task.NameTaskFunc("ConfigMap", func(ctx context.Context) task.Result {
		one := state.TiDBOne()
		cluster := state.Cluster()
		if coreutil.IsTLSClusterEnabled(cluster) {
			return task.Fail().With("TLS is not supported by tidb-one yet")
		}
		var kv map[string]any
		if err := toml.Unmarshal([]byte(one.Spec.TiKVConfig), &kv); err != nil {
			return task.Fail().With("%s cannot be decoded: %v", v1alpha1.FileNameConfigTiDBOneTiKV, err)
		}
		// The config refers to the cert the pod mounts, see SessionTokenSecret.
		secret := SessionTokenSecret(cluster, state.Pod())
		tidbCfg, err := tidbConfig(one, secret)
		if err != nil {
			return task.Fail().With("%v", err)
		}
		expected := newConfigMap(one, tidbCfg)
		if e := c.Apply(ctx, expected); e != nil {
			return task.Fail().With("can't create/update cm of tidb-one: %v", e)
		}
		if secret == "" {
			return task.Complete().With("cm is synced; no session token signing cert, so the routers cannot move sessions")
		}
		return task.Complete().With("cm is synced")
	})
}

// tidbConfig is the user's TiDB config plus the session token signing cert.
// The other operator managed settings (addresses, PD, data dir) are flags
// of tidb-one-server, which take precedence over the file.
func tidbConfig(one *v1alpha1.TiDBOne, tokenSecret string) (string, error) {
	if tokenSecret == "" {
		var v map[string]any
		if err := toml.Unmarshal([]byte(one.Spec.Config), &v); err != nil {
			return "", fmt.Errorf("%s cannot be decoded: %w", v1alpha1.FileNameConfig, err)
		}
		return string(one.Spec.Config), nil
	}
	cfg := map[string]any{}
	if err := toml.Unmarshal([]byte(one.Spec.Config), &cfg); err != nil {
		return "", fmt.Errorf("%s cannot be decoded: %w", v1alpha1.FileNameConfig, err)
	}
	sec, _ := cfg["security"].(map[string]any)
	if sec == nil {
		sec = map[string]any{}
	}
	for _, k := range []string{"session-token-signing-cert", "session-token-signing-key"} {
		if _, ok := sec[k]; ok {
			return "", fmt.Errorf("security.%s: %w", k, v1alpha1.ErrFieldIsManagedByOperator)
		}
	}
	sec["session-token-signing-cert"] = path.Join(v1alpha1.DirPathTiDBSessionTokenSigningTLS, corev1.TLSCertKey)
	sec["session-token-signing-key"] = path.Join(v1alpha1.DirPathTiDBSessionTokenSigningTLS, corev1.TLSPrivateKeyKey)
	cfg["security"] = sec
	data, err := toml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("%s cannot be encoded: %w", v1alpha1.FileNameConfig, err)
	}
	return string(data), nil
}

func newConfigMap(one *v1alpha1.TiDBOne, tidbCfg string) *corev1.ConfigMap {
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
			v1alpha1.FileNameConfig:            tidbCfg,
			v1alpha1.FileNameConfigTiDBOneTiKV: string(one.Spec.TiKVConfig),
		},
	}
}
