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

package coreutil

import (
	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
)

func portOr(p *v1alpha1.Port, def int32) int32 {
	if p != nil {
		return p.Port
	}
	return def
}

func TiDBOneGroupClientPort(g *v1alpha1.TiDBOneGroup) int32 {
	return portOr(g.Spec.Template.Spec.Server.Ports.Client, v1alpha1.DefaultTiDBOnePortClient)
}

func TiDBOneGroupStatusPort(g *v1alpha1.TiDBOneGroup) int32 {
	return portOr(g.Spec.Template.Spec.Server.Ports.Status, v1alpha1.DefaultTiDBOnePortStatus)
}

func TiDBOneGroupTiKVClientPort(g *v1alpha1.TiDBOneGroup) int32 {
	return portOr(g.Spec.Template.Spec.Server.Ports.TiKVClient, v1alpha1.DefaultTiDBOnePortTiKVClient)
}

func TiDBOneGroupTiKVStatusPort(g *v1alpha1.TiDBOneGroup) int32 {
	return portOr(g.Spec.Template.Spec.Server.Ports.TiKVStatus, v1alpha1.DefaultTiDBOnePortTiKVStatus)
}

func TiDBOneClientPort(one *v1alpha1.TiDBOne) int32 {
	return portOr(one.Spec.Server.Ports.Client, v1alpha1.DefaultTiDBOnePortClient)
}

func TiDBOneStatusPort(one *v1alpha1.TiDBOne) int32 {
	return portOr(one.Spec.Server.Ports.Status, v1alpha1.DefaultTiDBOnePortStatus)
}

func TiDBOneTiKVClientPort(one *v1alpha1.TiDBOne) int32 {
	return portOr(one.Spec.Server.Ports.TiKVClient, v1alpha1.DefaultTiDBOnePortTiKVClient)
}

func TiDBOneTiKVStatusPort(one *v1alpha1.TiDBOne) int32 {
	return portOr(one.Spec.Server.Ports.TiKVStatus, v1alpha1.DefaultTiDBOnePortTiKVStatus)
}

// TiDBOneDrainTimeoutSeconds is how long a restart waits for the routers.
func TiDBOneDrainTimeoutSeconds(one *v1alpha1.TiDBOne) int64 {
	if one.Spec.DrainTimeoutSeconds != nil {
		return *one.Spec.DrainTimeoutSeconds
	}
	return v1alpha1.DefaultTiDBOneDrainTimeoutSeconds
}

func TiDBOneRouterGroupClientPort(g *v1alpha1.TiDBOneRouterGroup) int32 {
	return portOr(g.Spec.Template.Spec.Server.Ports.Client, v1alpha1.DefaultTiDBOneRouterPortClient)
}

func TiDBOneRouterGroupStatusPort(g *v1alpha1.TiDBOneRouterGroup) int32 {
	return portOr(g.Spec.Template.Spec.Server.Ports.Status, v1alpha1.DefaultTiDBOneRouterPortStatus)
}

func TiDBOneRouterClientPort(r *v1alpha1.TiDBOneRouter) int32 {
	return portOr(r.Spec.Server.Ports.Client, v1alpha1.DefaultTiDBOneRouterPortClient)
}

func TiDBOneRouterStatusPort(r *v1alpha1.TiDBOneRouter) int32 {
	return portOr(r.Spec.Server.Ports.Status, v1alpha1.DefaultTiDBOneRouterPortStatus)
}

// TiDBOneSessionTokenSigningSecretName is the secret with the cert that signs
// the session tokens. The routers move a session to another server with
// such a token, so all servers of the cluster must share the cert.
func TiDBOneSessionTokenSigningSecretName(cluster *v1alpha1.Cluster) string {
	if cluster.Spec.Security != nil && cluster.Spec.Security.SessionTokenSigningCertKeyPair != nil {
		return cluster.Spec.Security.SessionTokenSigningCertKeyPair.Name
	}
	return ""
}
