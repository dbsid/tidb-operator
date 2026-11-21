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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	pdv1 "github.com/pingcap/tidb-operator/v2/pkg/timanager/apis/pd/v1"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/fake"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

type fakeRouterAPI struct {
	conns   map[string]int64 // router status addr -> connections
	fail    map[string]bool
	drained []string
	resumed []string
}

func (f *fakeRouterAPI) Drain(_ context.Context, statusAddr, sqlAddr string) (int64, error) {
	if f.fail[statusAddr] {
		return 0, errors.New("down")
	}
	f.drained = append(f.drained, statusAddr+"/"+sqlAddr)
	return f.conns[statusAddr], nil
}

func (f *fakeRouterAPI) Resume(_ context.Context, statusAddr, sqlAddr string) error {
	f.resumed = append(f.resumed, statusAddr+"/"+sqlAddr)
	return nil
}

func fakeRouter(name string) *v1alpha1.TiDBOneRouter {
	return fake.FakeObj(name, func(r *v1alpha1.TiDBOneRouter) *v1alpha1.TiDBOneRouter {
		r.Labels = map[string]string{
			v1alpha1.LabelKeyManagedBy: v1alpha1.LabelValManagedByOperator,
			v1alpha1.LabelKeyCluster:   "aaa",
			v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOneRouter,
		}
		r.Spec.Cluster.Name = "aaa"
		r.Spec.Subdomain = "r-tidb-one-router-peer"
		return r
	})
}

func readyPod() *corev1.Pod {
	return fake.FakeObj("aaa-tidb-one-xxx", func(p *corev1.Pod) *corev1.Pod {
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		return p
	})
}

func evictedOne(offline bool) *v1alpha1.TiDBOne {
	return fake.FakeObj("aaa-xxx", func(one *v1alpha1.TiDBOne) *v1alpha1.TiDBOne {
		one.Spec.Cluster.Name = "aaa"
		one.Spec.Subdomain = "one-tidb-one-peer"
		one.Spec.Offline = ptr.To(offline)
		one.Status.Conditions = []metav1.Condition{{
			Type:   v1alpha1.TiKVCondLeadersEvicted,
			Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonEvicted,
		}}
		return one
	})
}

func drainCond(one *v1alpha1.TiDBOne) *metav1.Condition {
	return meta.FindStatusCondition(one.Status.Conditions, v1alpha1.TiDBOneCondRoutersDrained)
}

func runDrain(t *testing.T, rtx *ReconcileContext, api RouterAPI, objs ...client.Object) task.Result {
	t.Helper()
	c := client.NewFakeClient(objs...)
	res, _ := task.RunTask(context.Background(), TaskRouterDrain(rtx, c, api))
	return res
}

func TestTaskRouterDrain(t *testing.T) {
	cluster := fake.FakeObj[v1alpha1.Cluster]("aaa")
	r1, r2 := fakeRouter("r-1"), fakeRouter("r-2")

	t.Run("waits for the leaders", func(t *testing.T) {
		one := evictedOne(false)
		one.Status.Conditions = nil
		rtx := &ReconcileContext{
			State:           &state{one: one, cluster: cluster, pod: readyPod()},
			Store:           &pdv1.Store{ID: "1"},
			PodNeedsRestart: true,
		}
		api := &fakeRouterAPI{}
		res := runDrain(t, rtx, api, r1)
		assert.Equal(t, task.SWait, res.Status())
		assert.Empty(t, api.drained)
	})

	t.Run("drains on every router until no connection is left", func(t *testing.T) {
		one := evictedOne(false)
		rtx := &ReconcileContext{
			State:           &state{one: one, cluster: cluster, pod: readyPod()},
			Store:           &pdv1.Store{ID: "1"},
			PodNeedsRestart: true,
		}
		api := &fakeRouterAPI{conns: map[string]int64{}}
		api.conns[routerAddr(cluster, r1)] = 3
		res := runDrain(t, rtx, api, r1, r2)
		assert.Equal(t, task.SRetry, res.Status())
		assert.Len(t, api.drained, 2)
		cond := drainCond(one)
		require.NotNil(t, cond)
		assert.Equal(t, v1alpha1.ReasonDraining, cond.Reason)
		require.Error(t, CheckRoutersDrained(one))
		assert.True(t, strings.HasSuffix(api.drained[0], "/"+SQLAddr(cluster, one)))

		api.conns[routerAddr(cluster, r1)] = 0
		res = runDrain(t, rtx, api, r1, r2)
		assert.Equal(t, task.SComplete, res.Status())
		assert.Equal(t, v1alpha1.ReasonDrained, drainCond(one).Reason)
		require.NoError(t, CheckRoutersDrained(one))
	})

	t.Run("a router that is down does not block", func(t *testing.T) {
		one := evictedOne(true)
		rtx := &ReconcileContext{
			State: &state{one: one, cluster: cluster, pod: readyPod()},
			Store: &pdv1.Store{ID: "1"},
		}
		api := &fakeRouterAPI{fail: map[string]bool{routerAddr(cluster, r2): true}}
		res := runDrain(t, rtx, api, r1, r2)
		assert.Equal(t, task.SComplete, res.Status())
		require.NoError(t, CheckRoutersDrained(one))
	})

	t.Run("gives up after the drain timeout", func(t *testing.T) {
		one := evictedOne(false)
		one.Spec.DrainTimeoutSeconds = ptr.To[int64](10)
		one.Status.Conditions = append(one.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.TiDBOneCondRoutersDrained,
			Status:             metav1.ConditionFalse,
			Reason:             v1alpha1.ReasonDraining,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-11 * time.Second)),
		})
		rtx := &ReconcileContext{
			State:           &state{one: one, cluster: cluster, pod: readyPod()},
			Store:           &pdv1.Store{ID: "1"},
			PodNeedsRestart: true,
		}
		api := &fakeRouterAPI{conns: map[string]int64{routerAddr(cluster, r1): 2}}
		res := runDrain(t, rtx, api, r1)
		assert.Equal(t, task.SComplete, res.Status())
		assert.Equal(t, v1alpha1.ReasonDrainTimeout, drainCond(one).Reason)
		require.NoError(t, CheckRoutersDrained(one))
	})

	t.Run("the timeout starts with the drain, not with an old condition", func(t *testing.T) {
		one := evictedOne(false)
		one.Status.Conditions = append(one.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.TiDBOneCondRoutersDrained,
			Status:             metav1.ConditionFalse,
			Reason:             v1alpha1.ReasonNotDrained,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
		})
		rtx := &ReconcileContext{
			State:           &state{one: one, cluster: cluster, pod: readyPod()},
			Store:           &pdv1.Store{ID: "1"},
			PodNeedsRestart: true,
		}
		api := &fakeRouterAPI{conns: map[string]int64{routerAddr(cluster, r1): 1}}
		runDrain(t, rtx, api, r1)
		res := runDrain(t, rtx, api, r1)
		assert.Equal(t, task.SRetry, res.Status())
		assert.Equal(t, v1alpha1.ReasonDraining, drainCond(one).Reason)
	})

	t.Run("resumes after the restart", func(t *testing.T) {
		one := evictedOne(false)
		one.Status.Conditions = []metav1.Condition{{
			Type:   v1alpha1.TiDBOneCondRoutersDrained,
			Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonDrained,
		}}
		api := &fakeRouterAPI{}

		notReady := &ReconcileContext{State: &state{one: one, cluster: cluster, pod: fake.FakeObj[corev1.Pod]("p")}}
		assert.Equal(t, task.SWait, runDrain(t, notReady, api, r1).Status())
		assert.Empty(t, api.resumed)

		rtx := &ReconcileContext{State: &state{one: one, cluster: cluster, pod: readyPod()}}
		assert.Equal(t, task.SComplete, runDrain(t, rtx, api, r1, r2).Status())
		assert.Len(t, api.resumed, 2)
		assert.Equal(t, v1alpha1.ReasonNotDrained, drainCond(one).Reason)
		require.Error(t, CheckRoutersDrained(one))

		// Nothing to do once resumed.
		api.resumed = nil
		assert.Equal(t, task.SComplete, runDrain(t, rtx, api, r1).Status())
		assert.Empty(t, api.resumed)
	})
}

func routerAddr(cluster *v1alpha1.Cluster, r *v1alpha1.TiDBOneRouter) string {
	addrs, _ := routerStatusAddrs(context.Background(), client.NewFakeClient(r), cluster)
	return addrs[0]
}

func TestHTTPRouterAPI(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		_, _ = w.Write([]byte(`[{"addr":"a.svc:4000","connections":2,"draining":true},{"addr":"b.svc:4000","connections":5}]`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	api := NewRouterAPI()

	n, err := api.Drain(context.Background(), addr, "a.svc:4000")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	n, err = api.Drain(context.Background(), addr, "c.svc:4000")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "a backend the router does not know has no connection")
	require.NoError(t, api.Resume(context.Background(), addr, "a.svc:4000"))
	assert.Equal(t, []string{
		"POST /api/backends/drain?addr=a.svc%3A4000",
		"POST /api/backends/drain?addr=c.svc%3A4000",
		"POST /api/backends/resume?addr=a.svc%3A4000",
	}, got)
}

func TestServerCommand(t *testing.T) {
	cluster := fake.FakeObj("aaa", func(c *v1alpha1.Cluster) *v1alpha1.Cluster {
		c.Namespace = "ns1"
		c.Status.PD = "http://aaa-pd.ns1:2379"
		return c
	})
	one := evictedOne(false)
	one.Namespace = "ns1"
	one.Spec.Version = "v9.0.0-tidbone"
	one.Spec.Volumes = []v1alpha1.Volume{{
		Name:   "data",
		Mounts: []v1alpha1.VolumeMount{{Type: v1alpha1.VolumeMountTypeTiDBOneData}},
	}}
	pod := newPod(cluster, one, &pdv1.Store{ID: "7"}, "", nil)
	cmd := strings.Join(pod.Spec.Containers[0].Command, " ")
	assert.Contains(t, cmd, "/tidb-one-server -P 4000 --status 10080")
	assert.Contains(t, cmd, "--advertise-address aaa-tidb-one-xxx.one-tidb-one-peer.ns1.svc ")
	assert.Contains(t, cmd, "--path aaa-pd.ns1:2379")
	assert.Contains(t, cmd, "--tikv.advertise-addr aaa-tidb-one-xxx.one-tidb-one-peer.ns1:20160")
	assert.Contains(t, cmd, "--tikv.data-dir "+v1alpha1.VolumeMountTiDBOneDataDefaultPath)
	assert.Equal(t, "aaa-tidb-one-xxx.one-tidb-one-peer.ns1.svc:4000", SQLAddr(cluster, one))
	assert.Equal(t, "10080", pod.Annotations["prometheus.io/port"])
	assert.Equal(t, "20180", pod.Annotations["tikv.prometheus.io/port"])
	assert.Equal(t, "7", pod.Labels[v1alpha1.LabelKeyStoreID])
	assert.Equal(t, "/status", pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Path)
	assert.Equal(t, "ghcr.io/dbsid/tidb-one-server:v9.0.0-tidbone", pod.Spec.Containers[0].Image)
}

func TestTiDBConfig(t *testing.T) {
	one := evictedOne(false)
	one.Spec.Version = "v9.0.0"
	one.Spec.Config = "[log]\nlevel = \"warn\"\n"
	cfg, err := tidbConfig(one, "")
	require.NoError(t, err)
	assert.Equal(t, string(one.Spec.Config), cfg, "no signing cert: the config is kept as is")

	cfg, err = tidbConfig(one, "token")
	require.NoError(t, err)
	assert.Contains(t, cfg, "level = 'warn'")
	assert.Contains(t, cfg, "session-token-signing-cert = '"+v1alpha1.DirPathTiDBSessionTokenSigningTLS+"/tls.crt'")

	one.Spec.Config = "[security]\nsession-token-signing-key = \"/x\"\n"
	_, err = tidbConfig(one, "token")
	require.ErrorIs(t, err, v1alpha1.ErrFieldIsManagedByOperator)
}

func TestSessionTokenSecret(t *testing.T) {
	signed := fake.FakeObj("aaa", func(c *v1alpha1.Cluster) *v1alpha1.Cluster {
		c.Spec.Security = &v1alpha1.ClusterSecurity{
			SessionTokenSigningCertKeyPair: &corev1.LocalObjectReference{Name: "token"},
		}
		return c
	})
	one := evictedOne(false)
	one.Spec.Version = "v9.0.0"
	// A new pod gets the cert of the cluster.
	assert.Equal(t, "token", SessionTokenSecret(signed, nil))
	withCert := newPod(signed, one, nil, SessionTokenSecret(signed, nil), nil)
	assert.Equal(t, "token", SessionTokenSecret(signed, withCert))
	// A running pod keeps the cert it was created with.
	without := newPod(signed, one, nil, "", nil)
	assert.Empty(t, SessionTokenSecret(signed, without))
}
