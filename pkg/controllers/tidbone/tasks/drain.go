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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/apicall"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	httputil "github.com/pingcap/tidb-operator/v2/pkg/utils/http"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/podutil"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

const routerRequestTimeout = 3 * time.Second

// RouterBackend is one entry of `GET /api/backends` on a router.
type RouterBackend struct {
	Addr        string `json:"addr"`
	Connections int64  `json:"connections"`
	Draining    bool   `json:"draining"`
}

// RouterAPI calls the drain API of the routers. It is replaced in tests.
type RouterAPI interface {
	// Drain stops new transactions to sqlAddr and moves its sessions away;
	// it returns the open connections from the router to sqlAddr.
	Drain(ctx context.Context, statusAddr, sqlAddr string) (int64, error)
	Resume(ctx context.Context, statusAddr, sqlAddr string) error
}

type httpRouterAPI struct {
	c *http.Client
}

func NewRouterAPI() RouterAPI {
	return &httpRouterAPI{c: &http.Client{
		Timeout:       routerRequestTimeout,
		CheckRedirect: httputil.RejectRedirect,
	}}
}

func (a *httpRouterAPI) call(ctx context.Context, statusAddr, op, sqlAddr string) ([]RouterBackend, error) {
	u := fmt.Sprintf("http://%s/api/backends/%s?addr=%s", statusAddr, op, url.QueryEscape(sqlAddr))
	body, err := httputil.PostBodyOK(ctx, a.c, u, nil)
	if err != nil {
		return nil, err
	}
	var bs []RouterBackend
	if err := json.Unmarshal(body, &bs); err != nil {
		return nil, fmt.Errorf("cannot decode %s: %w", u, err)
	}
	return bs, nil
}

func (a *httpRouterAPI) Drain(ctx context.Context, statusAddr, sqlAddr string) (int64, error) {
	bs, err := a.call(ctx, statusAddr, "drain", sqlAddr)
	if err != nil {
		return 0, err
	}
	for _, b := range bs {
		if b.Addr == sqlAddr {
			return b.Connections, nil
		}
	}
	return 0, nil
}

func (a *httpRouterAPI) Resume(ctx context.Context, statusAddr, sqlAddr string) error {
	_, err := a.call(ctx, statusAddr, "resume", sqlAddr)
	return err
}

// routerStatusAddrs lists the status addresses of the ready routers of the cluster.
func routerStatusAddrs(ctx context.Context, c client.Client, cluster *v1alpha1.Cluster) ([]string, error) {
	routers, err := apicall.ListClusterInstances[scope.TiDBOneRouter](ctx, c, cluster.Namespace, cluster.Name)
	if err != nil {
		return nil, err
	}
	var addrs []string
	for _, r := range routers {
		if !r.GetDeletionTimestamp().IsZero() {
			continue
		}
		addrs = append(addrs, coreutil.InstanceAdvertiseAddress[scope.TiDBOneRouter](
			cluster, r, coreutil.TiDBOneRouterStatusPort(r)))
	}
	return addrs, nil
}

// TaskRouterDrain drains the server on all routers before it is restarted
// (state.PodNeedsRestart) or taken offline, after its leaders are evicted,
// and resumes it once it serves again.
// The progress is the RoutersDrained condition: TaskPod and TaskOfflineStore
// wait for it to be true.
func TaskRouterDrain(state *ReconcileContext, c client.Client, api RouterAPI) task.Task {
	return task.NameTaskFunc("RouterDrain", func(ctx context.Context) task.Result {
		one := state.TiDBOne()
		offline := coreutil.IsOffline[scope.TiDBOne](one)
		if !state.PodNeedsRestart && !offline {
			return resumeOnRouters(ctx, state, c, api)
		}
		if state.Store != nil {
			// Leaders first: the routers send transactions next to the
			// leaders, so with no leader left few sessions are on the server.
			leaders := CheckTiKVLeadersEvicted(one)
			if offline {
				leaders = CheckTiKVLeadersEvictedOrTimeout(one, defaultLeaderEvictTimeout)
			}
			if leaders != nil {
				return task.Wait().With("drain after the leaders are evicted: %v", leaders)
			}
		}
		return drainOnRouters(ctx, state, c, api)
	})
}

func resumeOnRouters(ctx context.Context, state *ReconcileContext, c client.Client, api RouterAPI) task.Result {
	one := state.TiDBOne()
	cond := meta.FindStatusCondition(one.Status.Conditions, v1alpha1.TiDBOneCondRoutersDrained)
	if cond == nil || cond.Reason == v1alpha1.ReasonNotDrained {
		return task.Complete().With("not drained")
	}
	if !podutil.IsReady(state.Pod()) || state.IsPodTerminating() {
		return task.Wait().With("resume after the pod is ready")
	}
	addrs, err := routerStatusAddrs(ctx, c, state.Cluster())
	if err != nil {
		return task.Fail().With("cannot list routers: %v", err)
	}
	sqlAddr := SQLAddr(state.Cluster(), one)
	for _, a := range addrs {
		if err := api.Resume(ctx, a, sqlAddr); err != nil {
			// A router that is down does not route to the server; it
			// learns the server from /info/all when it is back.
			logr.FromContextOrDiscard(ctx).Info("cannot resume on router, skip it", "router", a, "error", err.Error())
		}
	}
	if setDrainedCond(one, metav1.ConditionFalse, v1alpha1.ReasonNotDrained, "routers send transactions to the server") {
		state.SetStatusChanged()
	}
	return task.Complete().With("resumed on %d routers", len(addrs))
}

func drainOnRouters(ctx context.Context, state *ReconcileContext, c client.Client, api RouterAPI) task.Result {
	one := state.TiDBOne()
	cond := meta.FindStatusCondition(one.Status.Conditions, v1alpha1.TiDBOneCondRoutersDrained)
	addrs, err := routerStatusAddrs(ctx, c, state.Cluster())
	if err != nil {
		return task.Fail().With("cannot list routers: %v", err)
	}
	sqlAddr := SQLAddr(state.Cluster(), one)
	var conns int64
	for _, a := range addrs {
		n, err := api.Drain(ctx, a, sqlAddr)
		if err != nil {
			logr.FromContextOrDiscard(ctx).Info("cannot drain on router, skip it", "router", a, "error", err.Error())
			continue
		}
		conns += n
	}

	switch {
	case conns == 0:
		if setDrainedCond(one, metav1.ConditionTrue, v1alpha1.ReasonDrained,
			fmt.Sprintf("no connection from %d routers", len(addrs))) {
			state.SetStatusChanged()
		}
		return task.Complete().With("drained on %d routers", len(addrs))
	case cond == nil || cond.Reason == v1alpha1.ReasonNotDrained:
		// The first drain request: the timeout starts now, so the
		// transition time of the condition must be reset.
		meta.RemoveStatusCondition(&one.Status.Conditions, v1alpha1.TiDBOneCondRoutersDrained)
		setDrainedCond(one, metav1.ConditionFalse, v1alpha1.ReasonDraining,
			fmt.Sprintf("%d connections left", conns))
		state.SetStatusChanged()
	case cond.Reason == v1alpha1.ReasonDraining &&
		time.Since(cond.LastTransitionTime.Time) >= time.Duration(coreutil.TiDBOneDrainTimeoutSeconds(one))*time.Second:
		setDrainedCond(one, metav1.ConditionTrue, v1alpha1.ReasonDrainTimeout,
			fmt.Sprintf("drain timed out with %d connections left", conns))
		state.SetStatusChanged()
		return task.Complete().With("drain timed out, %d connections left", conns)
	}
	return task.Retry(time.Second).With("draining, %d connections left", conns)
}

func setDrainedCond(one *v1alpha1.TiDBOne, status metav1.ConditionStatus, reason, msg string) bool {
	return meta.SetStatusCondition(&one.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.TiDBOneCondRoutersDrained,
		Status:             status,
		ObservedGeneration: one.Generation,
		Reason:             reason,
		Message:            msg,
	})
}

// CheckRoutersDrained returns nil if the routers have no connection to the
// server, or the drain timed out.
func CheckRoutersDrained(one *v1alpha1.TiDBOne) error {
	cond := meta.FindStatusCondition(one.Status.Conditions, v1alpha1.TiDBOneCondRoutersDrained)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return fmt.Errorf("routers are still draining the server")
	}
	return nil
}
