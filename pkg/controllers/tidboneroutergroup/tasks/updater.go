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
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	metav1alpha1 "github.com/pingcap/tidb-operator/api/v2/meta/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/action"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/features"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/updater"
	"github.com/pingcap/tidb-operator/v2/pkg/updater/policy"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/tracker"
)

const (
	defaultUpdateWaitTime = time.Second * 30
)

// TaskUpdater is a task to scale or update TiKV instances when spec of TiKVGroup is changed.
func TaskUpdater(state *ReconcileContext, c client.Client, af tracker.AllocateFactory) task.Task {
	return task.NameTaskFunc("Updater", func(ctx context.Context) task.Result {
		logger := logr.FromContextOrDiscard(ctx)
		og := state.TiDBOneRouterGroup()

		checker := action.NewUpgradeChecker[scope.TiDBOneRouterGroup](c, state.Cluster(), logger)

		if needVersionUpgrade(og) && !checker.CanUpgrade(ctx, og) {
			// TODO(liubo02): change to Wait
			return task.Retry(defaultUpdateWaitTime).With("wait until preconditions of upgrading is met")
		}

		retryAfter := coreutil.RetryIfInstancesReadyButNotAvailable[scope.TiDBOneRouter](
			state.InstanceSlice(),
			coreutil.MinReadySeconds[scope.TiDBOneRouterGroup](og),
		)
		if retryAfter != 0 {
			return task.Retry(retryAfter).With("wait until no instances is ready but not available")
		}

		var topos []v1alpha1.ScheduleTopology
		for _, p := range og.Spec.SchedulePolicies {
			switch p.Type {
			case v1alpha1.SchedulePolicyTypeEvenlySpread:
				topos = p.EvenlySpread.Topologies
			default:
				// do nothing
			}
		}

		updateRevision, _, _ := state.Revision()

		kvs := state.Slice()

		topoPolicy, err := policy.NewTopologyPolicy(topos, updateRevision, kvs...)
		if err != nil {
			return task.Fail().With("invalid topo policy, it should be validated: %v", err)
		}

		var instances []string
		for _, in := range kvs {
			instances = append(instances, in.Name)
		}

		allocator := af.New(og.Namespace, og.Name, instances...)
		wait, err := updater.New[runtime.TiDBOneRouterTuple]().
			WithInstances(kvs...).
			WithDesired(int(state.Group().Replicas())).
			WithClient(c).
			// Start a new router before stopping an old one: a router
			// restart closes its client connections.
			WithMaxSurge(1).
			WithMaxUnavailable(0).
			WithRevision(updateRevision).
			WithNewFactory(TiDBOneRouterNewer(og, updateRevision, state.FeatureGates())).
			WithAddHooks(
				updater.AllocateName[*runtime.TiDBOneRouter](allocator),
				topoPolicy,
			).
			WithDelHooks(topoPolicy).
			WithUpdateHooks(topoPolicy).
			WithScaleInPreferPolicy(
				topoPolicy.PolicyScaleIn(),
			).
			WithMinReadySeconds(coreutil.MinReadySeconds[scope.TiDBOneRouterGroup](og)).
			Build().
			Do(ctx)
		if err != nil {
			return task.Fail().With("cannot update instances: %v", err)
		}
		if wait {
			return task.Wait().With("wait for all instances ready")
		}
		return task.Complete().With("all instances are synced")
	})
}

func needVersionUpgrade(og *v1alpha1.TiDBOneRouterGroup) bool {
	return og.Spec.Template.Spec.Version != og.Status.Version && og.Status.Version != ""
}

func TiDBOneRouterNewer(og *v1alpha1.TiDBOneRouterGroup, rev string, fg features.Gates) updater.NewFactory[*runtime.TiDBOneRouter] {
	return updater.NewFunc[*runtime.TiDBOneRouter](func() *runtime.TiDBOneRouter {
		spec := og.Spec.Template.Spec.DeepCopy()

		one := &v1alpha1.TiDBOneRouter{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: og.Namespace,
				// Name will be allocated by updater.AllocateName
				Labels:      coreutil.InstanceLabels[scope.TiDBOneRouterGroup](og, rev),
				Annotations: coreutil.InstanceAnnotations[scope.TiDBOneRouterGroup](og),
				Finalizers:  []string{metav1alpha1.Finalizer},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(og, v1alpha1.SchemeGroupVersion.WithKind("TiDBOneRouterGroup")),
				},
			},
			Spec: v1alpha1.TiDBOneRouterSpec{
				Cluster:                   og.Spec.Cluster,
				Features:                  og.Spec.Features,
				Subdomain:                 coreutil.HeadlessServiceName[scope.TiDBOneRouterGroup](og),
				TiDBOneRouterTemplateSpec: *spec,
			},
		}
		if fg.Enabled(metav1alpha1.ClusterSubdomain) {
			one.Spec.Subdomain = coreutil.ClusterSubdomain(og.Spec.Cluster.Name)
		}

		return runtime.FromTiDBOneRouter(one)
	})
}
