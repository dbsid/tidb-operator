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

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	meta "github.com/pingcap/tidb-operator/api/v2/meta/v1alpha1"
	coreutil "github.com/pingcap/tidb-operator/v2/pkg/apiutil/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/v2/pkg/features"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	pdm "github.com/pingcap/tidb-operator/v2/pkg/timanager/pd"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

const (
	defaultLeaderEvictTimeout = 5 * time.Minute
	jitter                    = 10 * time.Second
)

func TaskOfflineStore(state *ReconcileContext, m pdm.PDClientManager, c client.Client) task.Task {
	return task.NameTaskFunc("OfflineStore", func(ctx context.Context) task.Result {
		if !state.PDSynced {
			return task.Wait().With("pd is not synced")
		}

		one := state.Object()
		isOffline := coreutil.IsOffline[scope.TiDBOne](one)

		// If the store is nil, it means the store has been deleted or not created yet.
		// No need to check if leaders are evicted.
		if state.Store != nil && isOffline && !features.NewFromCluster(state.Cluster().Spec.FeatureGates).Enabled(meta.TiKVSequentialScaleIn) {
			state.ShouldEvictLeader = true

			if err := CheckTiKVLeadersEvictedOrTimeout(one, defaultLeaderEvictTimeout); err != nil {
				return task.Retry(defaultLeaderEvictTimeout+jitter).
					With("waiting for leaders evicted or timeout: %v", err)
			}
		}

		// The routers move the sessions away before the store goes offline.
		if state.Store != nil && isOffline && state.GetStoreState() != v1alpha1.StoreStateRemoved {
			if err := CheckRoutersDrained(one); err != nil {
				return task.Retry(time.Second).With("wait for the routers before offline: %v", err)
			}
		}

		pc, ok := state.GetPDClient(m)
		if !ok {
			return task.Wait().With("pd client is not registered")
		}

		isSelected := !features.NewFromCluster(state.Cluster().Spec.FeatureGates).Enabled(meta.TiKVSequentialScaleIn) ||
			one.Annotations[v1alpha1.AnnoKeySelectedToDelete] == v1alpha1.AnnoValTrue

		if err := common.TaskOfflineStore[scope.TiDBOne](
			ctx,
			pc.Underlay(),
			one,
			state.GetStoreID(),
			state.GetStoreState(),
			isSelected,
		); err != nil {
			// refresh store state
			pc.Stores().Refresh()

			if task.IsWaitError(err) {
				return task.Wait().With("%v", err)
			}

			return task.Fail().With("%v", err)
		}

		// Release the persisted selection only after the PD-backed offline task
		// confirms cancellation (or that the store has already been removed).
		if !isOffline && one.Annotations[v1alpha1.AnnoKeySelectedToDelete] == v1alpha1.AnnoValTrue {
			updated := one.DeepCopy()
			delete(updated.Annotations, v1alpha1.AnnoKeySelectedToDelete)
			patch := ctrlclient.MergeFromWithOptions(one, ctrlclient.MergeFromWithOptimisticLock{})
			if err := c.Patch(ctx, updated, patch); err != nil {
				return task.Fail().With("cannot clear selected-to-delete annotation: %v", err)
			}
			*one = *updated
		}

		return task.Complete().With("offline is completed or no need, spec.offline: %v", isOffline)
	})
}
