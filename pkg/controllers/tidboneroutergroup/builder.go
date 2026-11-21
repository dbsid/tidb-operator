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

package tidboneroutergroup

import (
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidboneroutergroup/tasks"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

func (r *Reconciler) NewRunner(state *tasks.ReconcileContext, reporter task.TaskReporter) task.TaskRunner {
	runner := task.NewTaskRunner(reporter,
		// get tikvgroup
		common.TaskContextObject[scope.TiDBOneRouterGroup](state, r.Client),
		// if it's gone just return
		task.IfBreak(common.CondObjectHasBeenDeleted[scope.TiDBOneRouterGroup](state)),

		// get cluster
		common.TaskContextCluster[scope.TiDBOneRouterGroup](state, r.Client),
		// if it's paused just return
		task.IfBreak(common.CondClusterIsPaused(state)),
		task.IfBreak(common.CondGroupIsNotProgressing(state)),
		task.IfBreak(common.CondFeatureGatesIsNotSynced[scope.TiDBOneRouterGroup](state)),

		// get all tikvs
		common.TaskContextSlice[scope.TiDBOneRouterGroup](state, r.Client),

		task.IfBreak(common.CondObjectIsDeleting[scope.TiDBOneRouterGroup](state),
			common.TaskGroupFinalizerDel[scope.TiDBOneRouterGroup](state, r.Client),
			common.TaskGroupConditionReady[scope.TiDBOneRouterGroup](state),
			common.TaskGroupConditionSynced[scope.TiDBOneRouterGroup](state),
			common.TaskStatusRevisionAndReplicas[scope.TiDBOneRouterGroup](state),
			common.TaskStatusPersister[scope.TiDBOneRouterGroup](state, r.Client),
		),
		common.TaskFinalizerAdd[scope.TiDBOneRouterGroup](state, r.Client),

		task.IfBreak(
			common.CondClusterIsSuspending(state),
			common.TaskGroupConditionSuspended[scope.TiDBOneRouterGroup](state),
			common.TaskGroupConditionReady[scope.TiDBOneRouterGroup](state),
			common.TaskGroupConditionSynced[scope.TiDBOneRouterGroup](state),
			common.TaskStatusPersister[scope.TiDBOneRouterGroup](state, r.Client),
		),

		common.TaskRevision[runtime.TiDBOneRouterGroupTuple](state, r.Client),
		tasks.TaskService(state, r.Client),
		tasks.TaskUpdater(state, r.Client, r.AllocateFactory),
		common.TaskGroupStatusSelector[scope.TiDBOneRouterGroup](state),
		common.TaskGroupConditionSuspended[scope.TiDBOneRouterGroup](state),
		common.TaskGroupConditionReady[scope.TiDBOneRouterGroup](state),
		common.TaskGroupConditionSynced[scope.TiDBOneRouterGroup](state),
		common.TaskStatusRevisionAndReplicas[scope.TiDBOneRouterGroup](state),
		common.TaskStatusPersister[scope.TiDBOneRouterGroup](state, r.Client),
	)

	return runner
}
