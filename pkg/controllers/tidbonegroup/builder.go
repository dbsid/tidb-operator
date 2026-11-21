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

package tidbonegroup

import (
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidbonegroup/tasks"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

func (r *Reconciler) NewRunner(state *tasks.ReconcileContext, reporter task.TaskReporter) task.TaskRunner {
	runner := task.NewTaskRunner(reporter,
		// get tikvgroup
		common.TaskContextObject[scope.TiDBOneGroup](state, r.Client),
		// if it's gone just return
		task.IfBreak(common.CondObjectHasBeenDeleted[scope.TiDBOneGroup](state)),

		// get cluster
		common.TaskContextCluster[scope.TiDBOneGroup](state, r.Client),
		// if it's paused just return
		task.IfBreak(common.CondClusterIsPaused(state)),
		task.IfBreak(common.CondGroupIsNotProgressing(state)),
		task.IfBreak(common.CondFeatureGatesIsNotSynced[scope.TiDBOneGroup](state)),

		// get all tikvs
		common.TaskContextSlice[scope.TiDBOneGroup](state, r.Client),

		task.IfBreak(common.CondObjectIsDeleting[scope.TiDBOneGroup](state),
			common.TaskGroupFinalizerDel[scope.TiDBOneGroup](state, r.Client),
			common.TaskGroupConditionReady[scope.TiDBOneGroup](state),
			common.TaskGroupConditionSynced[scope.TiDBOneGroup](state),
			common.TaskStatusRevisionAndReplicas[scope.TiDBOneGroup](state),
			common.TaskStatusPersister[scope.TiDBOneGroup](state, r.Client),
		),
		common.TaskFinalizerAdd[scope.TiDBOneGroup](state, r.Client),

		task.IfBreak(
			common.CondClusterIsSuspending(state),
			common.TaskGroupConditionSuspended[scope.TiDBOneGroup](state),
			common.TaskGroupConditionReady[scope.TiDBOneGroup](state),
			common.TaskGroupConditionSynced[scope.TiDBOneGroup](state),
			common.TaskStatusPersister[scope.TiDBOneGroup](state, r.Client),
		),

		common.TaskRevision[runtime.TiDBOneGroupTuple](state, r.Client),
		tasks.TaskService(state, r.Client),
		tasks.TaskUpdater(state, r.Client, r.AllocateFactory),
		common.TaskGroupStatusSelector[scope.TiDBOneGroup](state),
		common.TaskGroupConditionSuspended[scope.TiDBOneGroup](state),
		common.TaskGroupConditionReady[scope.TiDBOneGroup](state),
		common.TaskGroupConditionSynced[scope.TiDBOneGroup](state),
		common.TaskStatusRevisionAndReplicas[scope.TiDBOneGroup](state),
		common.TaskStatusPersister[scope.TiDBOneGroup](state, r.Client),
	)

	return runner
}
