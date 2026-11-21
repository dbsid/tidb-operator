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

package tidbonerouter

import (
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidbonerouter/tasks"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/task/v3"
)

func (r *Reconciler) NewRunner(state *tasks.ReconcileContext, reporter task.TaskReporter) task.TaskRunner {
	runner := task.NewTaskRunner(reporter,
		common.TaskContextObject[scope.TiDBOneRouter](state, r.Client),
		common.TaskTrack[scope.TiDBOneRouter](state, r.Tracker),
		common.TaskObserveInstance[scope.TiDBOneRouter](state),
		task.IfBreak(common.CondObjectHasBeenDeleted[scope.TiDBOneRouter](state)),

		common.TaskContextCluster[scope.TiDBOneRouter](state, r.Client),
		task.IfBreak(common.CondClusterIsPaused(state)),
		task.IfBreak(common.CondClusterIsDeleting(state),
			common.TaskInstanceFinalizerDel[scope.TiDBOneRouter](state, r.Client, common.DefaultInstanceSubresourceLister),
		),

		common.TaskContextPod[scope.TiDBOneRouter](state, r.Client),
		task.IfBreak(common.CondClusterPDAddrIsNotRegistered(state)),

		task.IfBreak(common.CondObjectIsDeleting[scope.TiDBOneRouter](state),
			tasks.TaskDeletePod(state, r.Client),
			task.If(task.CondFunc(func() bool { return state.Pod() == nil }),
				common.TaskInstanceFinalizerDel[scope.TiDBOneRouter](state, r.Client, common.DefaultInstanceSubresourceLister),
			),
		),

		common.TaskValidateInstanceName(state),
		common.TaskFinalizerAdd[scope.TiDBOneRouter](state, r.Client),
		common.TaskInstanceConditionSuspended[scope.TiDBOneRouter](state),
		task.IfBreak(common.CondClusterIsSuspending(state),
			common.TaskInstanceConditionSynced[scope.TiDBOneRouter](state),
			common.TaskInstanceConditionReady[scope.TiDBOneRouter](state),
			common.TaskInstanceConditionRunning[scope.TiDBOneRouter](state),
			common.TaskStatusPersister[scope.TiDBOneRouter](state, r.Client),
			common.TaskSuspendPod(state, r.Client),
		),

		tasks.TaskPod(state, r.Client, r.EventRecorder),
		tasks.TaskContextHealth(state),
		common.TaskInstanceConditionSynced[scope.TiDBOneRouter](state),
		common.TaskInstanceConditionReady[scope.TiDBOneRouter](state),
		common.TaskInstanceConditionRunning[scope.TiDBOneRouter](state),
		tasks.TaskStatus(state, r.Client),
	)

	return runner
}
