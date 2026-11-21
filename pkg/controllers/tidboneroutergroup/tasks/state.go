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
	"k8s.io/apimachinery/pkg/types"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
	stateutil "github.com/pingcap/tidb-operator/v2/pkg/state"
)

type state struct {
	key types.NamespacedName

	cluster *v1alpha1.Cluster
	og     *v1alpha1.TiDBOneRouterGroup
	kvs     []*v1alpha1.TiDBOneRouter

	updateRevision  string
	currentRevision string
	collisionCount  int32

	statusChanged bool

	stateutil.IFeatureGates
}

type State interface {
	common.TiDBOneRouterSliceStateInitializer
	common.RevisionStateInitializer[*runtime.TiDBOneRouterGroup]

	common.TiDBOneRouterGroupState
	common.ClusterState
	common.TiDBOneRouterSliceState
	common.RevisionState

	common.GroupState[*runtime.TiDBOneRouterGroup]

	common.ContextClusterNewer[*v1alpha1.TiDBOneRouterGroup]
	common.ContextObjectNewer[*v1alpha1.TiDBOneRouterGroup]
	common.ContextSliceNewer[*v1alpha1.TiDBOneRouterGroup, *v1alpha1.TiDBOneRouter]

	common.InstanceSliceState[*runtime.TiDBOneRouter]
	common.SliceState[*v1alpha1.TiDBOneRouter]

	common.StatusUpdater
	common.StatusPersister[*v1alpha1.TiDBOneRouterGroup]

	stateutil.IFeatureGates
}

func NewState(key types.NamespacedName) State {
	s := &state{
		key: key,
	}
	s.IFeatureGates = stateutil.NewFeatureGates[scope.TiDBOneRouterGroup](s)
	return s
}

func (s *state) Key() types.NamespacedName {
	return s.key
}

func (s *state) Object() *v1alpha1.TiDBOneRouterGroup {
	return s.og
}

func (s *state) SetObject(og *v1alpha1.TiDBOneRouterGroup) {
	s.og = og
}

func (s *state) TiDBOneRouterGroup() *v1alpha1.TiDBOneRouterGroup {
	return s.og
}

func (s *state) Group() *runtime.TiDBOneRouterGroup {
	return runtime.FromTiDBOneRouterGroup(s.og)
}

func (s *state) Cluster() *v1alpha1.Cluster {
	return s.cluster
}

func (s *state) TiDBOneRouterSlice() []*v1alpha1.TiDBOneRouter {
	return s.kvs
}

func (s *state) Slice() []*runtime.TiDBOneRouter {
	return runtime.FromTiDBOneRouterSlice(s.kvs)
}

func (s *state) InstanceSlice() []*v1alpha1.TiDBOneRouter {
	return s.kvs
}

func (s *state) SetInstanceSlice(kvs []*v1alpha1.TiDBOneRouter) {
	s.kvs = kvs
}

func (s *state) SetCluster(cluster *v1alpha1.Cluster) {
	s.cluster = cluster
}

func (s *state) IsStatusChanged() bool {
	return s.statusChanged
}

func (s *state) SetStatusChanged() {
	s.statusChanged = true
}

func (s *state) TiDBOneRouterSliceInitializer() common.TiDBOneRouterSliceInitializer {
	return common.NewResourceSlice(func(kvs []*v1alpha1.TiDBOneRouter) { s.kvs = kvs }).
		WithNamespace(common.Namespace(s.key.Namespace)).
		WithLabels(s.Labels()).
		Initializer()
}

func (s *state) RevisionInitializer() common.RevisionInitializer[*runtime.TiDBOneRouterGroup] {
	return common.NewRevision[*runtime.TiDBOneRouterGroup](
		common.RevisionSetterFunc(func(update, current string, collisionCount int32) {
			s.updateRevision = update
			s.currentRevision = current
			s.collisionCount = collisionCount
		})).
		WithCurrentRevision(common.Lazy[string](func() string {
			return s.og.Status.CurrentRevision
		})).
		WithCollisionCount(common.Lazy[*int32](func() *int32 {
			return s.og.Status.CollisionCount
		})).
		WithParent(common.Lazy[*runtime.TiDBOneRouterGroup](func() *runtime.TiDBOneRouterGroup {
			return s.Group()
		})).
		WithLabels(s.Labels()).
		Initializer()
}

func (s *state) Revision() (update, current string, collisionCount int32) {
	return s.updateRevision, s.currentRevision, s.collisionCount
}

func (s *state) Labels() common.LabelsOption {
	return common.Lazy[map[string]string](func() map[string]string {
		return map[string]string{
			v1alpha1.LabelKeyManagedBy: v1alpha1.LabelValManagedByOperator,
			v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOneRouter,
			v1alpha1.LabelKeyCluster:   s.cluster.Name,
			v1alpha1.LabelKeyGroup:     s.og.Name,
		}
	})
}
