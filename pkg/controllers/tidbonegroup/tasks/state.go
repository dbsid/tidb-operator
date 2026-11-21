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
	og      *v1alpha1.TiDBOneGroup
	kvs     []*v1alpha1.TiDBOne

	updateRevision  string
	currentRevision string
	collisionCount  int32

	statusChanged bool

	stateutil.IFeatureGates
}

type State interface {
	common.TiDBOneSliceStateInitializer
	common.RevisionStateInitializer[*runtime.TiDBOneGroup]

	common.TiDBOneGroupState
	common.ClusterState
	common.TiDBOneSliceState
	common.RevisionState

	common.GroupState[*runtime.TiDBOneGroup]

	common.ContextClusterNewer[*v1alpha1.TiDBOneGroup]
	common.ContextObjectNewer[*v1alpha1.TiDBOneGroup]
	common.ContextSliceNewer[*v1alpha1.TiDBOneGroup, *v1alpha1.TiDBOne]

	common.InstanceSliceState[*runtime.TiDBOne]
	common.SliceState[*v1alpha1.TiDBOne]

	common.StatusUpdater
	common.StatusPersister[*v1alpha1.TiDBOneGroup]

	stateutil.IFeatureGates
}

func NewState(key types.NamespacedName) State {
	s := &state{
		key: key,
	}
	s.IFeatureGates = stateutil.NewFeatureGates[scope.TiDBOneGroup](s)
	return s
}

func (s *state) Key() types.NamespacedName {
	return s.key
}

func (s *state) Object() *v1alpha1.TiDBOneGroup {
	return s.og
}

func (s *state) SetObject(og *v1alpha1.TiDBOneGroup) {
	s.og = og
}

func (s *state) TiDBOneGroup() *v1alpha1.TiDBOneGroup {
	return s.og
}

func (s *state) Group() *runtime.TiDBOneGroup {
	return runtime.FromTiDBOneGroup(s.og)
}

func (s *state) Cluster() *v1alpha1.Cluster {
	return s.cluster
}

func (s *state) TiDBOneSlice() []*v1alpha1.TiDBOne {
	return s.kvs
}

func (s *state) Slice() []*runtime.TiDBOne {
	return runtime.FromTiDBOneSlice(s.kvs)
}

func (s *state) InstanceSlice() []*v1alpha1.TiDBOne {
	return s.kvs
}

func (s *state) SetInstanceSlice(kvs []*v1alpha1.TiDBOne) {
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

func (s *state) TiDBOneSliceInitializer() common.TiDBOneSliceInitializer {
	return common.NewResourceSlice(func(kvs []*v1alpha1.TiDBOne) { s.kvs = kvs }).
		WithNamespace(common.Namespace(s.key.Namespace)).
		WithLabels(s.Labels()).
		Initializer()
}

func (s *state) RevisionInitializer() common.RevisionInitializer[*runtime.TiDBOneGroup] {
	return common.NewRevision[*runtime.TiDBOneGroup](
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
		WithParent(common.Lazy[*runtime.TiDBOneGroup](func() *runtime.TiDBOneGroup {
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
			v1alpha1.LabelKeyComponent: v1alpha1.LabelValComponentTiDBOne,
			v1alpha1.LabelKeyCluster:   s.cluster.Name,
			v1alpha1.LabelKeyGroup:     s.og.Name,
		}
	})
}
