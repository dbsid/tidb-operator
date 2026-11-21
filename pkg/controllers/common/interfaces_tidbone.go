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

package common

import "github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"

// tidb-one components.
type (
	TiDBOneSliceInitializer       = ResourceSliceInitializer[v1alpha1.TiDBOne]
	TiDBOneRouterSliceInitializer = ResourceSliceInitializer[v1alpha1.TiDBOneRouter]

	TiDBOneGroupState interface {
		TiDBOneGroup() *v1alpha1.TiDBOneGroup
	}
	TiDBOneState interface {
		TiDBOne() *v1alpha1.TiDBOne
	}
	TiDBOneSliceStateInitializer interface {
		TiDBOneSliceInitializer() TiDBOneSliceInitializer
	}
	TiDBOneSliceState interface {
		TiDBOneSlice() []*v1alpha1.TiDBOne
	}

	TiDBOneRouterGroupState interface {
		TiDBOneRouterGroup() *v1alpha1.TiDBOneRouterGroup
	}
	TiDBOneRouterState interface {
		TiDBOneRouter() *v1alpha1.TiDBOneRouter
	}
	TiDBOneRouterSliceStateInitializer interface {
		TiDBOneRouterSliceInitializer() TiDBOneRouterSliceInitializer
	}
	TiDBOneRouterSliceState interface {
		TiDBOneRouterSlice() []*v1alpha1.TiDBOneRouter
	}
)
