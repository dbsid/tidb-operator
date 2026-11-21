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

package main

import (
	"context"

	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidbone"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidbonegroup"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidbonerouter"
	"github.com/pingcap/tidb-operator/v2/pkg/controllers/tidboneroutergroup"
	pdm "github.com/pingcap/tidb-operator/v2/pkg/timanager/pd"
	"github.com/pingcap/tidb-operator/v2/pkg/utils/tracker"
	"github.com/pingcap/tidb-operator/v2/pkg/volumes"
)

// tidb-one controllers: TiDBOne(Group) and TiDBOneRouter(Group).

func tidbOneControllerSetups(mgr ctrl.Manager, c client.Client, pdcm pdm.PDClientManager,
	vm volumes.ModifierFactory, tf tracker.Factory,
) []controllerSetup {
	return []controllerSetup{
		{
			name:      "TiDBOneGroup",
			setupFunc: func() error { return tidbonegroup.Setup(mgr, c, tf.AllocateFactory("tidbone")) },
		},
		{
			name:      "TiDBOne",
			setupFunc: func() error { return tidbone.Setup(mgr, c, pdcm, vm, tf.Tracker("tidbone")) },
		},
		{
			name:      "TiDBOneRouterGroup",
			setupFunc: func() error { return tidboneroutergroup.Setup(mgr, c, tf.AllocateFactory("tidbonerouter")) },
		},
		{
			name:      "TiDBOneRouter",
			setupFunc: func() error { return tidbonerouter.Setup(mgr, c, pdcm, vm, tf.Tracker("tidbonerouter")) },
		},
	}
}

func addTiDBOneIndexer(ctx context.Context, mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.TiDBOneGroup{}, "spec.cluster.name",
		func(obj client.Object) []string {
			return []string{obj.(*v1alpha1.TiDBOneGroup).Spec.Cluster.Name}
		}); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.TiDBOneRouterGroup{}, "spec.cluster.name",
		func(obj client.Object) []string {
			return []string{obj.(*v1alpha1.TiDBOneRouterGroup).Spec.Cluster.Name}
		})
}

func addTiDBOneCacheByObject(byObj map[client.Object]cache.ByObject) {
	for _, obj := range []client.Object{
		&v1alpha1.TiDBOneGroup{}, &v1alpha1.TiDBOne{},
		&v1alpha1.TiDBOneRouterGroup{}, &v1alpha1.TiDBOneRouter{},
	} {
		byObj[obj] = cache.ByObject{Label: labels.Everything()}
	}
}
