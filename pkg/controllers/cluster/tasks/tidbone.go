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
	"fmt"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	meta "github.com/pingcap/tidb-operator/api/v2/meta/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/apicall"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	"github.com/pingcap/tidb-operator/v2/pkg/runtime/scope"
)

// tidb-one: the cluster controller handles TiDBOneGroup and
// TiDBOneRouterGroup like the other groups.

type tidbOneGroups struct {
	TiDBOneGroups       []*v1alpha1.TiDBOneGroup
	TiDBOneRouterGroups []*v1alpha1.TiDBOneRouterGroup
}

func (g *tidbOneGroups) tidbOneCount() int {
	return len(g.TiDBOneGroups) + len(g.TiDBOneRouterGroups)
}

func (g *tidbOneGroups) listTiDBOne(ctx context.Context, c client.Client, ns, name string) error {
	ogs, err := apicall.ListGroups[scope.TiDBOneGroup](ctx, c, ns, name)
	if err != nil {
		return fmt.Errorf("can't list tidb-one groups: %w", err)
	}
	rgs, err := apicall.ListGroups[scope.TiDBOneRouterGroup](ctx, c, ns, name)
	if err != nil {
		return fmt.Errorf("can't list tidb-one router groups: %w", err)
	}
	g.TiDBOneGroups, g.TiDBOneRouterGroups = ogs, rgs
	return nil
}

func (g *tidbOneGroups) patchTiDBOneFeatures(ctx context.Context, c client.Client, fs []meta.Feature) error {
	for _, og := range g.TiDBOneGroups {
		if err := patchFeatures[scope.TiDBOneGroup](ctx, c, og, fs); err != nil {
			return fmt.Errorf("can't update feature gates for tidb-one group %s/%s: %w", og.Namespace, og.Name, err)
		}
	}
	for _, rg := range g.TiDBOneRouterGroups {
		if err := patchFeatures[scope.TiDBOneRouterGroup](ctx, c, rg, fs); err != nil {
			return fmt.Errorf("can't update feature gates for tidb-one router group %s/%s: %w", rg.Namespace, rg.Name, err)
		}
	}
	return nil
}

func (g *tidbOneGroups) deleteTiDBOne(ctx context.Context, c client.Client) error {
	for _, og := range g.TiDBOneGroups {
		//nolint:gocritic // not a real issue, see https://github.com/go-critic/go-critic/issues/1448
		if err := c.Delete(ctx, og); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("can't delete tidb-one group: %w", err)
		}
	}
	for _, rg := range g.TiDBOneRouterGroups {
		//nolint:gocritic // not a real issue, see https://github.com/go-critic/go-critic/issues/1448
		if err := c.Delete(ctx, rg); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("can't delete tidb-one router group: %w", err)
		}
	}
	return nil
}

func (g *tidbOneGroups) tidbOneComponents() []v1alpha1.ComponentStatus {
	var cs []v1alpha1.ComponentStatus
	if len(g.TiDBOneGroups) > 0 {
		s := v1alpha1.ComponentStatus{Kind: v1alpha1.ComponentKindTiDBOne}
		for _, og := range g.TiDBOneGroups {
			if og.Spec.Replicas != nil {
				s.Replicas += *og.Spec.Replicas
			}
		}
		cs = append(cs, s)
	}
	if len(g.TiDBOneRouterGroups) > 0 {
		s := v1alpha1.ComponentStatus{Kind: v1alpha1.ComponentKindTiDBOneRouter}
		for _, rg := range g.TiDBOneRouterGroups {
			if rg.Spec.Replicas != nil {
				s.Replicas += *rg.Spec.Replicas
			}
		}
		cs = append(cs, s)
	}
	return cs
}

func (g *tidbOneGroups) tidbOneNotSatisfying(groups []string, clusterGeneration int64, conditionType string) []string {
	groups = appendGroupsNotSatisfyingCondition[scope.TiDBOneGroup](groups, g.TiDBOneGroups, clusterGeneration, conditionType)
	return appendGroupsNotSatisfyingCondition[scope.TiDBOneRouterGroup](groups, g.TiDBOneRouterGroups, clusterGeneration, conditionType)
}
