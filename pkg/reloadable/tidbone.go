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

package reloadable

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	meta "github.com/pingcap/tidb-operator/api/v2/meta/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/features"
)

// CheckTiDBOne returns whether changes are reloadable (used by the group controller).
func CheckTiDBOne(group *v1alpha1.TiDBOneGroup, instance *v1alpha1.TiDBOne) bool {
	return equalTiDBOneTemplate(&group.Spec.Template, templateFromTiDBOne(instance)) &&
		features.Reloadable(meta.ComponentTiDBOne, group.Spec.Features, instance.Spec.Features)
}

// CheckTiDBOnePod returns whether changes are reloadable (used by the instance controller).
func CheckTiDBOnePod(instance *v1alpha1.TiDBOne, pod *corev1.Pod) bool {
	tmpl := &v1alpha1.TiDBOneTemplate{}
	if err := json.Unmarshal([]byte(pod.Annotations[v1alpha1.AnnoKeyLastInstanceTemplate]), tmpl); err != nil {
		return true
	}
	return equalTiDBOneTemplate(templateFromTiDBOne(instance), tmpl) &&
		features.Reloadable(meta.ComponentTiDBOne, instance.Spec.Features, decodeFeatures(pod.Annotations[v1alpha1.AnnoKeyFeatures]))
}

func MustEncodeLastTiDBOneTemplate(instance *v1alpha1.TiDBOne, pod *corev1.Pod) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	data, err := json.Marshal(templateFromTiDBOne(instance))
	if err != nil {
		panic("cannot encode tidb-one template: " + err.Error())
	}
	pod.Annotations[v1alpha1.AnnoKeyLastInstanceTemplate] = string(data)
	pod.Annotations[v1alpha1.AnnoKeyFeatures] = encodeFeatures(instance.Spec.Features)
}

func templateFromTiDBOne(one *v1alpha1.TiDBOne) *v1alpha1.TiDBOneTemplate {
	return &v1alpha1.TiDBOneTemplate{
		ObjectMeta: v1alpha1.ObjectMeta{
			Labels:      one.Labels,
			Annotations: one.Annotations,
		},
		Spec: one.Spec.TiDBOneTemplateSpec,
	}
}

func convertTiDBOneTemplate(tmpl *v1alpha1.TiDBOneTemplate) *v1alpha1.TiDBOneTemplate {
	t := tmpl.DeepCopy()
	t.Labels = convertLabels(t.Labels)
	t.Annotations = convertAnnotations(t.Annotations)
	// These fields are applied without a restart.
	t.Spec.Server.Labels = nil
	t.Spec.CacheTTLSeconds = nil
	t.Spec.MinReadyForLeaderSeconds = nil
	t.Spec.DrainTimeoutSeconds = nil
	t.Spec.Volumes = convertVolumes(t.Spec.Volumes)
	t.Spec.Overlay = convertOverlay(t.Spec.Overlay)
	return t
}

func equalTiDBOneTemplate(c, p *v1alpha1.TiDBOneTemplate) bool {
	p = convertTiDBOneTemplate(p)
	c = convertTiDBOneTemplate(c)
	// Config changes restart the server only with the Restart strategy.
	if c.Spec.UpdateStrategy.Config == v1alpha1.ConfigUpdateStrategyRestart &&
		(p.Spec.Config != c.Spec.Config || p.Spec.TiKVConfig != c.Spec.TiKVConfig) {
		return false
	}
	p.Spec.Config, c.Spec.Config = "", ""
	p.Spec.TiKVConfig, c.Spec.TiKVConfig = "", ""
	p.Spec.UpdateStrategy.Config, c.Spec.UpdateStrategy.Config = "", ""
	return apiequality.Semantic.DeepEqual(p, c)
}

// CheckTiDBOneRouter returns whether changes are reloadable (used by the group controller).
func CheckTiDBOneRouter(group *v1alpha1.TiDBOneRouterGroup, instance *v1alpha1.TiDBOneRouter) bool {
	return equalTiDBOneRouterTemplate(&group.Spec.Template, templateFromTiDBOneRouter(instance)) &&
		features.Reloadable(meta.ComponentTiDBOneRouter, group.Spec.Features, instance.Spec.Features)
}

// CheckTiDBOneRouterPod returns whether changes are reloadable (used by the instance controller).
func CheckTiDBOneRouterPod(instance *v1alpha1.TiDBOneRouter, pod *corev1.Pod) bool {
	tmpl := &v1alpha1.TiDBOneRouterTemplate{}
	if err := json.Unmarshal([]byte(pod.Annotations[v1alpha1.AnnoKeyLastInstanceTemplate]), tmpl); err != nil {
		return true
	}
	return equalTiDBOneRouterTemplate(templateFromTiDBOneRouter(instance), tmpl) &&
		features.Reloadable(meta.ComponentTiDBOneRouter, instance.Spec.Features, decodeFeatures(pod.Annotations[v1alpha1.AnnoKeyFeatures]))
}

func MustEncodeLastTiDBOneRouterTemplate(instance *v1alpha1.TiDBOneRouter, pod *corev1.Pod) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	data, err := json.Marshal(templateFromTiDBOneRouter(instance))
	if err != nil {
		panic("cannot encode tidb-one-router template: " + err.Error())
	}
	pod.Annotations[v1alpha1.AnnoKeyLastInstanceTemplate] = string(data)
	pod.Annotations[v1alpha1.AnnoKeyFeatures] = encodeFeatures(instance.Spec.Features)
}

func templateFromTiDBOneRouter(r *v1alpha1.TiDBOneRouter) *v1alpha1.TiDBOneRouterTemplate {
	return &v1alpha1.TiDBOneRouterTemplate{
		ObjectMeta: v1alpha1.ObjectMeta{
			Labels:      r.Labels,
			Annotations: r.Annotations,
		},
		Spec: r.Spec.TiDBOneRouterTemplateSpec,
	}
}

func equalTiDBOneRouterTemplate(c, p *v1alpha1.TiDBOneRouterTemplate) bool {
	p = p.DeepCopy()
	c = c.DeepCopy()
	for _, t := range []*v1alpha1.TiDBOneRouterTemplate{p, c} {
		t.Labels = convertLabels(t.Labels)
		t.Annotations = convertAnnotations(t.Annotations)
		t.Spec.Volumes = convertVolumes(t.Spec.Volumes)
		t.Spec.Overlay = convertOverlay(t.Spec.Overlay)
		t.Spec.UpdateStrategy.Config = ""
	}
	return apiequality.Semantic.DeepEqual(p, c)
}
