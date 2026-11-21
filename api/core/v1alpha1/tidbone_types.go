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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	meta "github.com/pingcap/tidb-operator/api/v2/meta/v1alpha1"
)

// tidb-one: a TiDBOne runs tidb-one-server, TiDB and TiKV in one process.
// It is a TiDB server and a TiKV store at the same time: it serves SQL and
// is registered in PD as a store.

const (
	LabelValComponentTiDBOne       = string(meta.ComponentTiDBOne)
	LabelValComponentTiDBOneRouter = string(meta.ComponentTiDBOneRouter)

	ComponentKindTiDBOne       ComponentKind = "TiDBOne"
	ComponentKindTiDBOneRouter ComponentKind = "TiDBOneRouter"

	ContainerNameTiDBOne       = "tidb-one"
	ContainerNameTiDBOneRouter = "tidb-one-router"

	DirPathConfigTiDBOne = "/etc/tidb-one"
	// FileNameConfigTiDBOneTiKV is the config file of the embedded TiKV.
	// The TiDB config uses FileNameConfig.
	FileNameConfigTiDBOneTiKV = "tikv.toml"

	// VolumeMountTypeTiDBOneData is the data dir of the embedded TiKV.
	VolumeMountTypeTiDBOneData        VolumeMountType = "data"
	VolumeMountTiDBOneDataDefaultPath                 = "/var/lib/tidb-one"

	TiDBOnePortNameClient     = "mysql-client"
	TiDBOnePortNameStatus     = "status"
	TiDBOnePortNameTiKVClient = "tikv-client"
	TiDBOnePortNameTiKVStatus = "tikv-status"

	DefaultTiDBOnePortClient     = 4000
	DefaultTiDBOnePortStatus     = 10080
	DefaultTiDBOnePortTiKVClient = 20160
	DefaultTiDBOnePortTiKVStatus = 20180

	DefaultTiDBOneMinReadySeconds          = 5
	DefaultTiDBOneMinReadyForLeaderSeconds = 5
	// DefaultTiDBOneDrainTimeoutSeconds caps how long a restart waits for the
	// routers to move all sessions off the server.
	DefaultTiDBOneDrainTimeoutSeconds = 60

	// TiDBOneCondRoutersDrained is true when every router stopped sending
	// transactions to the server and has no connection to it. The leaders
	// use TiKVCondLeadersEvicted, as for TiKV.
	TiDBOneCondRoutersDrained = "RoutersDrained"
	ReasonDrained             = "Drained"
	ReasonDraining            = "Draining"
	ReasonNotDrained          = "NotDrained"
	ReasonDrainTimeout        = "DrainTimeout"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// TiDBOneGroupList defines a list of TiDBOne groups
type TiDBOneGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TiDBOneGroup `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:categories=group,shortName=oneg
// +kubebuilder:selectablefield:JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Desired",type=string,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Updated",type=string,JSONPath=`.status.updatedReplicas`
// +kubebuilder:printcolumn:name="UpdateRevision",type=string,JSONPath=`.status.updateRevision`
// +kubebuilder:printcolumn:name="CurrentRevision",type=string,JSONPath=`.status.currentRevision`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// TiDBOneGroup defines a group of similar TiDBOne instances.
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 40",message="name must not exceed 40 characters"
type TiDBOneGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TiDBOneGroupSpec   `json:"spec,omitempty"`
	Status TiDBOneGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// TiDBOneList defines a list of TiDBOne instances
type TiDBOneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TiDBOne `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas
// +kubebuilder:resource:categories=instance
// +kubebuilder:selectablefield:JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="StoreID",type=string,JSONPath=`.status.id`
// +kubebuilder:printcolumn:name="StoreState",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Offline",type=boolean,JSONPath=`.spec.offline`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// TiDBOne defines a TiDBOne instance.
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 47",message="name must not exceed 47 characters"
type TiDBOne struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TiDBOneSpec   `json:"spec,omitempty"`
	Status TiDBOneStatus `json:"status,omitempty"`
}

// TiDBOneGroupSpec describes the common attributes of a TiDBOneGroup
type TiDBOneGroupSpec struct {
	// Progressing controls group reconciliation, including updates, scaling, deletion, and status.
	// +optional
	// +kubebuilder:default=true
	Progressing *bool `json:"progressing,omitempty"`

	Cluster ClusterReference `json:"cluster"`
	// Features are enabled feature
	Features []meta.Feature `json:"features,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas"`

	// +listType=map
	// +listMapKey=type
	SchedulePolicies []SchedulePolicy `json:"schedulePolicies,omitempty"`

	// MinReadySeconds specifies the minimum number of seconds for which a newly created pod be ready without any of its containers crashing, for it to be considered available.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinReadySeconds *int64 `json:"minReadySeconds,omitempty"`

	Template TiDBOneTemplate `json:"template"`
}

type TiDBOneTemplate struct {
	ObjectMeta `json:"metadata,omitempty"`
	Spec       TiDBOneTemplateSpec `json:"spec"`
}

// TiDBOneTemplateSpec can only be specified in TiDBOneGroup
// +kubebuilder:validation:XValidation:rule="!has(self.overlay) || !has(self.overlay.volumeClaims) || (has(self.volumes) && self.overlay.volumeClaims.all(vc, vc.name in self.volumes.map(v, v.name)))",message="overlay volumeClaims names must exist in volumes"
// +kubebuilder:validation:XValidation:rule="has(self.volumes) && ('data' in self.volumes.map(v, v.name))",message="data volume must be configured"
type TiDBOneTemplateSpec struct {
	// Version must be a semantic version.
	// It can have a v prefix or not.
	// +kubebuilder:validation:Pattern=`^(v)?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`
	Version string `json:"version"`
	// Image is the tidb-one-server image.
	// If tag is omitted, version will be used as the image tag.
	// Default is ghcr.io/dbsid/tidb-one-server
	Image *string `json:"image,omitempty"`
	// Server defines the server config of TiDBOne
	Server TiDBOneServer `json:"server,omitempty"`
	// Resources defines resource required by TiDBOne
	Resources ResourceRequirements `json:"resources,omitempty"`
	// Config defines the TiDB config file (tidb.toml)
	Config ConfigFile `json:"config,omitempty"`
	// TiKVConfig defines the config file of the embedded TiKV (tikv.toml)
	TiKVConfig     ConfigFile     `json:"tikvConfig,omitempty"`
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty"`

	// Security defines security config.
	// NOTE: TLS is not supported by TiDBOne yet.
	Security *Security `json:"security,omitempty"`

	// Volumes defines data volume of TiDBOne
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=256
	Volumes []Volume `json:"volumes"`

	// CacheTTLSeconds specifies how long to wait after all leaders have been evicted
	// before restarting an instance, allowing the region caches to refresh.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CacheTTLSeconds *int64 `json:"cacheTTLSeconds,omitempty"`

	// MinReadyForLeaderSeconds specifies how long the restarted pod must remain ready
	// before removing the evict leader scheduler.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinReadyForLeaderSeconds *int64 `json:"minReadyForLeaderSeconds,omitempty"`

	// DrainTimeoutSeconds caps how long a restart or scale-in waits for the
	// TiDBOneRouters to move all sessions off the instance. Default is 60.
	// +kubebuilder:validation:Minimum=0
	// +optional
	DrainTimeoutSeconds *int64 `json:"drainTimeoutSeconds,omitempty"`

	// Overlay defines a k8s native resource template patch
	Overlay *Overlay `json:"overlay,omitempty"`
}

type TiDBOneServer struct {
	// Ports defines all ports listened by tidb-one-server
	Ports TiDBOnePorts `json:"ports,omitempty"`

	// Labels defines the server labels of the embedded TiKV store.
	// Operator will set these `labels` by API dynamically.
	// +kubebuilder:validation:XValidation:rule="!('host' in self) && !('region' in self) && !('zone' in self)",message="labels cannot contain 'host', 'region', or 'zone' keys"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !k.startsWith('k/') && !k.startsWith('$k/'))",message="labels cannot contain keys with 'k/' or '$k/' prefix"
	Labels map[string]string `json:"labels,omitempty"`
}

type TiDBOnePorts struct {
	// Client defines port for the MySQL protocol
	Client *Port `json:"client,omitempty"`
	// Status defines port for the TiDB status api
	Status *Port `json:"status,omitempty"`
	// TiKVClient defines port for the gRPC api of the embedded TiKV
	TiKVClient *Port `json:"tikvClient,omitempty"`
	// TiKVStatus defines port for the status api of the embedded TiKV
	TiKVStatus *Port `json:"tikvStatus,omitempty"`
}

type TiDBOneGroupStatus struct {
	CommonStatus `json:",inline"`
	GroupStatus  `json:",inline"`
}

// TiDBOneSpec defines the spec of a TiDBOne instance
// +kubebuilder:validation:XValidation:rule="(!has(oldSelf.topology) && !has(self.topology)) || (has(oldSelf.topology) && has(self.topology))",fieldPath=".topology",message="topology can only be set when creating"
type TiDBOneSpec struct {
	// Cluster is a reference of tidb cluster
	Cluster ClusterReference `json:"cluster"`
	// Features are enabled feature
	Features []meta.Feature `json:"features,omitempty"`
	// Topology defines the topology domain of this instance
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="topology is immutable"
	Topology Topology `json:"topology,omitempty"`
	// Subdomain means the subdomain of the exported dns.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="subdomain is immutable"
	Subdomain string `json:"subdomain"`

	// Offline marks the store as offline in PD to begin data migration.
	// +optional
	Offline *bool `json:"offline,omitempty"`

	// Replicas is used for compatibility with PodDisruptionBudgets and is read-only.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	// +default:value=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	TiDBOneTemplateSpec `json:",inline"`
}

type TiDBOneStatus struct {
	CommonStatus `json:",inline"`
	StoreStatus  `json:",inline"`
}
