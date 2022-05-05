// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package namespace

import (
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
)

type (
	// Mutation changes a Namespace "in-flight" during a Clone operation.
	Mutation interface {
		apply(*persistence.GetNamespaceResponse)
	}

	// BadBinaryError is an error type carrying additional information about
	// when/why/who configured a given checksum as being bad.
	BadBinaryError struct {
		cksum string
		info  *namespacepb.BadBinaryInfo
	}

	// ID is the unique identifier type for a Namespace.
	ID string

	// Name is a user-supplied nickname for a Namespace.
	Name string

	// Namespaces is a *Namespace slice
	Namespaces []*Namespace

	// Namespace contains the info and config for a namespace
	Namespace struct {
		info                        persistencespb.NamespaceInfo
		config                      persistencespb.NamespaceConfig
		replicationConfig           persistencespb.NamespaceReplicationConfig
		configVersion               int64
		failoverVersion             int64
		isGlobalNamespace           bool
		failoverNotificationVersion int64
		notificationVersion         int64
	}
)

const (
	EmptyName Name = ""
	EmptyID   ID   = ""
)

func NewID() ID {
	return ID(uuid.NewString())
}

func FromPersistentState(record *persistence.GetNamespaceResponse) *Namespace {
	return &Namespace{
		info:                        *record.Namespace.Info,
		config:                      *record.Namespace.Config,
		replicationConfig:           *record.Namespace.ReplicationConfig,
		configVersion:               record.Namespace.ConfigVersion,
		failoverVersion:             record.Namespace.FailoverVersion,
		isGlobalNamespace:           record.IsGlobalNamespace,
		failoverNotificationVersion: record.Namespace.FailoverNotificationVersion,
		notificationVersion:         record.NotificationVersion,
	}
}

func (ns *Namespace) Clone(ms ...Mutation) *Namespace {
	newns := *ns
	r := persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        &newns.info,
			Config:                      &newns.config,
			ReplicationConfig:           &newns.replicationConfig,
			ConfigVersion:               newns.configVersion,
			FailoverNotificationVersion: newns.failoverNotificationVersion,
			FailoverVersion:             newns.failoverVersion,
		},
		IsGlobalNamespace:   newns.isGlobalNamespace,
		NotificationVersion: newns.notificationVersion,
	}
	for _, m := range ms {
		m.apply(&r)
	}
	return FromPersistentState(&r)
}

// VisibilityArchivalState observes the visibility archive configuration (state
// and URI) for this namespace.
func (ns *Namespace) VisibilityArchivalState() ArchivalState {
	return ArchivalState{
		State: ns.config.VisibilityArchivalState,
		URI:   ns.config.VisibilityArchivalUri,
	}
}

// HistoryArchivalState observes the history archive configuration (state and
// URI) for this namespace.
func (ns *Namespace) HistoryArchivalState() ArchivalState {
	return ArchivalState{
		State: ns.config.HistoryArchivalState,
		URI:   ns.config.HistoryArchivalUri,
	}
}

// VerifyBinaryChecksum returns an error if the provided checksum is one of this
// namespace's configured bad binary checksums. The returned error (if any) will
// be unwrappable as BadBinaryError.
func (ns *Namespace) VerifyBinaryChecksum(cksum string) error {
	badBinMap := ns.config.GetBadBinaries().GetBinaries()
	if badBinMap == nil {
		return nil
	}
	if info, ok := badBinMap[cksum]; ok {
		return BadBinaryError{cksum: cksum, info: info}
	}
	return nil
}

// ID observes this namespace's permanent unique identifier in string form.
func (ns *Namespace) ID() ID {
	return ID(ns.info.Id)
}

// Name observes this namespace's configured name.
func (ns *Namespace) Name() Name {
	return Name(ns.info.Name)
}

func (ns *Namespace) State() enumspb.NamespaceState {
	return ns.info.State
}

func (ns *Namespace) ReplicationState() enumspb.ReplicationState {
	return ns.replicationConfig.State
}

// ActiveClusterName observes the name of the cluster that is currently active
// for this namspace.
func (ns *Namespace) ActiveClusterName() string {
	return ns.replicationConfig.ActiveClusterName
}

// ClusterNames observes the names of the clusters to which this namespace is
// replicated.
func (ns *Namespace) ClusterNames() []string {
	// copy slice to preserve immutability
	out := make([]string, len(ns.replicationConfig.Clusters))
	copy(out, ns.replicationConfig.Clusters)
	return out
}

// ConfigVersion return the namespace config version
func (ns *Namespace) ConfigVersion() int64 {
	return ns.configVersion
}

// FailoverVersion return the namespace failover version
func (ns *Namespace) FailoverVersion() int64 {
	return ns.failoverVersion
}

// IsGlobalNamespace return whether the namespace is a global namespace
func (ns *Namespace) IsGlobalNamespace() bool {
	return ns.isGlobalNamespace
}

// FailoverNotificationVersion return the global notification version of when failover happened
func (ns *Namespace) FailoverNotificationVersion() int64 {
	return ns.failoverNotificationVersion
}

// NotificationVersion return the global notification version of when namespace changed
func (ns *Namespace) NotificationVersion() int64 {
	return ns.notificationVersion
}

// ActiveInCluster returns whether the namespace is active, i.e. non global
// namespace or global namespace which active cluster is the provided cluster
func (ns *Namespace) ActiveInCluster(clusterName string) bool {
	if !ns.isGlobalNamespace {
		// namespace is not a global namespace, meaning namespace is always
		// "active" within each cluster
		return true
	}
	return clusterName == ns.ActiveClusterName()
}

// ReplicationPolicy return the derived workflow replication policy
func (ns *Namespace) ReplicationPolicy() ReplicationPolicy {
	// frontend guarantee that the clusters always contains the active
	// namespace, so if the # of clusters is 1 then we do not need to send out
	// any events for replication
	if ns.isGlobalNamespace && len(ns.replicationConfig.Clusters) > 1 {
		return ReplicationPolicyMultiCluster
	}
	return ReplicationPolicyOneCluster
}

func (ns *Namespace) GetCustomData(key string) string {
	if ns.info.Data == nil {
		return ""
	}
	return ns.info.Data[key]
}

// Len return length
func (t Namespaces) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t Namespaces) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t Namespaces) Less(i, j int) bool {
	return t[i].notificationVersion < t[j].notificationVersion
}

// Retention returns retention duration for this namespace.
func (ns *Namespace) Retention() time.Duration {
	if ns.config.Retention == nil {
		return 0
	}

	return *ns.config.Retention
}

// Error returns the reason associated with this bad binary.
func (e BadBinaryError) Error() string {
	return e.info.Reason
}

// Reason returns the reason associated with this bad binary.
func (e BadBinaryError) Reason() string {
	return e.info.Reason
}

// Operator returns the operator associated with this bad binary.
func (e BadBinaryError) Operator() string {
	return e.info.Operator
}

// Created returns the time at which this bad binary was declared to be bad.
func (e BadBinaryError) Created() time.Time {
	return *e.info.CreateTime
}

// Checksum observes the binary checksum that caused this error.
func (e BadBinaryError) Checksum() string {
	return e.cksum
}

func (id ID) String() string {
	return string(id)
}

func (id ID) IsEmpty() bool {
	return id == EmptyID
}

func (n Name) String() string {
	return string(n)
}

func (n Name) IsEmpty() bool {
	return n == EmptyName
}
