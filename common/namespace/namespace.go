package namespace

import (
	"hash/fnv"
	"strconv"
	"time"

	namespacepb "go.temporal.io/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	// SampleRetentionKey is key to specify sample retention
	SampleRetentionKey = "sample_retention_days"

	// SampleRateKey is key to specify sample rate
	SampleRateKey = "sample_retention_rate"
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
func (ns *Namespace) ID() string {
	return ns.info.Id
}

// Name observes this namespace's configured name.
func (ns *Namespace) Name() string {
	return ns.info.Name
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

// Retention returns retention in days for given workflow
func (ns *Namespace) Retention(workflowID string) time.Duration {
	if ns.config.Retention == nil {
		return 0
	}

	if ns.IsSampledForLongerRetention(workflowID) {
		if sampledRetentionValue, ok := ns.info.Data[SampleRetentionKey]; ok {
			sampledRetentionDays, err := strconv.Atoi(sampledRetentionValue)
			sampledRetention := *timestamp.DurationFromDays(int32(sampledRetentionDays))
			if err != nil || sampledRetention < *ns.config.Retention {
				return *ns.config.Retention
			}
			return sampledRetention
		}
	}

	return *ns.config.Retention
}

// IsSampledForLongerRetentionEnabled return whether sample for longer retention
// is enabled or not
func (ns *Namespace) IsSampledForLongerRetentionEnabled(string) bool {
	_, ok := ns.info.Data[SampleRateKey]
	return ok
}

// IsSampledForLongerRetention return should given workflow been sampled or not
func (ns *Namespace) IsSampledForLongerRetention(workflowID string) bool {
	sampledRateValue, ok := ns.info.Data[SampleRateKey]
	if !ok {
		return false
	}
	sampledRate, err := strconv.ParseFloat(sampledRateValue, 64)
	if err != nil {
		return false
	}

	h := fnv.New32a()
	_, err = h.Write([]byte(workflowID))
	if err != nil {
		return false
	}
	hash := h.Sum32()

	// use 1000 so we support one decimal rate like 1.5%.
	r := float64(hash%1000) / float64(1000)
	return r < sampledRate
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
