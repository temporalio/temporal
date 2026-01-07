package namespace

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/util"
	expmaps "golang.org/x/exp/maps"
)

type (
	// Mutation changes a Namespace "in-flight" during a Clone operation.
	Mutation interface {
		apply(*Namespace)
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
		info                *persistencespb.NamespaceInfo
		config              *persistencespb.NamespaceConfig
		configVersion       int64
		notificationVersion int64

		customSearchAttributesMapper CustomSearchAttributesMapper
		replicationResolver          ReplicationResolver
	}

	CustomSearchAttributesMapper struct {
		fieldToAlias map[string]string
		aliasToField map[string]string
	}

	// ReplicationPolicy is the namespace's replication policy,
	// derived from namespace's replication config
	ReplicationPolicy int
)

const (
	EmptyName       Name = ""
	EmptyID         ID   = ""
	EmptyBusinessID      = ""

	// ReplicationPolicyOneCluster indicate that workflows does not need to be replicated
	// applicable to local namespace & global namespace with one cluster
	ReplicationPolicyOneCluster ReplicationPolicy = 0
	// ReplicationPolicyMultiCluster indicate that workflows need to be replicated
	ReplicationPolicyMultiCluster ReplicationPolicy = 1
)

func NewID() ID {
	return ID(uuid.NewString())
}

func FromPersistentState(
	detail *persistencespb.NamespaceDetail,
	resolver ReplicationResolver,
	mutations ...Mutation,
) (*Namespace, error) {
	if resolver == nil {
		return nil, serviceerror.NewInvalidArgument("replicationResolver must be provided")
	}
	ns := &Namespace{
		info:          detail.Info,
		config:        detail.Config,
		configVersion: detail.ConfigVersion,
		customSearchAttributesMapper: CustomSearchAttributesMapper{
			fieldToAlias: detail.Config.CustomSearchAttributeAliases,
			aliasToField: util.InverseMap(detail.Config.CustomSearchAttributeAliases),
		},
		replicationResolver: resolver,
	}

	for _, m := range mutations {
		m.apply(ns)
	}

	return ns, nil
}

func (ns *Namespace) Clone(mutations ...Mutation) *Namespace {
	// Clone the resolver to get a deep copy of replication state
	clonedResolver := ns.replicationResolver.Clone()

	cloned := &Namespace{
		info:          common.CloneProto(ns.info),
		config:        common.CloneProto(ns.config),
		configVersion: ns.configVersion,
		customSearchAttributesMapper: CustomSearchAttributesMapper{
			fieldToAlias: ns.customSearchAttributesMapper.fieldToAlias,
			aliasToField: ns.customSearchAttributesMapper.aliasToField,
		},
		notificationVersion: ns.notificationVersion,
		replicationResolver: clonedResolver,
	}

	for _, m := range mutations {
		m.apply(cloned)
	}

	return cloned
}

// VisibilityArchivalState observes the visibility archive configuration (state
// and URI) for this namespace.
func (ns *Namespace) VisibilityArchivalState() ArchivalConfigState {
	return ArchivalConfigState{
		State: ns.config.VisibilityArchivalState,
		URI:   ns.config.VisibilityArchivalUri,
	}
}

// HistoryArchivalState observes the history archive configuration (state and
// URI) for this namespace.
func (ns *Namespace) HistoryArchivalState() ArchivalConfigState {
	return ArchivalConfigState{
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
	if ns.info == nil {
		return ID("")
	}
	return ID(ns.info.Id)
}

// Name observes this namespace's configured name.
func (ns *Namespace) Name() Name {
	if ns.info == nil {
		return Name("")
	}
	return Name(ns.info.Name)
}

func (ns *Namespace) State() enumspb.NamespaceState {
	if ns.info == nil {
		return enumspb.NAMESPACE_STATE_UNSPECIFIED
	}
	return ns.info.State
}

func (ns *Namespace) ReplicationState() enumspb.ReplicationState {
	return ns.replicationResolver.ReplicationState()
}

// ActiveClusterName observes the name of the cluster that is currently active
// for this namspace.
func (ns *Namespace) ActiveClusterName(businessID string) string {
	return ns.replicationResolver.ActiveClusterName(businessID)
}

// ClusterNames observes the names of the clusters to which this namespace is
// replicated.
func (ns *Namespace) ClusterNames(businessID string) []string {
	return ns.replicationResolver.ClusterNames(businessID)
}

// IsOnCluster returns true is namespace is registered on cluster otherwise false.
func (ns *Namespace) IsOnCluster(clusterName string) bool {
	for _, cluster := range ns.ClusterNames(EmptyBusinessID) {
		if cluster == clusterName {
			return true
		}
	}
	return false
}

// ConfigVersion return the namespace config version
func (ns *Namespace) ConfigVersion() int64 {
	return ns.configVersion
}

// FailoverVersion return the namespace failover version
func (ns *Namespace) FailoverVersion() int64 {
	return ns.replicationResolver.FailoverVersion(EmptyBusinessID)
}

// IsGlobalNamespace returns whether the namespace is a global namespace.
// Being a global namespace doesn't necessarily mean that there are multiple registered clusters for it, only that it
// has a failover version. To determine whether operations should be replicated for a namespace, see ReplicationPolicy.
func (ns *Namespace) IsGlobalNamespace() bool {
	return ns.replicationResolver.IsGlobalNamespace()
}

// FailoverNotificationVersion return the global notification version of when failover happened
func (ns *Namespace) FailoverNotificationVersion() int64 {
	return ns.replicationResolver.FailoverNotificationVersion()
}

// NotificationVersion return the global notification version of when namespace changed
func (ns *Namespace) NotificationVersion() int64 {
	return ns.notificationVersion
}

// ActiveInCluster returns whether the namespace is active, i.e. non global
// namespace or global namespace which active cluster is the provided cluster
func (ns *Namespace) ActiveInCluster(clusterName string) bool {
	if !ns.replicationResolver.IsGlobalNamespace() {
		// namespace is not a global namespace, meaning namespace is always
		// "active" within each cluster
		return true
	}
	return clusterName == ns.ActiveClusterName(EmptyBusinessID)
}

// ReplicationPolicy return the derived workflow replication policy
func (ns *Namespace) ReplicationPolicy() ReplicationPolicy {
	// frontend guarantee that the clusters always contains the active
	// namespace, so if the # of clusters is 1 then we do not need to send out
	// any events for replication
	if ns.replicationResolver.IsGlobalNamespace() && len(ns.ClusterNames(EmptyBusinessID)) > 1 {
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

// Retention returns retention duration for this namespace.
func (ns *Namespace) Retention() time.Duration {
	if ns.config.Retention == nil {
		return 0
	}

	return ns.config.Retention.AsDuration()
}

// CustomSearchAttributesMapper is a part of temporary solution. Do not use this method.
func (ns *Namespace) CustomSearchAttributesMapper() CustomSearchAttributesMapper {
	return ns.customSearchAttributesMapper
}

func (ns *Namespace) GetWorkflowRules() []*rulespb.WorkflowRule {
	if ns.config.WorkflowRules == nil {
		return nil
	}
	return expmaps.Values(ns.config.WorkflowRules)
}

func (ns *Namespace) GetWorkflowRule(ruleID string) (*rulespb.WorkflowRule, bool) {
	if ns.config.WorkflowRules == nil {
		return nil, false
	}
	result, ok := ns.config.WorkflowRules[ruleID]
	return result, ok
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
	return e.info.CreateTime.AsTime()
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

func (m *CustomSearchAttributesMapper) GetAlias(fieldName string, namespace string) (string, error) {
	alias, ok := m.fieldToAlias[fieldName]
	if !ok {
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for field name %s", namespace, fieldName),
		)
	}
	return alias, nil
}

func (m *CustomSearchAttributesMapper) GetFieldName(alias string, namespace string) (string, error) {
	fieldName, ok := m.aliasToField[alias]
	if !ok {
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for search attribute %s", namespace, alias),
		)
	}
	return fieldName, nil
}

func (m *CustomSearchAttributesMapper) FieldToAliasMap() map[string]string {
	return maps.Clone(m.fieldToAlias)
}
