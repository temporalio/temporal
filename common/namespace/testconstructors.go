package namespace

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

// TODO: delete this whole file and transition usages to FromPersistentState

// NewLocalNamespaceForTest returns an entry with test data
func NewLocalNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	targetCluster string,
) *Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info:   ensureInfo(info),
		Config: ensureConfig(config),
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
			Clusters:          []string{targetCluster},
		},
		FailoverVersion: common.EmptyVersion,
	}
	factory := NewDefaultReplicationResolverFactory()
	resolver := factory(detail)
	ns, _ := FromPersistentState(detail, resolver, WithGlobalFlag(false))
	return ns
}

// NewNamespaceForTest returns an entry with test data
func NewNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	isGlobalNamespace bool,
	repConfig *persistencespb.NamespaceReplicationConfig,
	failoverVersion int64,
) *Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info:              ensureInfo(info),
		Config:            ensureConfig(config),
		ReplicationConfig: ensureRepConfig(repConfig),
		FailoverVersion:   failoverVersion,
	}
	factory := NewDefaultReplicationResolverFactory()
	resolver := factory(detail)
	ns, _ := FromPersistentState(detail, resolver, WithGlobalFlag(isGlobalNamespace))
	return ns
}

// newGlobalNamespaceForTest returns an entry with test data
func NewGlobalNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	repConfig *persistencespb.NamespaceReplicationConfig,
	failoverVersion int64,
) *Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info:              ensureInfo(info),
		Config:            ensureConfig(config),
		ReplicationConfig: ensureRepConfig(repConfig),
		FailoverVersion:   failoverVersion,
	}
	factory := NewDefaultReplicationResolverFactory()
	resolver := factory(detail)
	ns, _ := FromPersistentState(detail, resolver, WithGlobalFlag(true))
	return ns
}

func ensureInfo(proto *persistencespb.NamespaceInfo) *persistencespb.NamespaceInfo {
	if proto == nil {
		return &persistencespb.NamespaceInfo{}
	}
	return proto
}

func ensureConfig(proto *persistencespb.NamespaceConfig) *persistencespb.NamespaceConfig {
	if proto == nil {
		return &persistencespb.NamespaceConfig{}
	}
	return proto
}

func ensureRepConfig(proto *persistencespb.NamespaceReplicationConfig) *persistencespb.NamespaceReplicationConfig {
	if proto == nil {
		return &persistencespb.NamespaceReplicationConfig{}
	}
	return proto
}
