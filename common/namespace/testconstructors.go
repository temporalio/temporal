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
	return &Namespace{
		info:              ensureInfo(info),
		config:            ensureConfig(config),
		isGlobalNamespace: false,
		replicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
			Clusters:          []string{targetCluster},
		},
		failoverVersion: common.EmptyVersion,
	}
}

// NewNamespaceForTest returns an entry with test data
func NewNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	isGlobalNamespace bool,
	repConfig *persistencespb.NamespaceReplicationConfig,
	failoverVersion int64,
) *Namespace {
	return &Namespace{
		info:              ensureInfo(info),
		config:            ensureConfig(config),
		isGlobalNamespace: isGlobalNamespace,
		replicationConfig: ensureRepConfig(repConfig),
		failoverVersion:   failoverVersion,
	}
}

// newGlobalNamespaceForTest returns an entry with test data
func NewGlobalNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	repConfig *persistencespb.NamespaceReplicationConfig,
	failoverVersion int64,
) *Namespace {
	return &Namespace{
		info:              ensureInfo(info),
		config:            ensureConfig(config),
		isGlobalNamespace: true,
		replicationConfig: ensureRepConfig(repConfig),
		failoverVersion:   failoverVersion,
	}
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
