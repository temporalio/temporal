package testhooks

import (
	"context"
	"time"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace"
)

type NamespaceReplicationTaskExecutor func(context.Context, *replicationspb.NamespaceTaskAttributes) error

// Test hook keys with their return type and scope.
// Try to avoid global scope as it requires a dedicated test cluster.
var (
	MatchingDisableSyncMatch                 = newKey[bool, namespace.ID]()
	MatchingLBForceReadPartition             = newKey[int, namespace.ID]()
	MatchingLBForceWritePartition            = newKey[int, namespace.ID]()
	UpdateWithStartInBetweenLockAndStart     = newKey[func(), namespace.ID]()
	UpdateWithStartOnClosingWorkflowRetry    = newKey[func(), namespace.ID]()
	TaskQueuesInDeploymentSyncBatchSize      = newKey[int, global]()
	MatchingIgnoreRoutingConfigRevisionCheck = newKey[bool, namespace.ID]()
	MatchingDeploymentRegisterErrorBackoff   = newKey[time.Duration, namespace.ID]()
	MatchingForwardTaskDelay                 = newKey[time.Duration, namespace.ID]()
	HistoryReplicationDLQWrite               = newKey[func(any), namespace.ID]()
	HistoryReplicationTaskBeforeExecute      = newKey[func(any) error, namespace.ID]()
	HistoryReplicationTaskAfterExecute       = newKey[func(any), namespace.ID]()
	HistoryReplicationTaskAfterConvert       = newKey[func(string, any, any) any, global]()
	HistoryTaskInterceptor                   = newKey[func(any) (any, bool), namespace.ID]()
	HistoryTaskQueueBeforeDeleteTasks        = newKey[func(any) error, global]()
	NamespaceReplicationTaskBeforeExecute    = newKey[func(context.Context, *replicationspb.NamespaceTaskAttributes, NamespaceReplicationTaskExecutor) error, namespace.Name]()
	NamespaceReplicationTaskAfterExecute     = newKey[func(any), namespace.Name]()
)

// keyID is a unique identifier for a key, used as a map key.
type keyID = int64

// global is the scope type for global hooks.
type global struct{}

// GlobalScope is the singleton value for global hooks.
var GlobalScope = global{}

// ScopeType indicates the scope of a hook at runtime.
type ScopeType int

const (
	ScopeNamespace ScopeType = iota
	ScopeGlobal
)

type Key[T any, S any] struct {
	id        keyID
	scopeType ScopeType
}
