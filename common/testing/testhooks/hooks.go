package testhooks

import (
	"context"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	historytasks "go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
)

type (
	FrontendServiceRefs struct {
		RPCFactory      any
		HistoryClient   any
		MatchingClient  any
		SchedulerClient any
	}

	HistoryServiceRefs struct {
		ChasmEngine            any
		ChasmVisibilityManager any
		ChasmRegistry          any
	}
)

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
	HistoryReplicationTaskInterceptor        = newKey[func(*replicationspb.ReplicationTask, func() error) error, global]()
	HistoryReplicationDLQWriteInterceptor    = newKey[func(*persistencespb.ReplicationTaskInfo, func() error) error, global]()
	HistoryTransferTaskInterceptor           = newKey[func(historytasks.Task, func()), namespace.ID]()
	HistoryDLQTaskDeleteInterceptor          = newKey[func(context.Context, *historyservice.DeleteDLQTasksRequest, func(context.Context, *historyservice.DeleteDLQTasksRequest) (*historyservice.DeleteDLQTasksResponse, error)) (*historyservice.DeleteDLQTasksResponse, error), global]()
	NamespaceReplicationTaskInterceptor      = newKey[func(context.Context, *replicationspb.NamespaceTaskAttributes, func() error) error, namespace.Name]()
	ServiceGrpcInterceptors                  = newKey[func(primitives.ServiceName, *[]grpc.UnaryServerInterceptor, *[]grpc.StreamServerInterceptor), global]()
	ServiceClientDialOptions                 = newKey[func(map[primitives.ServiceName][]grpc.DialOption), global]()
	NamespaceRegistryCreated                 = newKey[func(primitives.ServiceName, namespace.Registry), global]()
	ChasmRegistryInitializer                 = newKey[func(any) error, global]()
	FrontendServiceRefsCreated               = newKey[func(FrontendServiceRefs), global]()
	HistoryServiceRefsCreated                = newKey[func(HistoryServiceRefs), global]()
	PersistenceExecutionManagerWrapper       = newKey[func(any, log.Logger) any, global]()
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
