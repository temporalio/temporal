package testhooks

import (
	"context"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/rpc/httpfaults"
	historytasks "go.temporal.io/server/service/history/tasks"
	testxhooks "go.temporal.io/testx/testhooks"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooks),
)

// HistoryPassiveReplicationTestHook defines the test-only seams used to replace an
// active workflow update with a passive replication apply and exercise the resulting
// history tasks through standby execution. InterceptUpdate's payload is owned by
// service/history/workflow and is intentionally opaque here.
type HistoryPassiveReplicationTestHook interface {
	InterceptUpdate(context.Context, any, func() error) error
	UseTransientWorkflowContextForReplication(context.Context) bool
	ShouldExecuteTaskAsPassive(historytasks.Task) bool
}

// Test hook keys with their return type and scope.
// Try to avoid global scope as it requires a dedicated test cluster.
var (
	MatchingLBForceReadPartition                = testxhooks.NewKey[int, namespace.ID]()
	MatchingLBForceWritePartition               = testxhooks.NewKey[int, namespace.ID]()
	TaskQueuesInDeploymentSyncBatchSize         = testxhooks.NewGlobalKey[int]()
	MatchingIgnoreRoutingConfigRevisionCheck    = testxhooks.NewKey[bool, namespace.ID]()
	MatchingDeploymentRegisterErrorBackoff      = testxhooks.NewKey[time.Duration, namespace.ID]()
	HistoryReplicationTaskInterceptor           = testxhooks.NewGlobalKey[func(*replicationspb.ReplicationTask, func() error) error]()
	HistoryReplicationTaskConversionInterceptor = testxhooks.NewKey[func(historytasks.Task, func() (*replicationspb.ReplicationTask, error)) (*replicationspb.ReplicationTask, error), namespace.ID]()
	HistoryReplicationDLQWriteInterceptor       = testxhooks.NewGlobalKey[func(*persistencespb.ReplicationTaskInfo, func() error) error]()
	HistoryChasmRuntimeProvider                 = testxhooks.NewGlobalKey[func(chasm.Engine, chasm.VisibilityManager, *chasm.Registry)]()
	HistoryTransferTaskInterceptor              = testxhooks.NewKey[func(historytasks.Task, func()), namespace.ID]()
	NexusRetainTimedOutOperation                = testxhooks.NewKey[bool, namespace.Name]()
	// HistoryPassiveReplicationTest enables the single-cluster passive replication
	// test stack for one namespace. Production builds always report it as unset.
	HistoryPassiveReplicationTest       = testxhooks.NewKey[HistoryPassiveReplicationTestHook, namespace.ID]()
	HistoryDLQTaskDeleteInterceptor     = testxhooks.NewGlobalKey[func(context.Context, *historyservice.DeleteDLQTasksRequest, func(context.Context, *historyservice.DeleteDLQTasksRequest) (*historyservice.DeleteDLQTasksResponse, error)) (*historyservice.DeleteDLQTasksResponse, error)]()
	NamespaceReplicationTaskInterceptor = testxhooks.NewKey[func(context.Context, *replicationspb.NamespaceTaskAttributes, func() error) error, namespace.Name]()
	// NamespaceReplicationBeforeLocalCommit injects a test failure after the
	// metadata write succeeds but before the CHASM component commit begins.
	NamespaceReplicationBeforeLocalCommit     = testxhooks.NewKey[func(context.Context) error, namespace.Name]()
	GRPCRequestFaultGeneratorByNamespaceID    = testxhooks.NewKey[grpcfaults.RequestCallback, namespace.ID]()
	GRPCRequestFaultGeneratorByNamespaceName  = testxhooks.NewKey[grpcfaults.RequestCallback, namespace.Name]()
	GRPCResponseFaultGeneratorByNamespaceID   = testxhooks.NewKey[grpcfaults.ResponseCallback, namespace.ID]()
	GRPCResponseFaultGeneratorByNamespaceName = testxhooks.NewKey[grpcfaults.ResponseCallback, namespace.Name]()
	HTTPRequestFaultGeneratorByNamespaceID    = testxhooks.NewKey[httpfaults.RequestCallback, namespace.ID]()
	HTTPRequestFaultGeneratorByNamespaceName  = testxhooks.NewKey[httpfaults.RequestCallback, namespace.Name]()
	HTTPResponseFaultGeneratorByNamespaceID   = testxhooks.NewKey[httpfaults.ResponseCallback, namespace.ID]()
	HTTPResponseFaultGeneratorByNamespaceName = testxhooks.NewKey[httpfaults.ResponseCallback, namespace.Name]()
)

const (
	ScopeNamespace = testxhooks.ScopeValue
	ScopeGlobal    = testxhooks.ScopeGlobal
)
