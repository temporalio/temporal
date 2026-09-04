package temporaltest_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testhooks"
	historytasks "go.temporal.io/server/service/history/tasks"
	historyworkflow "go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/localexecution"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/temporaltest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	localFirstDemoTaskQueue = "local-first-steel-thread"
	localFirstDemoActivity  = "local-first-demo-activity"
)

func TestLocalFirstSteelThread(t *testing.T) {
	for _, test := range []struct {
		name            string
		globalNamespace bool
	}{
		{name: "local namespace"},
		{name: "global namespace", globalNamespace: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testLocalFirstSteelThread(t, test.globalNamespace)
		})
	}
}

func testLocalFirstSteelThread(t *testing.T, globalNamespace bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	testHooks := testhooks.NewTestHooks()
	failureHook := &failAfterHistoryAppendHook{}

	upstreamOptions := []temporaltest.TestServerOption{
		temporaltest.WithT(t),
		temporaltest.WithDynamicConfig(dynamicconfig.EnableLocalExecution, true),
		temporaltest.WithBaseServerOptions(temporal.WithTestHooks(testHooks)),
	}
	if globalNamespace {
		upstreamOptions = append(upstreamOptions, temporaltest.WithGlobalNamespace())
	}
	upstream := temporaltest.NewServer(upstreamOptions...)
	localOptions := []temporaltest.TestServerOption{temporaltest.WithT(t)}
	if globalNamespace {
		localOptions = append(localOptions, temporaltest.WithGlobalNamespace())
	} else {
		localOptions = append(localOptions, temporaltest.WithGlobalNamespaceSupport())
	}
	local := temporaltest.NewServer(localOptions...)

	upstreamAdmin, upstreamNamespaceID := localFirstAdminClient(ctx, t, upstream)
	localAdmin, localNamespaceID := localFirstAdminClient(ctx, t, local)
	t.Cleanup(testhooks.Set[testhooks.HistoryPassiveReplicationTestHook](
		testHooks,
		testhooks.HistoryPassiveReplicationTest,
		failureHook,
		namespace.ID(upstreamNamespaceID),
	))

	run, err := upstream.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			ID:                  "local-first-steel-thread",
			TaskQueue:           localFirstDemoTaskQueue,
			WorkflowTaskTimeout: time.Minute,
		},
		localFirstLoopWorkflow,
		3,
	)
	require.NoError(t, err)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}
	acquisition, err := upstream.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: upstream.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: localFirstDemoTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "local-first-steel-thread",
			LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
				LocalServerId:          "local-first-steel-thread",
				ProtocolVersion:        localexecution.ProtocolVersion,
				SyncInterval:           durationpb.New(time.Minute),
				RequestedLeaseDuration: durationpb.New(3 * time.Minute),
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, execution, acquisition.GetWorkflowExecution())
	ownership := acquisition.GetLocalExecutionInfo()
	require.NotNil(t, ownership)
	localFirstWaitForHistory(ctx, t, local, execution)
	upstreamToLocal, err := localexecution.NewBaselineImporter(
		localexecution.HistoryEndpoint{
			NamespaceID: upstreamNamespaceID,
			AdminClient: upstreamAdmin,
		},
		localexecution.HistoryEndpoint{
			Namespace:   local.GetDefaultNamespace(),
			AdminClient: localAdmin,
		},
	)
	require.NoError(t, err)

	baseline, err := upstreamToLocal.Import(ctx, execution)
	require.NoError(t, err)
	require.Equal(t, int64(2), baseline.LastEventID)
	t.Logf("imported upstream baseline through event %d", baseline.LastEventID)

	local.NewWorker(localFirstDemoTaskQueue, func(registry worker.Registry) {
		registry.RegisterWorkflow(localFirstLoopWorkflow)
		registry.RegisterActivityWithOptions(
			func(iteration int) (string, error) {
				return fmt.Sprintf("activity-%d", iteration), nil
			},
			activity.RegisterOptions{Name: localFirstDemoActivity},
		)
	})

	var localResult []string
	err = local.GetDefaultClient().GetWorkflow(
		ctx,
		execution.WorkflowId,
		execution.RunId,
	).Get(ctx, &localResult)
	require.NoError(t, err)
	require.Equal(t, []string{"activity-0", "activity-1", "activity-2"}, localResult)

	localHistory := localFirstHistory(ctx, t, local, execution)
	require.Equal(t, 3, countEventType(localHistory, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED))
	require.Equal(t, 3, countEventType(localHistory, enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED))
	require.Equal(t, 3, countEventType(localHistory, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED))
	require.GreaterOrEqual(t, countEventType(localHistory, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED), 4)

	upstreamBeforeSync := localFirstHistory(ctx, t, upstream, execution)
	require.Len(t, upstreamBeforeSync, 2)
	t.Logf(
		"local history reached %d events across three full Activities while upstream remained at %d",
		len(localHistory),
		len(upstreamBeforeSync),
	)
	localToUpstream, err := localexecution.NewHistoryReplicator(
		localexecution.HistoryEndpoint{
			NamespaceID: localNamespaceID,
			AdminClient: localAdmin,
		},
		localexecution.ReplicationTarget{
			Namespace:      upstream.GetDefaultNamespace(),
			LocalServerID:  "local-first-steel-thread",
			OwnershipToken: ownership.GetOwnershipToken(),
			FencingEpoch:   ownership.GetFencingEpoch(),
			AdminClient:    upstreamAdmin,
		},
		localexecution.SyncCursor{
			EventID: baseline.LastEventID,
			Version: baseline.LastEventVersion,
		},
	)
	require.NoError(t, err)

	// Exercise the persistence failure window explicitly. History nodes may have
	// been appended, but the cursor and mutable state must remain at the previous
	// completed synchronization point and a retry must remain possible.
	failureHook.enabled.Store(true)
	_, err = localToUpstream.Sync(ctx, execution)
	require.Error(t, err)
	t.Logf("injected synchronization failure: %v", err)
	require.False(t, failureHook.enabled.Load(), "failure hook was not exercised")
	require.Len(t, localFirstHistory(ctx, t, upstream, execution), 2)
	mutableState, err := upstreamAdmin.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: upstream.GetDefaultNamespace(),
		Execution: execution,
	})
	require.NoError(t, err)
	localExecutionInfo := mutableState.GetDatabaseMutableState().GetExecutionInfo().GetLocalExecutionInfo()
	require.Equal(t, baseline.LastEventID, localExecutionInfo.GetLastSynchronizedEventId())
	require.Equal(t, baseline.LastEventVersion, localExecutionInfo.GetLastSynchronizedEventVersion())
	require.Empty(t, localExecutionInfo.GetLastSyncId())

	syncCtx, stopSync := context.WithCancel(ctx)
	defer stopSync()
	syncResults := make(chan localexecution.SyncResult, 1)
	syncErrors := make(chan error, 1)
	go func() {
		syncErrors <- localToUpstream.Run(syncCtx, execution, 100*time.Millisecond, func(result localexecution.SyncResult) {
			syncResults <- result
		})
	}()

	var syncResult localexecution.SyncResult
	select {
	case syncResult = <-syncResults:
		stopSync()
		require.NoError(t, <-syncErrors)
	case err := <-syncErrors:
		require.FailNow(t, "periodic history synchronization stopped before producing a result", err)
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for periodic history synchronization")
	}

	upstreamAfterSync := localFirstHistory(ctx, t, upstream, execution)
	protorequire.ProtoSliceEqual(t, localHistory, upstreamAfterSync)
	description, err := upstream.GetDefaultClient().DescribeWorkflowExecution(
		ctx,
		execution.WorkflowId,
		execution.RunId,
	)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.GetWorkflowExecutionInfo().GetStatus())
	var upstreamResult []string
	err = upstream.GetDefaultClient().GetWorkflow(
		ctx,
		execution.WorkflowId,
		execution.RunId,
	).Get(ctx, &upstreamResult)
	require.NoError(t, err)
	require.Equal(t, localResult, upstreamResult)
	t.Logf(
		"periodic sync copied %d history batches through event %d; upstream now has %d events",
		syncResult.HistoryBatches,
		syncResult.LastEventID,
		len(upstreamAfterSync),
	)

	_, err = localToUpstream.Sync(ctx, execution)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	protorequire.ProtoSliceEqual(t, localHistory, localFirstHistory(ctx, t, upstream, execution))

	_, err = upstreamAdmin.SyncLocalExecution(ctx, &adminservice.SyncLocalExecutionRequest{
		Namespace:            upstream.GetDefaultNamespace(),
		Execution:            execution,
		ProtocolVersion:      1,
		LocalServerId:        "stale-local-server",
		SyncId:               "stale-sync",
		PreviousEventId:      baseline.LastEventID,
		PreviousEventVersion: baseline.LastEventVersion,
		NewEventId:           baseline.LastEventID,
		NewEventVersion:      baseline.LastEventVersion,
		VersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{{
				EventId: baseline.LastEventID,
				Version: baseline.LastEventVersion,
			}},
		},
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	protorequire.ProtoSliceEqual(t, localHistory, localFirstHistory(ctx, t, upstream, execution))
}

type failAfterHistoryAppendHook struct {
	enabled atomic.Bool
}

func (h *failAfterHistoryAppendHook) InterceptUpdate(
	ctx context.Context,
	input any,
	next func() error,
) error {
	request, ok := input.(*historyworkflow.TestHookUpdateExecutionRequest)
	if !ok {
		return errors.New("unexpected workflow update hook request")
	}
	if !h.enabled.CompareAndSwap(true, false) {
		if err := next(); err != nil {
			return err
		}
		if request.ExecutionContext.IsDirty() {
			return errors.New("successful workflow update left mutable state dirty")
		}
		return nil
	}
	if err := request.PrepareMutableStateTransaction(); err != nil {
		return err
	}
	payload, err := request.CloseMutableStateTransaction()
	if err != nil {
		return err
	}
	if _, err := request.ExecutionContext.PersistWorkflowEvents(
		ctx,
		request.ShardContext,
		payload.ExecutionEvents...,
	); err != nil {
		return err
	}
	request.ExecutionContext.Clear()
	return errors.New("injected failure after history append and before mutable-state update")
}

func (*failAfterHistoryAppendHook) UseTransientWorkflowContextForReplication(context.Context) bool {
	return false
}

func (*failAfterHistoryAppendHook) ShouldExecuteTaskAsPassive(historytasks.Task) bool {
	return false
}

func localFirstWaitForHistory(
	ctx context.Context,
	t *testing.T,
	server *temporaltest.TestServer,
	execution *commonpb.WorkflowExecution,
) {
	t.Helper()

	await.Require(ctx, t, func(t *await.T) {
		_, err := server.GetDefaultClient().DescribeWorkflowExecution(
			ctx,
			execution.GetWorkflowId(),
			execution.GetRunId(),
		)
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
	}, 5*time.Second, 10*time.Millisecond)
}

func localFirstLoopWorkflow(ctx workflow.Context, iterations int) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})

	results := make([]string, 0, iterations)
	for i := 0; i < iterations; i++ {
		var result string
		if err := workflow.ExecuteActivity(ctx, localFirstDemoActivity, i).Get(ctx, &result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func localFirstAdminClient(
	ctx context.Context,
	t *testing.T,
	server *temporaltest.TestServer,
) (adminservice.AdminServiceClient, string) {
	t.Helper()

	connection, err := grpc.NewClient(
		server.GetFrontendHostPort(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, connection.Close())
	})

	response, err := workflowservice.NewWorkflowServiceClient(connection).DescribeNamespace(
		ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: server.GetDefaultNamespace()},
	)
	require.NoError(t, err)
	return adminservice.NewAdminServiceClient(connection), response.GetNamespaceInfo().GetId()
}

func localFirstHistory(
	ctx context.Context,
	t *testing.T,
	server *temporaltest.TestServer,
	execution *commonpb.WorkflowExecution,
) []*historypb.HistoryEvent {
	t.Helper()

	response, err := server.GetDefaultClient().WorkflowService().GetWorkflowExecutionHistory(
		ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: server.GetDefaultNamespace(),
			Execution: execution,
		},
	)
	require.NoError(t, err)
	return response.GetHistory().GetEvents()
}

func countEventType(events []*historypb.HistoryEvent, eventType enumspb.EventType) int {
	var count int
	for _, event := range events {
		if event.GetEventType() == eventType {
			count++
		}
	}
	return count
}
