package xdc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestParentChildReplicationGate(t *testing.T) {
	t.Run("passes unrelated tasks through", func(t *testing.T) {
		gate := newParentChildReplicationGate("namespace", "parent", "child")
		defer gate.close()

		executeErr := errors.New("execute failed")
		var executions atomic.Int32
		err := gate.intercept(
			newParentChildHistoryReplicationTask("other-namespace", "parent", 1),
			func() error {
				executions.Add(1)
				return executeErr
			},
		)
		require.ErrorIs(t, err, executeErr)
		require.Equal(t, int32(1), executions.Load())

		err = gate.intercept(
			newParentChildHistoryReplicationTask("namespace", "other-workflow", 2),
			func() error {
				executions.Add(1)
				return executeErr
			},
		)
		require.ErrorIs(t, err, executeErr)
		require.Equal(t, int32(2), executions.Load())
	})

	t.Run("applies and acknowledges without applying exactly once", func(t *testing.T) {
		gate := newParentChildReplicationGate("namespace", "parent", "child")
		defer gate.close()

		applyErr := errors.New("apply failed")
		var applied atomic.Int32
		applyResult := interceptParentChildTask(
			gate,
			newParentChildHistoryReplicationTask("namespace", "parent", 1),
			func() error {
				applied.Add(1)
				return applyErr
			},
		)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		deferredTask, err := gate.nextForWorkflow(ctx, "parent")
		cancel()
		require.NoError(t, err)
		require.Zero(t, applied.Load())
		require.ErrorIs(t, deferredTask.apply(), applyErr)
		require.ErrorIs(t, receiveParentChildInterceptResult(t, applyResult), applyErr)
		require.ErrorIs(t, deferredTask.apply(), errParentChildTaskAlreadyResolved)
		require.Equal(t, int32(1), applied.Load())

		var appliedWithoutPermission atomic.Int32
		ackResult := interceptParentChildTask(
			gate,
			newParentChildHistoryReplicationTask("namespace", "child", 2),
			func() error {
				appliedWithoutPermission.Add(1)
				return nil
			},
		)
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		deferredTask, err = gate.nextForWorkflow(ctx, "child")
		cancel()
		require.NoError(t, err)
		require.NoError(t, deferredTask.acknowledgeWithoutApplying())
		require.NoError(t, receiveParentChildInterceptResult(t, ackResult))
		require.ErrorIs(t, deferredTask.acknowledgeWithoutApplying(), errParentChildTaskAlreadyResolved)
		require.Zero(t, appliedWithoutPermission.Load())
	})

	t.Run("keeps tasks for the other workflow buffered", func(t *testing.T) {
		gate := newParentChildReplicationGate("namespace", "parent", "child")
		defer gate.close()

		childResult := interceptParentChildTask(
			gate,
			newParentChildHistoryReplicationTask("namespace", "child", 1),
			func() error { return nil },
		)
		await.RequireTrue(t, func() bool { return len(gate.pending) == 1 }, time.Second, time.Millisecond)
		parentResult := interceptParentChildTask(
			gate,
			newParentChildHistoryReplicationTask("namespace", "parent", 2),
			func() error { return nil },
		)
		await.RequireTrue(t, func() bool { return len(gate.pending) == 2 }, time.Second, time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		parentTask, err := gate.nextForWorkflow(ctx, "parent")
		cancel()
		require.NoError(t, err)
		require.Equal(t, int64(2), parentTask.task.GetSourceTaskId())
		require.Len(t, gate.buffered["child"], 1)
		require.NoError(t, parentTask.acknowledgeWithoutApplying())
		require.NoError(t, receiveParentChildInterceptResult(t, parentResult))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		childTask, err := gate.nextForWorkflow(ctx, "child")
		cancel()
		require.NoError(t, err)
		require.Equal(t, int64(1), childTask.task.GetSourceTaskId())
		require.NoError(t, childTask.acknowledgeWithoutApplying())
		require.NoError(t, receiveParentChildInterceptResult(t, childResult))
	})

	t.Run("close releases delayed and queued tasks", func(t *testing.T) {
		gate := newParentChildReplicationGate("namespace", "parent", "child")
		delayedResult := interceptParentChildTask(
			gate,
			newParentChildHistoryReplicationTask("namespace", "parent", 1),
			func() error { return nil },
		)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := gate.nextForWorkflow(ctx, "parent")
		cancel()
		require.NoError(t, err)

		queuedResult := interceptParentChildTask(
			gate,
			newParentChildHistoryReplicationTask("namespace", "child", 2),
			func() error { return nil },
		)
		await.RequireTrue(t, func() bool { return len(gate.pending) == 1 }, time.Second, time.Millisecond)

		gate.close()
		gate.close()
		require.NoError(t, receiveParentChildInterceptResult(t, delayedResult))
		require.NoError(t, receiveParentChildInterceptResult(t, queuedResult))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		_, err = gate.nextForWorkflow(ctx, "child")
		cancel()
		require.ErrorIs(t, err, errParentChildReplicationGateClosed)
	})
}

func TestParentChildHarnessAppliesPreviouslyDelayedTaskAfterActiveClusterChanges(t *testing.T) {
	gate := newParentChildReplicationGate("namespace", "parent", "child")
	defer gate.close()

	events := []*historypb.HistoryEvent{{
		EventId:   3,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	}}
	blob, err := serialization.NewSerializer().SerializeEvents(events)
	require.NoError(t, err)
	task := newParentChildHistoryReplicationTask("namespace", "parent", 1)
	task.GetHistoryTaskAttributes().Events = blob

	var executions atomic.Int32
	interceptResult := interceptParentChildTask(gate, task, func() error {
		executions.Add(1)
		return nil
	})
	runtime := &parentChildScenarioRuntime{
		parentID:     "parent",
		gates:        [2]*parentChildReplicationGate{nil, gate},
		delayedTasks: make(map[parentChildReplicationLane]*parentChildReplicationTask),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = delayReplicationAtTaskContainingEvent(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED).run(ctx, runtime)
	cancel()
	require.NoError(t, err)
	require.Zero(t, executions.Load())

	runtime.activeClusterIndex = int(initialStandbyCluster)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = applyDelayedReplication(initialStandbyCluster, parentWorkflow).run(ctx, runtime)
	cancel()
	require.NoError(t, err)
	require.NoError(t, receiveParentChildInterceptResult(t, interceptResult))
	require.Equal(t, int32(1), executions.Load())
	require.Contains(t, runtime.trace, "  apply delayed task 1 to cluster 1 for parent [WorkflowTaskStarted]")
}

func TestParentChildHarnessApplyDelayedReplicationRequiresDelayedTask(t *testing.T) {
	runtime := &parentChildScenarioRuntime{
		parentID:     "parent",
		delayedTasks: make(map[parentChildReplicationLane]*parentChildReplicationTask),
	}

	err := applyDelayedReplication(initialStandbyCluster, parentWorkflow).run(context.Background(), runtime)
	require.ErrorContains(t, err, "no delayed parent replication task to initial standby cluster")
}

func TestParentChildHarnessVersionedTransitionApplyThrough(t *testing.T) {
	gate := newParentChildReplicationGate("namespace", "parent", "child")
	defer gate.close()

	var verifyExecutions atomic.Int32
	verifyResult := interceptParentChildTask(
		gate,
		newParentChildVerifyVersionedTransitionTask("namespace", "parent", "run", 1),
		func() error {
			verifyExecutions.Add(1)
			return nil
		},
	)
	await.RequireTrue(t, func() bool { return len(gate.pending) == 1 }, time.Second, time.Millisecond)

	events := []*historypb.HistoryEvent{
		{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED},
	}
	blob, err := serialization.NewSerializer().SerializeEvents(events)
	require.NoError(t, err)
	var syncExecutions atomic.Int32
	syncResult := interceptParentChildTask(
		gate,
		newParentChildSyncVersionedTransitionTask("namespace", "parent", "run", 2, blob),
		func() error {
			syncExecutions.Add(1)
			return nil
		},
	)
	await.RequireTrue(t, func() bool { return len(gate.pending) == 2 }, time.Second, time.Millisecond)

	runtime := &parentChildScenarioRuntime{
		parentID:     "parent",
		gates:        [2]*parentChildReplicationGate{nil, gate},
		delayedTasks: make(map[parentChildReplicationLane]*parentChildReplicationTask),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = acknowledgeReplicationTaskContainingEventWithoutApplying(initialStandbyCluster, parentWorkflow, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED).run(ctx, runtime)
	cancel()
	require.NoError(t, err)
	require.NoError(t, receiveParentChildInterceptResult(t, verifyResult))
	require.NoError(t, receiveParentChildInterceptResult(t, syncResult))
	require.Equal(t, int32(1), verifyExecutions.Load())
	require.Zero(t, syncExecutions.Load())
	require.Contains(t, runtime.trace, "  apply task 1 to cluster 1 for parent [VerifyVersionedTransition]")
	require.Contains(t, runtime.trace, "  ack-without-apply task 2 to cluster 1 for parent [WorkflowTaskStarted, WorkflowTaskCompleted]")
}

func TestParentChildHarnessDecodeReplicationEvents(t *testing.T) {
	events := []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
	}
	blob, err := serialization.NewSerializer().SerializeEvents(events)
	require.NoError(t, err)

	t.Run("legacy history", func(t *testing.T) {
		task := newParentChildHistoryReplicationTask("namespace", "parent", 1)
		task.GetHistoryTaskAttributes().Events = blob
		actual, err := decodeParentChildReplicationEvents(task)
		require.NoError(t, err)
		protorequire.ProtoSliceEqual(t, events, actual)
	})

	t.Run("sync versioned transition", func(t *testing.T) {
		actual, err := decodeParentChildReplicationEvents(
			newParentChildSyncVersionedTransitionTask("namespace", "parent", "run", 1, blob),
		)
		require.NoError(t, err)
		protorequire.ProtoSliceEqual(t, events, actual)
	})

	t.Run("backfill history", func(t *testing.T) {
		actual, err := decodeParentChildReplicationEvents(
			newParentChildBackfillHistoryTask("namespace", "parent", "run", 1, blob),
		)
		require.NoError(t, err)
		protorequire.ProtoSliceEqual(t, events, actual)
	})

	t.Run("verify versioned transition", func(t *testing.T) {
		actual, err := decodeParentChildReplicationEvents(
			newParentChildVerifyVersionedTransitionTask("namespace", "parent", "run", 1),
		)
		require.NoError(t, err)
		require.Empty(t, actual)
	})
}

func newParentChildHistoryReplicationTask(
	namespaceID string,
	workflowID string,
	sourceTaskID int64,
) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		SourceTaskId: sourceTaskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
			},
		},
	}
}

func newParentChildSyncVersionedTransitionTask(
	namespaceID string,
	workflowID string,
	runID string,
	sourceTaskID int64,
	eventBatches ...*commonpb.DataBlob,
) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
		SourceTaskId: sourceTaskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{
					EventBatches: eventBatches,
				},
			},
		},
	}
}

func newParentChildVerifyVersionedTransitionTask(
	namespaceID string,
	workflowID string,
	runID string,
	sourceTaskID int64,
) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: sourceTaskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
}

func newParentChildBackfillHistoryTask(
	namespaceID string,
	workflowID string,
	runID string,
	sourceTaskID int64,
	eventBatches ...*commonpb.DataBlob,
) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK,
		SourceTaskId: sourceTaskID,
		Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId:  namespaceID,
				WorkflowId:   workflowID,
				RunId:        runID,
				EventBatches: eventBatches,
			},
		},
	}
}

func interceptParentChildTask(
	gate *parentChildReplicationGate,
	task *replicationspb.ReplicationTask,
	execute func() error,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- gate.intercept(task, execute)
	}()
	return result
}

func receiveParentChildInterceptResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replication interceptor")
		return nil
	}
}
