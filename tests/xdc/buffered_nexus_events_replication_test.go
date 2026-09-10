package xdc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
)

// TestBufferedNexusEventsReapplySharedOperationAndSkipLosingOnlyOperation buffers completion of an
// operation shared by both branches plus start and completion of an operation created only on the
// losing branch. It expects all three events on the loser, only the shared completion on the winner,
// and the losing-only operation skipped. This covers temporalio/temporal#10986.
func (s *NexusStateReplicationSuite) TestBufferedNexusEventsReapplySharedOperationAndSkipLosingOnlyOperation() {
	if !s.enableTransitionHistory || s.chasmEnabled {
		s.T().Skip("this conflict-reapplication regression is specific to transition-history HSM Nexus operations")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name

	// Phase 1: establish identical history with one shared, started Nexus operation.
	endpointName, operationCallbacks, allowLosingOnlyOperationStart := s.setupBufferedNexusEndpoint(ctx)

	workflowID := "buffered-nexus-conflict-" + uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: "buffered-nexus-conflict", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: workflowID,
		TaskQueue:  taskQueue,
	})

	firstWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     firstWorkflowTask,
		Commands: []*commandpb.Command{scheduleBufferedNexusOperationCommand(endpointName, "shared-operation")},
	})
	sharedOperationCallback := receiveBufferedNexusCallback(ctx, s.T(), operationCallbacks, "shared-operation")
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 0, ns, execution)
		sharedOperationScheduledEventID := findNexusScheduledEventID(history, "shared-operation")
		require.Positive(t, sharedOperationScheduledEventID)
		require.True(t, hasNexusEventForScheduledID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, sharedOperationScheduledEventID))
	}, replicationWaitTime, replicationCheckInterval)
	sharedOperationScheduledEventID := findNexusScheduledEventID(s.getWorkflowHistory(ctx, s.T(), 0, ns, execution), "shared-operation")
	s.Require().Positive(sharedOperationScheduledEventID)
	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.True(t, hasNexusEventForScheduledID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, sharedOperationScheduledEventID))
	}, replicationWaitTime, replicationCheckInterval)

	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)

	// Phase 2: hold a workflow task and buffer completions for shared and losing-only operations.
	s.signalWorkflow(ctx, workflowSignal{
		Namespace:  ns,
		Execution:  execution,
		SignalName: "create-losing-operation",
	})
	scheduleLosingOnlyOperationTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.completeWorkflowTaskAndScheduleNext(ctx, workflowTaskCompletion{
		Task:     scheduleLosingOnlyOperationTask,
		Commands: []*commandpb.Command{scheduleBufferedNexusOperationCommand(endpointName, "losing-only-operation")},
	})
	heldWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)
	losingOnlyOperationScheduledEventID := findNexusScheduledEventID(heldWorkflowTask.History.Events, "losing-only-operation")
	s.Require().Positive(losingOnlyOperationScheduledEventID)
	close(allowLosingOnlyOperationStart)
	losingOnlyOperationCallback := receiveBufferedNexusCallback(ctx, s.T(), operationCallbacks, "losing-only-operation")

	s.completeBufferedNexusOperation(ctx, bufferedNexusOperationCompletion{
		Callback: sharedOperationCallback,
		Result:   "shared-result",
	})
	s.completeBufferedNexusOperation(ctx, bufferedNexusOperationCompletion{
		Callback: losingOnlyOperationCallback,
		Result:   "losing-result",
	})
	s.assertBufferedEventTypes(ctx, bufferedEventExpectation{
		Namespace: ns,
		Execution: execution,
		EventTypes: []enumspb.EventType{
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		},
	})

	// Phase 3: fail over and create a winning branch on the new active cluster.
	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, activeClusterSignal{
		Namespace:  namespace,
		Execution:  execution,
		SignalName: "nexus-winner-signal",
	})

	// Phase 4: resolve the conflict and verify losing-branch storage versus reapplication.
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	losingHistory := s.findBufferedNexusLosingBranch(ctx, bufferedNexusLosingBranch{
		Namespace:        namespace,
		Execution:        execution,
		ScheduledEventID: losingOnlyOperationScheduledEventID,
	})
	s.Require().True(hasNexusEventForScheduledID(losingHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, losingOnlyOperationScheduledEventID))
	s.Require().True(hasNexusEventForScheduledID(losingHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, losingOnlyOperationScheduledEventID))
	s.Require().Equal(2, countBufferedEventType(losingHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED))

	for range 10 {
		targetHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
		if hasNexusEventForScheduledID(targetHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, sharedOperationScheduledEventID) {
			break
		}
		s.releaseReplicationTask(ctx, replicationToNewActive)
	}
	targetHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	s.Require().True(hasNexusEventForScheduledID(targetHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, sharedOperationScheduledEventID))
	s.Require().False(hasNexusEventForScheduledID(targetHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, losingOnlyOperationScheduledEventID))
	s.Require().False(hasNexusEventForScheduledID(targetHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, losingOnlyOperationScheduledEventID))
	await.Require(ctx, s.T(), func(t *await.T) {
		describeResponse, describeErr := s.clusters[1].FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: execution,
		})
		require.NoError(t, describeErr)
		require.Empty(t, describeResponse.PendingNexusOperations)
	}, replicationWaitTime, replicationCheckInterval)

	for attempt := 0; attempt < 10 && !s.bufferedEventsHistoriesEqual(ctx, ns, execution); attempt++ {
		s.releaseReplicationTask(ctx, replicationToOldActive)
	}
	await.Require(ctx, s.T(), func(t *await.T) {
		require.True(t, s.bufferedEventsHistoriesEqual(t.Context(), ns, execution))
	}, replicationWaitTime, replicationCheckInterval)
}

// TestNaturallyBufferedNexusOutcomesFlushedAndReapplied buffers failed, canceled, timed-out, and
// cancel-request-failed outcomes for operations shared by both branches. It expects the terminal operation
// outcomes to be reapplied to the winner and the non-cherry-pickable cancellation result to remain on the loser.
func (s *NexusStateReplicationSuite) TestNaturallyBufferedNexusOutcomesFlushedAndReapplied() {
	if !s.enableTransitionHistory || s.chasmEnabled {
		s.T().Skip("this conflict-reapplication regression is specific to transition-history HSM Nexus operations")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name

	callbacks := make(chan bufferedNexusCallback, 4)
	allowCancellationResponse := make(chan struct{})
	handler := nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, operation string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbacks <- bufferedNexusCallback{
				operation: operation,
				url:       options.CallbackURL,
				token:     options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: operation}, nil
		},
		OnCancelOperation: func(_ context.Context, _, operation, _ string, _ nexus.CancelOperationOptions) error {
			<-allowCancellationResponse
			if operation == "cancel-failed" {
				return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected cancellation failure")
			}
			return nil
		},
	}
	endpointName := s.createBufferedNexusEndpoint(ctx, handler)

	workflowID := "buffered-nexus-outcomes-" + uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: workflowID, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: workflowID,
		TaskQueue:  taskQueue,
	})
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	operations := []string{"failed", "canceled", "timed-out", "cancel-failed"}
	commands := make([]*commandpb.Command, 0, len(operations))
	for _, operation := range operations {
		timeout := time.Minute
		if operation == "timed-out" {
			timeout = 5 * time.Second
		}
		commands = append(commands, scheduleBufferedNexusOperationCommandWithTimeout(endpointName, operation, timeout))
	}
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     firstTask,
		Commands: commands,
	})

	operationCallbacks := make(map[string]bufferedNexusCallback, len(operations))
	for range operations {
		callback := receiveAnyBufferedNexusCallback(ctx, s.T(), callbacks)
		operationCallbacks[callback.operation] = callback
	}
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 0, ns, execution)
		for _, operation := range operations {
			scheduledID := findNexusScheduledEventID(history, operation)
			require.Positive(t, scheduledID)
			require.True(t, hasNexusEventForScheduledID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, scheduledID))
		}
	}, replicationWaitTime, replicationCheckInterval)
	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		for _, operation := range operations {
			scheduledID := findNexusScheduledEventID(history, operation)
			require.Positive(t, scheduledID)
			require.True(t, hasNexusEventForScheduledID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, scheduledID))
		}
	}, replicationWaitTime, replicationCheckInterval)
	await.Require(ctx, s.T(), func(t *await.T) {
		response, describeErr := s.clusters[1].FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: execution,
		})
		require.NoError(t, describeErr)
		require.Len(t, response.PendingNexusOperations, len(operations))
	}, replicationWaitTime, replicationCheckInterval)
	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)

	triggerTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	history := triggerTask.History.Events
	scheduledIDs := make(map[string]int64, len(operations))
	for _, operation := range operations {
		scheduledIDs[operation] = findNexusScheduledEventID(history, operation)
		s.Require().Positive(scheduledIDs[operation])
	}
	s.completeWorkflowTaskAndScheduleNext(ctx, workflowTaskCompletion{
		Task: triggerTask,
		Commands: []*commandpb.Command{
			requestCancelNexusOperationCommand(findNexusScheduledEventID(history, "cancel-failed")),
		},
	})
	heldTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldTask.TaskToken)
	close(allowCancellationResponse)

	s.failNexusOperation(ctx, operationCallbacks["failed"])
	s.cancelBufferedNexusOperation(ctx, operationCallbacks["canceled"])
	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
	}
	s.assertBufferedEventTypesPresent(ctx, bufferedEventExpectation{
		Namespace:  ns,
		Execution:  execution,
		EventTypes: expectedTypes,
	})

	losingHistory := s.finishNaturallyBufferedConflict(ctx, naturallyBufferedConflict{
		Namespace:              namespace,
		Execution:              execution,
		ReplicationToOldActive: replicationToOldActive,
		ReplicationToNewActive: replicationToNewActive,
		ExpectedEventTypes:     expectedTypes,
		WinnerSignal:           "nexus-outcomes-winner-signal",
	})
	for _, eventType := range expectedTypes {
		s.Require().NotNil(findHistoryEvent(losingHistory, eventType, nil))
	}
	winningHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	for operation, eventType := range map[string]enumspb.EventType{
		"failed":    enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		"canceled":  enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		"timed-out": enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
	} {
		s.Require().True(
			hasNexusEventForScheduledID(winningHistory, eventType, scheduledIDs[operation]),
			"%s for common operation %q must be reapplied to the winning branch",
			eventType,
			operation,
		)
	}
	s.Require().False(
		hasNexusEventForScheduledID(winningHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED, scheduledIDs["cancel-failed"]),
		"Nexus cancellation results are not cherry-pickable and must remain only on the losing branch",
	)
}

// TestNaturallyBufferedNexusCancelRequestCompletedFlushedAndReapplied buffers a successful Nexus
// cancel-request result. It expects the result to be persisted on the losing branch but skipped on the
// winner because cancellation results are not cherry-pickable.
func (s *NexusStateReplicationSuite) TestNaturallyBufferedNexusCancelRequestCompletedFlushedAndReapplied() {
	if !s.enableTransitionHistory || s.chasmEnabled {
		s.T().Skip("this conflict-reapplication regression is specific to transition-history HSM Nexus operations")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name

	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	allowCancellationResponse := make(chan struct{})
	handler := nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, operation string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			started <- struct{}{}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: operation}, nil
		},
		OnCancelOperation: func(_ context.Context, _, _, _ string, _ nexus.CancelOperationOptions) error {
			canceled <- struct{}{}
			<-allowCancellationResponse
			return nil
		},
	}
	endpointName := s.createBufferedNexusEndpoint(ctx, handler)
	workflowID := "buffered-nexus-cancel-completed-" + uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: workflowID, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: workflowID,
		TaskQueue:  taskQueue,
	})
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     firstTask,
		Commands: []*commandpb.Command{scheduleBufferedNexusOperationCommand(endpointName, "cancel-completed")},
	})
	select {
	case <-started:
	case <-ctx.Done():
		s.FailNow("timed out waiting for Nexus operation to start")
	}
	triggerTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	scheduledID := findNexusScheduledEventID(triggerTask.History.Events, "cancel-completed")
	s.Require().Positive(scheduledID)
	s.waitForClusterSynced()
	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)
	s.completeWorkflowTaskAndScheduleNext(ctx, workflowTaskCompletion{
		Task:     triggerTask,
		Commands: []*commandpb.Command{requestCancelNexusOperationCommand(scheduledID)},
	})
	heldTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldTask.TaskToken)
	close(allowCancellationResponse)
	select {
	case <-canceled:
	case <-ctx.Done():
		s.FailNow("timed out waiting for Nexus cancellation handler")
	}
	expectedTypes := []enumspb.EventType{enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED}
	s.assertBufferedEventTypesPresent(ctx, bufferedEventExpectation{
		Namespace:  ns,
		Execution:  execution,
		EventTypes: expectedTypes,
	})

	s.finishNaturallyBufferedConflict(ctx, naturallyBufferedConflict{
		Namespace:              namespace,
		Execution:              execution,
		ReplicationToOldActive: replicationToOldActive,
		ReplicationToNewActive: replicationToNewActive,
		ExpectedEventTypes:     expectedTypes,
		WinnerSignal:           "nexus-cancel-completed-winner-signal",
	})
	winningHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	s.Require().False(
		hasNexusEventForScheduledID(winningHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED, scheduledID),
		"Nexus cancellation results are not cherry-pickable and must remain only on the losing branch",
	)
}
