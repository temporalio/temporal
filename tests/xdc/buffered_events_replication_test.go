package xdc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/testing/await"
)

// Four values in HistoryBuilder's buffered-event set cannot be naturally
// buffered on current main. EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
// EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY, and
// EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY have no production emitter.
// EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED has an emitter, but
// its production precondition rejects every workflow with a pending workflow
// task, so it cannot enter the buffer. The tests below cover every other value
// through its production API, transfer task, timer, callback, or reapplier.

// TestNaturallyBufferedInputsFlushedAndReappliedAfterFailover buffers an activity result, timer,
// cancel request, signal, options update, pause, and unpause behind one workflow task; conflict
// reapplication then buffers an update-admitted event on the winner. It expects external inputs and
// the update to reach the winner while activity, timer, and pause state remain only on the losing branch.
func (s *FunctionalClustersTestSuite) TestNaturallyBufferedInputsFlushedAndReappliedAfterFailover() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name
	s.enableWorkflowPauseForTest()

	// Phase 1: establish identical history with one pending activity.
	execution, taskQueue := s.startWorkflowWithPendingActivity(ctx, ns)
	workflowID := execution.WorkflowId
	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.NotEmpty(t, history)
		require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, history[len(history)-1].EventType)
	}, replicationWaitTime, replicationCheckInterval)

	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)

	// Phase 2: hold a workflow task and create the losing branch's buffered events.
	updateID := "buffered-update-" + uuid.NewString()
	s.acceptUpdateAndStartTimer(ctx, ns, execution, taskQueue, updateID)

	heldWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)

	optionsRequestID := s.completeActivityAndBufferExternalEvents(ctx, ns, execution, taskQueue)
	naturallyBufferedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED,
	}
	bufferedInputs := bufferedInputsExpectation{
		Namespace:        namespace,
		Execution:        execution,
		UpdateID:         updateID,
		OptionsRequestID: optionsRequestID,
		EventTypes:       naturallyBufferedTypes,
	}
	s.assertBufferedEventTypes(ctx, bufferedEventExpectation{
		Namespace:  ns,
		Execution:  execution,
		EventTypes: naturallyBufferedTypes,
	})

	// Phase 3: fail over and create a winning branch on the new active cluster.
	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, activeClusterSignal{
		Namespace:  namespace,
		Execution:  execution,
		SignalName: "winner-signal",
	})
	winnerWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 1, ns, taskQueue)
	s.Require().NotEmpty(winnerWorkflowTask.TaskToken)

	// Phase 4: resolve the conflict and verify losing-branch storage versus reapplication.
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	s.assertBufferedEventsPersistedOnLosingBranch(ctx, bufferedInputs)

	// Reapplying the losing update while the winner's workflow task is running
	// naturally creates UpdateAdmitted as a buffered event.
	for attempt := 0; attempt < 10 && !s.hasBufferedEventType(ctx, 1, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED); attempt++ {
		s.releaseReplicationTask(ctx, replicationToNewActive)
	}
	s.Require().True(s.hasBufferedEventType(ctx, 1, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED))
	await.Require(ctx, s.T(), func(t *await.T) {
		require.False(t, s.hasBufferedEventType(t.Context(), 1, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED))
	}, 45*time.Second, replicationCheckInterval)
	// The timeout flush and conflict resolution can produce more than one
	// state-based task. Release only this workflow's tasks until all reapplied
	// inputs reach the active cluster.
	for attempt := 0; attempt < 10 && !s.hasReappliedBufferedInputs(ctx, bufferedInputs); attempt++ {
		s.releaseReplicationTask(ctx, replicationToNewActive)
	}
	s.Require().True(s.hasReappliedBufferedInputs(ctx, bufferedInputs))
	for attempt := 0; attempt < 10 && !s.bufferedEventsHistoriesEqual(ctx, ns, execution); attempt++ {
		s.releaseReplicationTask(ctx, replicationToOldActive)
	}

	await.Require(ctx, s.T(), func(t *await.T) {
		sourceHistory := s.getWorkflowHistory(t.Context(), t.AssertionT(), 0, ns, execution)
		targetHistory := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.Equal(t, targetHistory, sourceHistory)
	}, replicationWaitTime, replicationCheckInterval)

	finalHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	s.Require().Equal(1, countBufferedEventType(finalHistory, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED))
	s.Require().Equal(1, countBufferedEventType(finalHistory, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED))
	s.Require().Equal(1, countBufferedEventType(finalHistory, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED))
	s.Require().Equal(2, countBufferedEventType(finalHistory, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED))
	assertOnlyExpectedBufferedEventsReapplied(s.T(), finalHistory, naturallyBufferedTypes)
}

// TestNaturallyBufferedActivityOutcomesFlushedToLosingBranch buffers started, completed, failed,
// timed-out, and canceled activity outcomes. It expects all outcomes on the losing branch and none
// to be reapplied to the winner, apart from the winner independently producing its own timeout.
func (s *FunctionalClustersTestSuite) TestNaturallyBufferedActivityOutcomesFlushedToLosingBranch() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name

	workflowID := "buffered-activities-xdc-" + uuid.NewString()
	workflowQueue := &taskqueuepb.TaskQueue{Name: workflowID + "-workflow"}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: workflowID,
		TaskQueue:  workflowQueue,
	})
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)

	activityQueues := map[string]*taskqueuepb.TaskQueue{}
	var scheduleCommands []*commandpb.Command
	for _, activityID := range []string{"started", "completed", "failed", "canceled"} {
		activityQueues[activityID] = &taskqueuepb.TaskQueue{Name: workflowID + "-" + activityID}
		scheduleCommands = append(scheduleCommands, scheduleActivityCommand(activityID, activityQueues[activityID], time.Minute))
	}
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     firstTask,
		Commands: scheduleCommands,
	})

	// Start the activity that will be canceled before establishing the common
	// prefix, so its cancellation request can be issued by a workflow command.
	canceledTask := s.pollBufferedActivityTask(ctx, ns, activityQueues["canceled"])
	s.signalWorkflow(ctx, workflowSignal{
		Namespace:  ns,
		Execution:  execution,
		SignalName: "prepare-activity-cancellation",
	})
	cancelCommandTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)
	s.Require().NotNil(cancelCommandTask.History)
	canceledScheduledID := findActivityScheduledEventID(cancelCommandTask.History.Events, "canceled")
	s.Require().Positive(canceledScheduledID)
	s.completeWorkflowTaskAndScheduleNext(ctx, workflowTaskCompletion{
		Task: cancelCommandTask,
		Commands: []*commandpb.Command{
			requestCancelActivityCommand(canceledScheduledID),
			scheduleActivityCommand("timed-out", &taskqueuepb.TaskQueue{Name: workflowID + "-unpolled"}, 5*time.Second),
		},
	})

	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.Positive(t, findActivityScheduledEventID(history, "canceled"))
	}, replicationWaitTime, replicationCheckInterval)

	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)
	heldWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)

	startedTask := s.pollBufferedActivityTask(ctx, ns, activityQueues["started"])
	s.Require().NotEmpty(startedTask.TaskToken)
	completedTask := s.pollBufferedActivityTask(ctx, ns, activityQueues["completed"])
	s.completeActivityTask(ctx, completedTask)
	failedTask := s.pollBufferedActivityTask(ctx, ns, activityQueues["failed"])
	s.failActivityTask(ctx, failedTask)
	s.cancelActivityTask(ctx, canceledTask)

	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
	}
	s.assertBufferedEventTypes(ctx, bufferedEventExpectation{
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
		WinnerSignal:           "activity-winner-signal",
	})
	winningHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	for _, eventType := range []enumspb.EventType{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
	} {
		s.Require().Zero(countBufferedEventType(winningHistory, eventType), "%s must remain only on the losing branch", eventType)
	}
	s.Require().Equal(
		1,
		countBufferedEventType(winningHistory, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT),
		"the winner may produce its own timeout, but must not contain a duplicate reapplied timeout",
	)
}

// TestNaturallyBufferedChildWorkflowOutcomesFlushedToLosingBranch buffers child start failure,
// started, completed, failed, canceled, timed-out, and terminated callbacks for children created only
// on the losing branch. It expects every callback to be persisted there and skipped on the winner.
func (s *FunctionalClustersTestSuite) TestNaturallyBufferedChildWorkflowOutcomesFlushedToLosingBranch() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name

	workflowID := "buffered-children-xdc-" + uuid.NewString()
	parentQueue := &taskqueuepb.TaskQueue{Name: workflowID + "-parent"}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: workflowID,
		TaskQueue:  parentQueue,
	})
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, parentQueue)

	childIDs := map[string]string{}
	childQueues := map[string]*taskqueuepb.TaskQueue{}
	for _, outcome := range []string{"completed", "failed", "canceled", "timed-out", "terminated", "duplicate"} {
		childIDs[outcome] = workflowID + "-" + outcome
		childQueues[outcome] = &taskqueuepb.TaskQueue{Name: childIDs[outcome] + "-queue"}
	}
	duplicateExecution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: childIDs["duplicate"],
		TaskQueue:  childQueues["duplicate"],
	})
	s.Require().NotEmpty(duplicateExecution.RunId)

	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.NotEmpty(t, history)
	}, replicationWaitTime, replicationCheckInterval)

	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)
	var commands []*commandpb.Command
	for _, outcome := range []string{"completed", "failed", "canceled", "timed-out", "terminated", "duplicate"} {
		runTimeout := time.Minute
		if outcome == "timed-out" {
			runTimeout = 5 * time.Second
		}
		commands = append(commands, startChildWorkflowCommand(childIDs[outcome], childQueues[outcome], runTimeout))
	}
	heldWorkflowTask := s.completeWorkflowTaskAndReturnNext(ctx, workflowTaskCompletion{
		Task:     firstTask,
		Commands: commands,
	})
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)

	completedTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, childQueues["completed"])
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     completedTask,
		Commands: []*commandpb.Command{completeWorkflowCommand()},
	})

	failedTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, childQueues["failed"])
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     failedTask,
		Commands: []*commandpb.Command{failWorkflowCommand("expected child failure")},
	})

	s.requestWorkflowCancellationEventually(ctx, workflowCancellation{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: childIDs["canceled"]},
	})
	canceledTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, childQueues["canceled"])
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task:     canceledTask,
		Commands: []*commandpb.Command{cancelWorkflowCommand()},
	})

	s.terminateWorkflowEventually(ctx, workflowTermination{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: childIDs["terminated"]},
		Reason:    "expected child termination",
	})

	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
	}
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
		WinnerSignal:           "child-winner-signal",
	})
	winningHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	for _, eventType := range expectedTypes {
		s.Require().Zero(countBufferedEventType(winningHistory, eventType), "%s has no child initiation on the winning branch and must be skipped", eventType)
	}
}

// TestNaturallyBufferedExternalWorkflowOutcomesFlushedToLosingBranch buffers successful and failed
// signal-external and cancel-external results. It expects every result on the losing branch and none
// on the winner because the corresponding initiated commands do not exist there.
func (s *FunctionalClustersTestSuite) TestNaturallyBufferedExternalWorkflowOutcomesFlushedToLosingBranch() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	namespace := s.createBufferedEventsNamespace(ctx)
	ns := namespace.Name

	workflowID := "buffered-external-xdc-" + uuid.NewString()
	workflowQueue := &taskqueuepb.TaskQueue{Name: workflowID + "-source"}
	execution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: workflowID,
		TaskQueue:  workflowQueue,
	})
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)
	signalTargetID := workflowID + "-signal-target"
	signalTargetQueue := &taskqueuepb.TaskQueue{Name: signalTargetID + "-queue"}
	signalTargetExecution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: signalTargetID,
		TaskQueue:  signalTargetQueue,
	})
	s.Require().NotEmpty(signalTargetExecution.RunId)
	targetTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, signalTargetQueue)
	s.completeWorkflowTask(ctx, workflowTaskCompletion{
		Task: targetTask,
	})
	cancelTargetID := workflowID + "-completed-cancel-target"
	cancelTargetExecution := s.startBufferedEventsWorkflow(ctx, startBufferedEventsWorkflowArgs{
		Namespace:  ns,
		WorkflowID: cancelTargetID,
		TaskQueue:  &taskqueuepb.TaskQueue{Name: cancelTargetID + "-queue"},
	})
	s.terminateWorkflow(ctx, workflowTermination{
		Namespace: ns,
		Execution: cancelTargetExecution,
		Reason:    "completed target makes external cancellation deterministic",
	})

	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.NotEmpty(t, history)
	}, replicationWaitTime, replicationCheckInterval)
	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)

	missingWorkflowID := workflowID + "-missing"
	missingRunID := uuid.NewString()
	heldWorkflowTask := s.completeWorkflowTaskAndReturnNext(ctx, workflowTaskCompletion{
		Task: firstTask,
		Commands: []*commandpb.Command{
			signalExternalWorkflowCommand(signalTargetID, "", "successful-external-signal"),
			signalExternalWorkflowCommand(missingWorkflowID, missingRunID, "failed-external-signal"),
			cancelExternalWorkflowCommand(missingWorkflowID, missingRunID),
			cancelExternalWorkflowCommand(cancelTargetID, cancelTargetExecution.RunId),
		},
	})
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 0, ns, signalTargetExecution)
		require.True(t, hasSignalNamed(history, "successful-external-signal"))
	}, replicationWaitTime, replicationCheckInterval)

	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
	}
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
		WinnerSignal:           "external-winner-signal",
	})
	winningHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	for _, eventType := range expectedTypes {
		s.Require().Zero(countBufferedEventType(winningHistory, eventType), "%s must remain only on the losing branch", eventType)
	}
}
