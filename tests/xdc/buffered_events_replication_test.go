package xdc

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type blockedReplicationTask struct {
	execute func() error
	result  chan error
}

// Four values in HistoryBuilder's buffered-event set cannot be naturally
// buffered on current main. EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
// EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY, and
// EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY have no production emitter.
// EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED has an emitter, but
// its production precondition rejects every workflow with a pending workflow
// task, so it cannot enter the buffer. The tests below cover every other value
// through its production API, transfer task, timer, callback, or reapplier.

func (s *FunctionalClustersTestSuite) TestNaturallyBufferedInputsFlushedAndReappliedAfterFailover() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.Require().NoError(err)

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
	s.assertBufferedEventTypes(ctx, 0, ns, execution, naturallyBufferedTypes)

	err = s.syncWorkflowState(ctx, namespace.NamespaceInfo.Id, execution)
	var workflowNotReady *serviceerror.WorkflowNotReady
	s.Require().ErrorAs(err, &workflowNotReady)

	// Phase 3: fail over and create a winning branch on the new active cluster.
	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, namespace.NamespaceInfo.Id, ns, execution, "winner-signal")
	winnerWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 1, ns, taskQueue)
	s.Require().NotEmpty(winnerWorkflowTask.TaskToken)

	// Phase 4: resolve the conflict and verify losing-branch storage versus reapplication.
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	s.assertBufferedEventsPersistedOnLosingBranch(ctx, ns, namespace.NamespaceInfo.Id, execution, updateID, optionsRequestID, naturallyBufferedTypes)
	err = s.syncWorkflowState(ctx, namespace.NamespaceInfo.Id, execution)
	s.Require().NoError(err)

	// Reapplying the losing update while the winner's workflow task is running
	// naturally creates UpdateAdmitted as a buffered event.
	for attempt := 0; attempt < 10 && !s.hasBufferedEventType(ctx, 1, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED); attempt++ {
		s.releaseReplicationTask(ctx, replicationToNewActive)
	}
	s.Require().True(s.hasBufferedEventType(ctx, 1, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED))
	err = s.syncWorkflowStateFrom(ctx, 1, namespace.NamespaceInfo.Id, execution, 1)
	s.Require().ErrorAs(err, &workflowNotReady)
	await.Require(ctx, s.T(), func(t *await.T) {
		require.False(t, s.hasBufferedEventType(t.Context(), 1, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED))
	}, 45*time.Second, replicationCheckInterval)
	s.Require().NoError(s.syncWorkflowStateFrom(ctx, 1, namespace.NamespaceInfo.Id, execution, 1))

	// The timeout flush and conflict resolution can produce more than one
	// state-based task. Release only this workflow's tasks until all reapplied
	// inputs reach the active cluster.
	for attempt := 0; attempt < 10 && !s.hasReappliedBufferedInputs(ctx, 1, ns, execution, updateID, optionsRequestID); attempt++ {
		s.releaseReplicationTask(ctx, replicationToNewActive)
	}
	s.Require().True(s.hasReappliedBufferedInputs(ctx, 1, ns, execution, updateID, optionsRequestID))
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

func (s *FunctionalClustersTestSuite) TestNaturallyBufferedActivityOutcomesFlushedToLosingBranch() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)

	workflowID := "buffered-activities-xdc-" + uuid.NewString()
	workflowQueue := &taskqueuepb.TaskQueue{Name: workflowID + "-workflow"}
	execution := s.startBufferedEventsWorkflow(ctx, ns, workflowID, workflowQueue)
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)

	activityQueues := map[string]*taskqueuepb.TaskQueue{}
	var scheduleCommands []*commandpb.Command
	for _, activityID := range []string{"started", "completed", "failed", "canceled"} {
		activityQueues[activityID] = &taskqueuepb.TaskQueue{Name: workflowID + "-" + activityID}
		scheduleCommands = append(scheduleCommands, scheduleActivityCommand(activityID, activityQueues[activityID], time.Minute))
	}
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: firstTask.TaskToken,
		Identity:  "buffered-activities-xdc-test",
		Commands:  scheduleCommands,
	})
	s.Require().NoError(err)

	// Start the activity that will be canceled before establishing the common
	// prefix, so its cancellation request can be issued by a workflow command.
	canceledTask := s.pollBufferedActivityTask(ctx, ns, activityQueues["canceled"])
	_, err = s.clusters[0].FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: execution,
		SignalName:        "prepare-activity-cancellation",
		RequestId:         uuid.NewString(),
		Identity:          "buffered-activities-xdc-test",
	})
	s.Require().NoError(err)
	cancelCommandTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)
	s.Require().NotNil(cancelCommandTask.History)
	canceledScheduledID := findActivityScheduledEventID(cancelCommandTask.History.Events, "canceled")
	s.Require().Positive(canceledScheduledID)
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  cancelCommandTask.TaskToken,
		Identity:                   "buffered-activities-xdc-test",
		ForceCreateNewWorkflowTask: true,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{
					RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{ScheduledEventId: canceledScheduledID},
				},
			},
			scheduleActivityCommand("timed-out", &taskqueuepb.TaskQueue{Name: workflowID + "-unpolled"}, 5*time.Second),
		},
	})
	s.Require().NoError(err)

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
	_, err = s.clusters[0].FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: completedTask.TaskToken,
		Identity:  "buffered-activities-xdc-test",
	})
	s.Require().NoError(err)
	failedTask := s.pollBufferedActivityTask(ctx, ns, activityQueues["failed"])
	_, err = s.clusters[0].FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: failedTask.TaskToken,
		Identity:  "buffered-activities-xdc-test",
		Failure: &failurepb.Failure{
			Message: "expected activity failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
			},
		},
	})
	s.Require().NoError(err)
	_, err = s.clusters[0].FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
		TaskToken: canceledTask.TaskToken,
		Identity:  "buffered-activities-xdc-test",
	})
	s.Require().NoError(err)

	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
	}
	s.assertBufferedEventTypes(ctx, 0, ns, execution, expectedTypes)
	s.finishNaturallyBufferedConflict(ctx, ns, namespace.NamespaceInfo.Id, execution,
		replicationToOldActive, replicationToNewActive, expectedTypes, "activity-winner-signal")
}

func (s *FunctionalClustersTestSuite) TestNaturallyBufferedChildWorkflowOutcomesFlushedToLosingBranch() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)

	workflowID := "buffered-children-xdc-" + uuid.NewString()
	parentQueue := &taskqueuepb.TaskQueue{Name: workflowID + "-parent"}
	execution := s.startBufferedEventsWorkflow(ctx, ns, workflowID, parentQueue)
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, parentQueue)

	childIDs := map[string]string{}
	childQueues := map[string]*taskqueuepb.TaskQueue{}
	for _, outcome := range []string{"completed", "failed", "canceled", "timed-out", "terminated", "duplicate"} {
		childIDs[outcome] = workflowID + "-" + outcome
		childQueues[outcome] = &taskqueuepb.TaskQueue{Name: childIDs[outcome] + "-queue"}
	}
	duplicateExecution := s.startBufferedEventsWorkflow(ctx, ns, childIDs["duplicate"], childQueues["duplicate"])
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
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  firstTask.TaskToken,
		Identity:                   "buffered-children-xdc-test",
		ForceCreateNewWorkflowTask: true,
		Commands:                   commands,
	})
	s.Require().NoError(err)
	heldWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, parentQueue)
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)

	completedTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, childQueues["completed"])
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: completedTask.TaskToken,
		Identity:  "buffered-children-xdc-test",
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.Require().NoError(err)

	failedTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, childQueues["failed"])
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: failedTask.TaskToken,
		Identity:  "buffered-children-xdc-test",
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
				FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{Failure: &failurepb.Failure{
					Message: "expected child failure",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
					},
				}},
			},
		}},
	})
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		_, describeErr := s.clusters[0].FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: childIDs["canceled"]},
		})
		require.NoError(t, describeErr)
	}, replicationWaitTime, replicationCheckInterval)
	_, err = s.clusters[0].FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: childIDs["canceled"]},
		RequestId:         uuid.NewString(),
		Identity:          "buffered-children-xdc-test",
	})
	s.Require().NoError(err)
	canceledTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, childQueues["canceled"])
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: canceledTask.TaskToken,
		Identity:  "buffered-children-xdc-test",
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
				CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		_, terminateErr := s.clusters[0].FrontendClient().TerminateWorkflowExecution(t.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         ns,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: childIDs["terminated"]},
			Reason:            "expected child termination",
			Identity:          "buffered-children-xdc-test",
		})
		require.NoError(t, terminateErr)
	}, replicationWaitTime, replicationCheckInterval)

	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
	}
	s.assertBufferedEventTypesPresent(ctx, 0, ns, execution, expectedTypes)
	s.finishNaturallyBufferedConflict(ctx, ns, namespace.NamespaceInfo.Id, execution,
		replicationToOldActive, replicationToNewActive, expectedTypes, "child-winner-signal")
}

func (s *FunctionalClustersTestSuite) TestNaturallyBufferedExternalWorkflowOutcomesFlushedToLosingBranch() {
	if !s.enableTransitionHistory {
		s.T().Skip("buffered event state-based replication requires transition history")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)

	workflowID := "buffered-external-xdc-" + uuid.NewString()
	workflowQueue := &taskqueuepb.TaskQueue{Name: workflowID + "-source"}
	execution := s.startBufferedEventsWorkflow(ctx, ns, workflowID, workflowQueue)
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, workflowQueue)
	signalTargetID := workflowID + "-signal-target"
	signalTargetQueue := &taskqueuepb.TaskQueue{Name: signalTargetID + "-queue"}
	signalTargetExecution := s.startBufferedEventsWorkflow(ctx, ns, signalTargetID, signalTargetQueue)
	s.Require().NotEmpty(signalTargetExecution.RunId)
	targetTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, signalTargetQueue)
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: targetTask.TaskToken,
		Identity:  "buffered-external-xdc-test",
	})
	s.Require().NoError(err)
	cancelTargetID := workflowID + "-completed-cancel-target"
	cancelTargetExecution := s.startBufferedEventsWorkflow(ctx, ns, cancelTargetID, &taskqueuepb.TaskQueue{Name: cancelTargetID + "-queue"})
	_, err = s.clusters[0].FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: cancelTargetExecution,
		Reason:            "completed target makes external cancellation deterministic",
		Identity:          "buffered-external-xdc-test",
	})
	s.Require().NoError(err)

	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.NotEmpty(t, history)
	}, replicationWaitTime, replicationCheckInterval)

	missingWorkflowID := workflowID + "-missing"
	missingRunID := uuid.NewString()
	heldWorkflowTask := s.respondWorkflowTaskAndStartNext(ctx, 0, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  firstTask.TaskToken,
		Identity:                   "buffered-external-xdc-test",
		ForceCreateNewWorkflowTask: true,
		Commands: []*commandpb.Command{
			signalExternalWorkflowCommand(ns, signalTargetID, "", "successful-external-signal"),
			signalExternalWorkflowCommand(ns, missingWorkflowID, missingRunID, "failed-external-signal"),
			cancelExternalWorkflowCommand(ns, missingWorkflowID, missingRunID),
			cancelExternalWorkflowCommand(ns, cancelTargetID, cancelTargetExecution.RunId),
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
	s.assertBufferedEventTypesPresent(ctx, 0, ns, execution, expectedTypes)
	replicationToOldActive := s.blockReplicationForWorkflow(0, workflowID)
	replicationToNewActive := s.blockReplicationForWorkflow(1, workflowID)
	s.finishNaturallyBufferedConflict(ctx, ns, namespace.NamespaceInfo.Id, execution,
		replicationToOldActive, replicationToNewActive, expectedTypes, "external-winner-signal")
}

func (s *xdcBaseSuite) finishNaturallyBufferedConflict(
	ctx context.Context,
	ns string,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	replicationToOldActive <-chan *blockedReplicationTask,
	replicationToNewActive <-chan *blockedReplicationTask,
	expectedTypes []enumspb.EventType,
	winnerSignal string,
) []*historypb.HistoryEvent {
	err := s.syncWorkflowState(ctx, namespaceID, execution)
	var workflowNotReady *serviceerror.WorkflowNotReady
	s.Require().ErrorAs(err, &workflowNotReady)

	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, namespaceID, ns, execution, winnerSignal)
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	losingHistory := s.findNonCurrentHistoryBranch(ctx, ns, namespaceID, execution, func(history []*historypb.HistoryEvent) bool {
		for _, eventType := range expectedTypes {
			if findBufferedEventsHistoryEvent(history, eventType) == nil {
				return false
			}
		}
		return true
	})
	for _, eventType := range expectedTypes {
		event := findBufferedEventsHistoryEvent(losingHistory, eventType)
		s.Require().NotNil(event, "%s must be written to the losing branch", eventType)
		s.Require().Positive(event.EventId)
		s.Require().NotEqual(common.BufferedEventID, event.EventId)
	}
	s.Require().NoError(s.syncWorkflowState(ctx, namespaceID, execution))

	s.releaseReplicationTask(ctx, replicationToNewActive)
	for attempt := 0; attempt < 10 && !hasSignalNamed(s.getWorkflowHistory(ctx, s.T(), 1, ns, execution), winnerSignal); attempt++ {
		s.releaseReplicationTask(ctx, replicationToNewActive)
	}
	for attempt := 0; attempt < 10 && !s.bufferedEventsHistoriesEqual(ctx, ns, execution); attempt++ {
		s.releaseReplicationTask(ctx, replicationToOldActive)
	}
	return losingHistory
}

func signalExternalWorkflowCommand(namespace, workflowID, runID, signalName string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
			SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace:  namespace,
				Execution:  &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				SignalName: signalName,
			},
		},
	}
}

func cancelExternalWorkflowCommand(namespace, workflowID, runID string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{
			RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
				Namespace:  namespace,
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	}
}

func startChildWorkflowCommand(childID string, taskQueue *taskqueuepb.TaskQueue, runTimeout time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId:          childID,
				WorkflowType:        &commonpb.WorkflowType{Name: "buffered-child"},
				TaskQueue:           taskQueue,
				WorkflowRunTimeout:  durationpb.New(runTimeout),
				WorkflowTaskTimeout: durationpb.New(30 * time.Second),
			},
		},
	}
}

func (s *xdcBaseSuite) startBufferedEventsWorkflow(
	ctx context.Context,
	ns string,
	workflowID string,
	taskQueue *taskqueuepb.TaskQueue,
) *commonpb.WorkflowExecution {
	response, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "buffered-events-xdc"},
		TaskQueue:           taskQueue,
		RequestId:           uuid.NewString(),
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(2 * time.Minute),
		Identity:            "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	return &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: response.RunId}
}

func scheduleActivityCommand(activityID string, taskQueue *taskqueuepb.TaskQueue, startToClose time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             activityID,
				ActivityType:           &commonpb.ActivityType{Name: activityID},
				TaskQueue:              taskQueue,
				ScheduleToCloseTimeout: durationpb.New(startToClose),
				StartToCloseTimeout:    durationpb.New(startToClose),
			},
		},
	}
}

func (s *xdcBaseSuite) pollBufferedActivityTask(
	ctx context.Context,
	ns string,
	taskQueue *taskqueuepb.TaskQueue,
) *workflowservice.PollActivityTaskQueueResponse {
	response, err := s.clusters[0].FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: ns,
		TaskQueue: taskQueue,
		Identity:  "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	s.Require().NotEmpty(response.TaskToken)
	return response
}

func findActivityScheduledEventID(history []*historypb.HistoryEvent, activityID string) int64 {
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED &&
			event.GetActivityTaskScheduledEventAttributes().GetActivityId() == activityID {
			return event.EventId
		}
	}
	return common.EmptyEventID
}

func assertOnlyExpectedBufferedEventsReapplied(t require.TestingT, history []*historypb.HistoryEvent, bufferedTypes []enumspb.EventType) {
	expectedCounts := map[enumspb.EventType]int{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:         2,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:  1,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED: 1,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:  1,
	}
	for _, eventType := range bufferedTypes {
		require.Equal(t, expectedCounts[eventType], countBufferedEventType(history, eventType), eventType.String())
	}
}

func (s *FunctionalClustersTestSuite) startWorkflowWithPendingActivity(
	ctx context.Context,
	ns string,
) (*commonpb.WorkflowExecution, *taskqueuepb.TaskQueue) {
	s.T().Helper()
	workflowID := "buffered-events-xdc-" + uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: "buffered-events-xdc", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startResponse, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "buffered-events-xdc"},
		TaskQueue:           taskQueue,
		RequestId:           uuid.NewString(),
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(30 * time.Second),
		Identity:            "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: startResponse.RunId}

	workflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: workflowTask.TaskToken,
		Identity:  "buffered-events-xdc-test",
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "buffered-activity",
					ActivityType:           &commonpb.ActivityType{Name: "buffered-activity"},
					TaskQueue:              taskQueue,
					ScheduleToCloseTimeout: durationpb.New(time.Minute),
					ScheduleToStartTimeout: durationpb.New(time.Minute),
					StartToCloseTimeout:    durationpb.New(time.Minute),
				},
			},
		}},
	})
	s.Require().NoError(err)
	return execution, taskQueue
}

func (s *FunctionalClustersTestSuite) acceptUpdateAndStartTimer(
	ctx context.Context,
	ns string,
	execution *commonpb.WorkflowExecution,
	taskQueue *taskqueuepb.TaskQueue,
	updateID string,
) {
	s.T().Helper()
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.Require().NoError(err)
	defer sdkClient.Close()
	updateCtx, cancelUpdate := context.WithCancel(ctx)
	defer cancelUpdate()
	updateResult := make(chan error, 1)
	go func() {
		_, updateErr := sdkClient.UpdateWorkflow(updateCtx, sdkclient.UpdateWorkflowOptions{
			UpdateID:     updateID,
			WorkflowID:   execution.WorkflowId,
			RunID:        execution.RunId,
			UpdateName:   "buffered-update",
			Args:         []any{"source"},
			WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
		})
		updateResult <- updateErr
	}()
	await.Require(ctx, s.T(), func(t *await.T) {
		response, pollErr := sdkClient.WorkflowService().PollWorkflowExecutionUpdate(t.Context(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: ns,
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: execution,
				UpdateId:          updateID,
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
			},
		})
		require.NoError(t, pollErr)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, response.Stage)
	}, 10*time.Second, 20*time.Millisecond)
	workflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().Len(workflowTask.Messages, 1)
	updateRequestMessage := workflowTask.Messages[0]
	s.Require().Equal(updateID, updateRequestMessage.ProtocolInstanceId)
	acceptMessageID := "accept-" + uuid.NewString()
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  workflowTask.TaskToken,
		Identity:                   "buffered-events-xdc-test",
		ForceCreateNewWorkflowTask: true,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{
				ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{MessageId: acceptMessageID},
			},
		}, {
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{
				StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "losing-branch-timer",
					StartToFireTimeout: durationpb.New(100 * time.Millisecond),
				},
			},
		}},
		Messages: []*protocolpb.Message{{
			Id:                 acceptMessageID,
			ProtocolInstanceId: updateID,
			Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
				AcceptedRequestMessageId:         updateRequestMessage.Id,
				AcceptedRequestSequencingEventId: updateRequestMessage.GetEventId(),
			}),
		}},
	})
	s.Require().NoError(err)
	s.Require().True(hasUpdateAccepted(s.getWorkflowHistory(ctx, s.T(), 0, ns, execution), updateID))
	s.Require().NoError(<-updateResult)
}

func (s *FunctionalClustersTestSuite) completeActivityAndBufferExternalEvents(
	ctx context.Context,
	ns string,
	execution *commonpb.WorkflowExecution,
	taskQueue *taskqueuepb.TaskQueue,
) string {
	s.T().Helper()
	activityTask, err := s.clusters[0].FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: ns,
		TaskQueue: taskQueue,
		Identity:  "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	s.Require().NotEmpty(activityTask.TaskToken)
	_, err = s.clusters[0].FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: activityTask.TaskToken,
		Result:    payloads.EncodeString("activity-result"),
		Identity:  "buffered-events-xdc-test",
	})
	s.Require().NoError(err)

	_, err = s.clusters[0].FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: execution,
		SignalName:        "buffered-signal",
		Input:             payloads.EncodeString("source"),
		Identity:          "buffered-events-xdc-test",
		RequestId:         uuid.NewString(),
	})
	s.Require().NoError(err)
	_, err = s.clusters[0].FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: execution,
		Identity:          "buffered-events-xdc-test",
		RequestId:         uuid.NewString(),
	})
	s.Require().NoError(err)

	optionsRequestID := uuid.NewString()
	attachResponse, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                ns,
		WorkflowId:               execution.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: "buffered-events-xdc"},
		TaskQueue:                taskQueue,
		RequestId:                optionsRequestID,
		WorkflowRunTimeout:       durationpb.New(time.Minute),
		WorkflowTaskTimeout:      durationpb.New(30 * time.Second),
		Identity:                 "buffered-events-xdc-test",
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId: true,
		},
	})
	s.Require().NoError(err)
	s.Require().False(attachResponse.Started)

	_, err = s.clusters[0].FrontendClient().PauseWorkflowExecution(ctx, &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  ns,
		WorkflowId: execution.WorkflowId,
		RunId:      execution.RunId,
		Identity:   "buffered-events-xdc-test",
		Reason:     "exercise buffered pause",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)
	_, err = s.clusters[0].FrontendClient().UnpauseWorkflowExecution(ctx, &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  ns,
		WorkflowId: execution.WorkflowId,
		RunId:      execution.RunId,
		Identity:   "buffered-events-xdc-test",
		Reason:     "exercise buffered unpause",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)
	return optionsRequestID
}

func (s *xdcBaseSuite) failoverToNewActiveCluster(ctx context.Context, ns string) {
	s.T().Helper()
	response, err := s.clusters[0].FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.clusters[1].ClusterName(),
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(int64(2), response.FailoverVersion)
	await.Require(ctx, s.T(), func(t *await.T) {
		for _, cluster := range s.clusters {
			describeResponse, describeErr := cluster.FrontendClient().DescribeNamespace(t.Context(), &workflowservice.DescribeNamespaceRequest{
				Namespace: ns,
			})
			require.NoError(t, describeErr)
			require.Equal(t, s.clusters[1].ClusterName(), describeResponse.ReplicationConfig.ActiveClusterName)
		}
	}, replicationWaitTime, replicationCheckInterval)
	s.waitForNamespaceCacheRefresh()
}

func (s *xdcBaseSuite) writeSignalOnNewActive(
	ctx context.Context,
	namespaceID string,
	ns string,
	execution *commonpb.WorkflowExecution,
	signalName string,
) {
	s.T().Helper()
	request := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: namespaceID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         ns,
			WorkflowExecution: execution,
			SignalName:        signalName,
			Identity:          "buffered-events-xdc-test",
			RequestId:         uuid.NewString(),
		},
	}
	await.Require(ctx, s.T(), func(t *await.T) {
		_, err := s.clusters[1].HistoryClient().SignalWorkflowExecution(t.Context(), request)
		require.NoError(t, err)
	}, replicationWaitTime, replicationCheckInterval)
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution)
		require.True(t, hasSignalNamed(history, signalName))
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *xdcBaseSuite) syncWorkflowState(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
) error {
	s.T().Helper()
	return s.syncWorkflowStateFrom(ctx, 0, namespaceID, execution, 2)
}

func (s *xdcBaseSuite) syncWorkflowStateFrom(
	ctx context.Context,
	sourceCluster int,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	targetClusterID int32,
) error {
	s.T().Helper()
	_, err := s.clusters[sourceCluster].AdminClient().SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
		NamespaceId:     namespaceID,
		Execution:       execution,
		TargetClusterId: targetClusterID,
		ArchetypeId:     chasm.WorkflowArchetypeID,
	})
	return err
}

func (s *xdcBaseSuite) hasBufferedEventType(
	ctx context.Context,
	clusterIndex int,
	ns string,
	execution *commonpb.WorkflowExecution,
	eventType enumspb.EventType,
) bool {
	response, err := s.clusters[clusterIndex].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: execution,
		Archetype: chasm.WorkflowArchetype,
	})
	if err != nil {
		return false
	}
	return findBufferedEventsHistoryEvent(response.GetCacheMutableState().GetBufferedEvents(), eventType) != nil
}

func (s *xdcBaseSuite) pollBufferedEventsWorkflowTask(
	ctx context.Context,
	clusterIndex int,
	ns string,
	taskQueue *taskqueuepb.TaskQueue,
) *workflowservice.PollWorkflowTaskQueueResponse {
	s.T().Helper()
	response, err := s.clusters[clusterIndex].FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns,
		TaskQueue: taskQueue,
		Identity:  "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	return response
}

func (s *xdcBaseSuite) respondWorkflowTaskAndStartNext(
	ctx context.Context,
	clusterIndex int,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) *workflowservice.PollWorkflowTaskQueueResponse {
	s.T().Helper()
	request.ForceCreateNewWorkflowTask = true
	request.ReturnNewWorkflowTask = true
	response, err := s.clusters[clusterIndex].FrontendClient().RespondWorkflowTaskCompleted(ctx, request)
	s.Require().NoError(err)
	s.Require().NotNil(response.GetWorkflowTask())
	return response.GetWorkflowTask()
}

func (s *xdcBaseSuite) blockReplicationForWorkflow(
	clusterIndex int,
	workflowID string,
) <-chan *blockedReplicationTask {
	s.T().Helper()
	tasks := make(chan *blockedReplicationTask, 20)
	s.clusters[clusterIndex].InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, func(
			task *replicationspb.ReplicationTask,
			execute func() error,
		) error {
			if workflowIDFromReplicationTask(task) != workflowID {
				return execute()
			}
			blockedTask := &blockedReplicationTask{
				execute: execute,
				result:  make(chan error, 1),
			}
			tasks <- blockedTask
			return <-blockedTask.result
		}),
		testhooks.GlobalScope,
	)
	return tasks
}

func (s *xdcBaseSuite) releaseReplicationTask(
	ctx context.Context,
	tasks <-chan *blockedReplicationTask,
) {
	s.T().Helper()
	select {
	case task := <-tasks:
		err := task.execute()
		task.result <- err
		var duplicateError *serviceerror.AlreadyExists
		var retryReplicationError *serviceerrors.RetryReplication
		s.Require().True(
			err == nil || errors.As(err, &duplicateError) || errors.As(err, &retryReplicationError),
			"replication task failed: %v",
			err,
		)
	case <-ctx.Done():
		s.FailNow("timed out waiting for controlled history replication task", ctx.Err().Error())
	}
}

func workflowIDFromReplicationTask(task *replicationspb.ReplicationTask) string {
	if attributes := task.GetSyncVersionedTransitionTaskAttributes(); attributes != nil {
		return attributes.WorkflowId
	}
	if attributes := task.GetSyncWorkflowStateTaskAttributes(); attributes != nil {
		return attributes.GetWorkflowState().GetExecutionInfo().GetWorkflowId()
	}
	if attributes := task.GetSyncHsmAttributes(); attributes != nil {
		return attributes.WorkflowId
	}
	if attributes := task.GetSyncActivityTaskAttributes(); attributes != nil {
		return attributes.WorkflowId
	}
	if attributes := task.GetVerifyVersionedTransitionTaskAttributes(); attributes != nil {
		return attributes.WorkflowId
	}
	if attributes := task.GetBackfillHistoryTaskAttributes(); attributes != nil {
		return attributes.WorkflowId
	}
	if attributes := task.GetHistoryTaskAttributes(); attributes != nil {
		return attributes.WorkflowId
	}
	return ""
}

func (s *xdcBaseSuite) assertBufferedEventTypes(
	ctx context.Context,
	clusterIndex int,
	ns string,
	execution *commonpb.WorkflowExecution,
	expectedTypes []enumspb.EventType,
) {
	s.T().Helper()
	await.Require(ctx, s.T(), func(t *await.T) {
		response, err := s.clusters[clusterIndex].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: execution,
			Archetype: chasm.WorkflowArchetype,
		})
		require.NoError(t, err)
		require.NotNil(t, response.CacheMutableState)
		bufferedEvents := response.CacheMutableState.BufferedEvents
		actualTypes := make([]enumspb.EventType, 0, len(bufferedEvents))
		for _, event := range bufferedEvents {
			require.Equal(t, common.BufferedEventID, event.EventId)
			actualTypes = append(actualTypes, event.EventType)
		}
		require.ElementsMatch(t, expectedTypes, actualTypes)
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *xdcBaseSuite) assertBufferedEventTypesPresent(
	ctx context.Context,
	clusterIndex int,
	ns string,
	execution *commonpb.WorkflowExecution,
	expectedTypes []enumspb.EventType,
) {
	s.T().Helper()
	await.Require(ctx, s.T(), func(t *await.T) {
		response, err := s.clusters[clusterIndex].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: execution,
			Archetype: chasm.WorkflowArchetype,
		})
		require.NoError(t, err)
		bufferedEvents := response.GetCacheMutableState().GetBufferedEvents()
		for _, event := range bufferedEvents {
			require.Equal(t, common.BufferedEventID, event.EventId)
		}
		for _, expectedType := range expectedTypes {
			require.NotNil(t, findBufferedEventsHistoryEvent(bufferedEvents, expectedType), "%s must be naturally buffered", expectedType)
		}
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *xdcBaseSuite) assertNoBufferedEvents(
	ctx context.Context,
	clusterIndex int,
	ns string,
	execution *commonpb.WorkflowExecution,
) {
	s.T().Helper()
	response, err := s.clusters[clusterIndex].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: execution,
		Archetype: chasm.WorkflowArchetype,
	})
	s.Require().NoError(err)
	s.Require().Empty(response.GetCacheMutableState().GetBufferedEvents())
	s.Require().Empty(response.GetDatabaseMutableState().GetBufferedEvents())
}

func (s *xdcBaseSuite) getWorkflowHistory(
	ctx context.Context,
	t require.TestingT,
	clusterIndex int,
	ns string,
	execution *commonpb.WorkflowExecution,
) []*historypb.HistoryEvent {
	response, err := s.clusters[clusterIndex].FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: ns,
		Execution: execution,
	})
	require.NoError(t, err)
	return response.History.Events
}

func (s *FunctionalClustersTestSuite) assertBufferedEventsPersistedOnLosingBranch(
	ctx context.Context,
	ns string,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	updateID string,
	optionsRequestID string,
	expectedEventTypes []enumspb.EventType,
) {
	s.T().Helper()
	losingHistory := s.findNonCurrentHistoryBranch(ctx, ns, namespaceID, execution, func(history []*historypb.HistoryEvent) bool {
		return hasSignalNamed(history, "buffered-signal")
	})
	s.Require().True(hasUpdateAccepted(losingHistory, updateID))
	s.Require().True(hasWorkflowTaskFailedForFailover(losingHistory))
	s.Require().True(hasOptionsUpdatedRequest(losingHistory, optionsRequestID))

	for _, eventType := range expectedEventTypes {
		event := findBufferedEventsHistoryEvent(losingHistory, eventType)
		s.Require().NotNil(event, "%s must be written to the losing branch", eventType)
		s.Require().NotEqual(common.BufferedEventID, event.EventId)
		s.Require().Positive(event.EventId)
	}
}

func (s *xdcBaseSuite) findNonCurrentHistoryBranch(
	ctx context.Context,
	ns string,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	matches func([]*historypb.HistoryEvent) bool,
) []*historypb.HistoryEvent {
	s.T().Helper()
	description, err := s.clusters[0].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: execution,
		Archetype: chasm.WorkflowArchetype,
	})
	s.Require().NoError(err)
	versionHistories := description.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
	s.Require().GreaterOrEqual(len(versionHistories.GetHistories()), 2)

	shardID := common.WorkflowIDToHistoryShard(namespaceID, execution.WorkflowId, s.numHistoryShards)
	for index, versionHistory := range versionHistories.GetHistories() {
		if int32(index) == versionHistories.GetCurrentVersionHistoryIndex() {
			continue
		}
		history := s.readBufferedEventsHistoryBranch(ctx, shardID, versionHistory.GetBranchToken())
		if matches(history) {
			return history
		}
	}
	s.FailNow("matching non-current history branch not found")
	return nil
}

func (s *xdcBaseSuite) readBufferedEventsHistoryBranch(
	ctx context.Context,
	shardID int32,
	branchToken []byte,
) []*historypb.HistoryEvent {
	s.T().Helper()
	request := &persistence.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.EndEventID,
		PageSize:    1000,
	}
	var events []*historypb.HistoryEvent
	for {
		response, err := s.clusters[0].ExecutionManager().ReadHistoryBranch(ctx, request)
		s.Require().NoError(err)
		events = append(events, response.HistoryEvents...)
		if len(response.NextPageToken) == 0 {
			return events
		}
		request.NextPageToken = response.NextPageToken
	}
}

func (s *xdcBaseSuite) bufferedEventsHistoriesEqual(
	ctx context.Context,
	ns string,
	execution *commonpb.WorkflowExecution,
) bool {
	sourceHistory := s.getWorkflowHistory(ctx, s.T(), 0, ns, execution)
	targetHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	if len(targetHistory) != len(sourceHistory) {
		return false
	}
	for index := range targetHistory {
		if !proto.Equal(targetHistory[index], sourceHistory[index]) {
			return false
		}
	}
	return true
}

func (s *FunctionalClustersTestSuite) hasReappliedBufferedInputs(
	ctx context.Context,
	clusterIndex int,
	ns string,
	execution *commonpb.WorkflowExecution,
	updateID string,
	optionsRequestID string,
) bool {
	history := s.getWorkflowHistory(ctx, s.T(), clusterIndex, ns, execution)
	hasSourceSignal := false
	hasUpdate := false
	hasCancel := false
	hasOptionsUpdate := false
	for _, event := range history {
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			hasSourceSignal = hasSourceSignal || event.GetWorkflowExecutionSignaledEventAttributes().SignalName == "buffered-signal"
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
			hasUpdate = hasUpdate || event.GetWorkflowExecutionUpdateAdmittedEventAttributes().GetRequest().GetMeta().GetUpdateId() == updateID
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			hasCancel = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
			hasOptionsUpdate = hasOptionsUpdate || event.GetWorkflowExecutionOptionsUpdatedEventAttributes().AttachedRequestId == optionsRequestID
		default:
		}
	}
	return hasSourceSignal && hasUpdate && hasCancel && hasOptionsUpdate
}

func countBufferedEventType(history []*historypb.HistoryEvent, eventType enumspb.EventType) int {
	count := 0
	for _, event := range history {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}

func hasSignalNamed(history []*historypb.HistoryEvent, signalName string) bool {
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED &&
			event.GetWorkflowExecutionSignaledEventAttributes().SignalName == signalName {
			return true
		}
	}
	return false
}

func hasUpdateAccepted(history []*historypb.HistoryEvent, updateID string) bool {
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED &&
			event.GetWorkflowExecutionUpdateAcceptedEventAttributes().ProtocolInstanceId == updateID {
			return true
		}
	}
	return false
}

func hasWorkflowTaskFailedForFailover(history []*historypb.HistoryEvent) bool {
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED &&
			event.GetWorkflowTaskFailedEventAttributes().Cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND {
			return true
		}
	}
	return false
}

func hasOptionsUpdatedRequest(history []*historypb.HistoryEvent, requestID string) bool {
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED &&
			event.GetWorkflowExecutionOptionsUpdatedEventAttributes().AttachedRequestId == requestID {
			return true
		}
	}
	return false
}

func findBufferedEventsHistoryEvent(
	history []*historypb.HistoryEvent,
	eventType enumspb.EventType,
) *historypb.HistoryEvent {
	for _, event := range history {
		if event.EventType == eventType {
			return event
		}
	}
	return nil
}
