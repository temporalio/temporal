package xdc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
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
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type blockedReplicationTask struct {
	execute func() error
	once    sync.Once
	done    chan struct{}
	err     error
}

func (t *blockedReplicationTask) run() {
	t.once.Do(func() {
		t.err = t.execute()
		close(t.done)
	})
}

type bufferedEventsNamespace struct {
	Name string
	ID   string
}

type startBufferedEventsWorkflowArgs struct {
	Namespace  string
	WorkflowID string
	TaskQueue  *taskqueuepb.TaskQueue
}

type workflowTaskCompletion struct {
	Task     *workflowservice.PollWorkflowTaskQueueResponse
	Commands []*commandpb.Command
}

type bufferedEventExpectation struct {
	Namespace  string
	Execution  *commonpb.WorkflowExecution
	EventTypes []enumspb.EventType
}

type workflowSignal struct {
	Namespace  string
	Execution  *commonpb.WorkflowExecution
	SignalName string
}

type activeClusterSignal struct {
	Namespace  bufferedEventsNamespace
	Execution  *commonpb.WorkflowExecution
	SignalName string
}

type workflowCancellation struct {
	Namespace string
	Execution *commonpb.WorkflowExecution
}

type workflowTermination struct {
	Namespace string
	Execution *commonpb.WorkflowExecution
	Reason    string
}

type naturallyBufferedConflict struct {
	Namespace              bufferedEventsNamespace
	Execution              *commonpb.WorkflowExecution
	ReplicationToOldActive <-chan *blockedReplicationTask
	ReplicationToNewActive <-chan *blockedReplicationTask
	ExpectedEventTypes     []enumspb.EventType
	WinnerSignal           string
}

type bufferedInputsExpectation struct {
	Namespace        bufferedEventsNamespace
	Execution        *commonpb.WorkflowExecution
	UpdateID         string
	OptionsRequestID string
	EventTypes       []enumspb.EventType
}

type bufferedNexusCallback struct {
	operation string
	url       string
	token     string
}

type bufferedNexusOperationCompletion struct {
	Callback bufferedNexusCallback
	Result   string
}

type bufferedNexusLosingBranch struct {
	Namespace        bufferedEventsNamespace
	Execution        *commonpb.WorkflowExecution
	ScheduledEventID int64
}

func (s *xdcBaseSuite) createBufferedEventsNamespace(ctx context.Context) bufferedEventsNamespace {
	name := s.createGlobalNamespace()
	response, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: name,
	})
	s.Require().NoError(err)
	return bufferedEventsNamespace{Name: name, ID: response.NamespaceInfo.Id}
}

func (s *xdcBaseSuite) enableWorkflowPauseForTest() {
	s.T().Helper()
	for _, cluster := range s.clusters {
		cluster.OverrideDynamicConfig(s.T(), dynamicconfig.WorkflowPauseEnabled, true)
	}
}

func (s *xdcBaseSuite) signalWorkflow(ctx context.Context, signal workflowSignal) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         signal.Namespace,
		WorkflowExecution: signal.Execution,
		SignalName:        signal.SignalName,
		RequestId:         uuid.NewString(),
		Identity:          "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) requestWorkflowCancellationEventually(ctx context.Context, target workflowCancellation) {
	s.T().Helper()
	await.Require(ctx, s.T(), func(t *await.T) {
		_, err := s.clusters[0].FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: target.Namespace,
			Execution: target.Execution,
		})
		require.NoError(t, err)
	}, replicationWaitTime, replicationCheckInterval)
	_, err := s.clusters[0].FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace:         target.Namespace,
		WorkflowExecution: target.Execution,
		RequestId:         uuid.NewString(),
		Identity:          "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) terminateWorkflow(ctx context.Context, target workflowTermination) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         target.Namespace,
		WorkflowExecution: target.Execution,
		Reason:            target.Reason,
		Identity:          "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) terminateWorkflowEventually(ctx context.Context, target workflowTermination) {
	s.T().Helper()
	await.Require(ctx, s.T(), func(t *await.T) {
		_, err := s.clusters[0].FrontendClient().TerminateWorkflowExecution(t.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         target.Namespace,
			WorkflowExecution: target.Execution,
			Reason:            target.Reason,
			Identity:          "buffered-events-xdc-test",
		})
		require.NoError(t, err)
	}, replicationWaitTime, replicationCheckInterval)
}

// The buffered-event conflict tests intentionally use a fixed two-cluster topology:
//
//  1. Cluster 0 starts active, establishes the common history, and holds the workflow task while
//     naturally produced events enter its buffer.
//  2. Replication is blocked in both directions for this workflow. A namespace failover makes
//     cluster 1 active, where a signal creates the winning branch.
//  3. Cluster 1 -> cluster 0 replication is released first. Cluster 0 is now passive; applying the
//     winner resolves the conflict, failover-closes its outstanding workflow task, and flushes the
//     buffer onto the losing branch.
//  4. Cluster 0 -> cluster 1 replication is then released so conflict resolution sees the losing
//     branch and reapplies or skips its events against the winner.
//  5. Cluster 1 -> cluster 0 replication is finally released until both current histories converge.
//
// Replication channel names below identify the receiving cluster: replicationToOldActive contains
// tasks executing on cluster 0, and replicationToNewActive contains tasks executing on cluster 1.
func (s *xdcBaseSuite) finishNaturallyBufferedConflict(
	ctx context.Context,
	conflict naturallyBufferedConflict,
) []*historypb.HistoryEvent {
	losingBranchMarker := "losing-branch-marker-" + uuid.NewString()
	_, err := s.clusters[0].FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         conflict.Namespace.Name,
		WorkflowExecution: conflict.Execution,
		SignalName:        losingBranchMarker,
		RequestId:         uuid.NewString(),
		Identity:          "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	s.Require().True(s.hasBufferedEventType(ctx, 0, conflict.Namespace.Name, conflict.Execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED))

	s.failoverToNewActiveCluster(ctx, conflict.Namespace.Name)
	s.writeSignalOnNewActive(ctx, activeClusterSignal{
		Namespace:  conflict.Namespace,
		Execution:  conflict.Execution,
		SignalName: conflict.WinnerSignal,
	})
	s.releaseReplicationTask(ctx, conflict.ReplicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, conflict.Namespace.Name, conflict.Execution)
	losingHistory := s.findNonCurrentHistoryBranch(ctx, conflict.Namespace.Name, conflict.Namespace.ID, conflict.Execution, func(history []*historypb.HistoryEvent) bool {
		for _, eventType := range conflict.ExpectedEventTypes {
			if findHistoryEvent(history, eventType, nil) == nil {
				return false
			}
		}
		return true
	})
	for _, eventType := range conflict.ExpectedEventTypes {
		event := findHistoryEvent(losingHistory, eventType, nil)
		s.Require().NotNil(event, "%s must be written to the losing branch", eventType)
		s.Require().Positive(event.EventId)
		s.Require().NotEqual(common.BufferedEventID, event.EventId)
	}
	await.Require(ctx, s.T(), func(t *await.T) {
		s.tryReleaseReplicationTask(conflict.ReplicationToNewActive)
		require.True(
			t,
			hasSignalNamed(s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, conflict.Namespace.Name, conflict.Execution), losingBranchMarker),
			"the losing branch marker must be reapplied before checking the rest of the batch",
		)
	}, replicationWaitTime, replicationCheckInterval)
	for attempt := 0; attempt < 10 && !s.bufferedEventsHistoriesEqual(ctx, conflict.Namespace.Name, conflict.Execution); attempt++ {
		s.releaseReplicationTask(ctx, conflict.ReplicationToOldActive)
	}
	return losingHistory
}

func signalExternalWorkflowCommand(workflowID, runID, signalName string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
			SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Execution:  &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				SignalName: signalName,
			},
		},
	}
}

func cancelExternalWorkflowCommand(workflowID, runID string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{
			RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
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
	args startBufferedEventsWorkflowArgs,
) *commonpb.WorkflowExecution {
	response, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           args.Namespace,
		WorkflowId:          args.WorkflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "buffered-events-xdc"},
		TaskQueue:           args.TaskQueue,
		RequestId:           uuid.NewString(),
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(2 * time.Minute),
		Identity:            "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	return &commonpb.WorkflowExecution{WorkflowId: args.WorkflowID, RunId: response.RunId}
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

func requestCancelActivityCommand(scheduledEventID int64) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{
			RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
}

func completeWorkflowCommand() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	}
}

func failWorkflowCommand(message string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
			FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
				Failure: &failurepb.Failure{
					Message: message,
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
					},
				},
			},
		},
	}
}

func cancelWorkflowCommand() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
			CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{},
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

func (s *xdcBaseSuite) completeActivityTask(ctx context.Context, task *workflowservice.PollActivityTaskQueueResponse) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: task.TaskToken,
		Identity:  "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) failActivityTask(ctx context.Context, task *workflowservice.PollActivityTaskQueueResponse) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: task.TaskToken,
		Identity:  "buffered-events-xdc-test",
		Failure: &failurepb.Failure{
			Message: "expected activity failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
			},
		},
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) cancelActivityTask(ctx context.Context, task *workflowservice.PollActivityTaskQueueResponse) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
		TaskToken: task.TaskToken,
		Identity:  "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
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
	signal activeClusterSignal,
) {
	s.T().Helper()
	request := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: signal.Namespace.ID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         signal.Namespace.Name,
			WorkflowExecution: signal.Execution,
			SignalName:        signal.SignalName,
			Identity:          "buffered-events-xdc-test",
			RequestId:         uuid.NewString(),
		},
	}
	await.Require(ctx, s.T(), func(t *await.T) {
		_, err := s.clusters[1].HistoryClient().SignalWorkflowExecution(t.Context(), request)
		require.NoError(t, err)
	}, replicationWaitTime, replicationCheckInterval)
	await.Require(ctx, s.T(), func(t *await.T) {
		history := s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, signal.Namespace.Name, signal.Execution)
		require.True(t, hasSignalNamed(history, signal.SignalName))
	}, replicationWaitTime, replicationCheckInterval)
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
	return findHistoryEvent(response.GetCacheMutableState().GetBufferedEvents(), eventType, nil) != nil
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

func (s *xdcBaseSuite) completeWorkflowTask(ctx context.Context, completion workflowTaskCompletion) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: completion.Task.TaskToken,
		Identity:  "buffered-events-xdc-test",
		Commands:  completion.Commands,
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) completeWorkflowTaskAndScheduleNext(ctx context.Context, completion workflowTaskCompletion) {
	s.T().Helper()
	_, err := s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  completion.Task.TaskToken,
		Identity:                   "buffered-events-xdc-test",
		Commands:                   completion.Commands,
		ForceCreateNewWorkflowTask: true,
	})
	s.Require().NoError(err)
}

func (s *xdcBaseSuite) completeWorkflowTaskAndReturnNext(
	ctx context.Context,
	completion workflowTaskCompletion,
) *workflowservice.PollWorkflowTaskQueueResponse {
	s.T().Helper()
	response, err := s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  completion.Task.TaskToken,
		Identity:                   "buffered-events-xdc-test",
		Commands:                   completion.Commands,
		ForceCreateNewWorkflowTask: true,
		ReturnNewWorkflowTask:      true,
	})
	s.Require().NoError(err)
	s.Require().NotNil(response.GetWorkflowTask())
	return response.GetWorkflowTask()
}

func (s *xdcBaseSuite) blockReplicationForWorkflow(
	clusterIndex int,
	workflowID string,
) <-chan *blockedReplicationTask {
	s.T().Helper()
	// The interceptor runs on the receiving cluster, so clusterIndex identifies the replication
	// destination rather than the cluster that generated the task.
	tasks := make(chan *blockedReplicationTask, 20)
	stopped := make(chan struct{})
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
				done:    make(chan struct{}),
			}
			select {
			case tasks <- blockedTask:
			case <-stopped:
				return execute()
			}
			select {
			case <-blockedTask.done:
			case <-stopped:
				blockedTask.run()
			}
			return blockedTask.err
		}),
		testhooks.GlobalScope,
	)
	s.T().Cleanup(func() {
		close(stopped)
	})
	return tasks
}

func (s *xdcBaseSuite) releaseReplicationTask(
	ctx context.Context,
	tasks <-chan *blockedReplicationTask,
) {
	s.T().Helper()
	select {
	case task := <-tasks:
		s.executeReplicationTask(task)
	case <-ctx.Done():
		s.FailNow("timed out waiting for controlled history replication task", ctx.Err().Error())
	}
}

func (s *xdcBaseSuite) tryReleaseReplicationTask(tasks <-chan *blockedReplicationTask) bool {
	select {
	case task := <-tasks:
		s.executeReplicationTask(task)
		return true
	default:
		return false
	}
}

func (s *xdcBaseSuite) executeReplicationTask(task *blockedReplicationTask) {
	task.run()
	var duplicateError *serviceerror.AlreadyExists
	var retryReplicationError *serviceerrors.RetryReplication
	s.Require().True(
		task.err == nil || errors.As(task.err, &duplicateError) || errors.As(task.err, &retryReplicationError),
		"replication task failed: %v",
		task.err,
	)
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
	expectation bufferedEventExpectation,
) {
	s.T().Helper()
	await.Require(ctx, s.T(), func(t *await.T) {
		response, err := s.clusters[0].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: expectation.Namespace,
			Execution: expectation.Execution,
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
		require.ElementsMatch(t, expectation.EventTypes, actualTypes)
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *xdcBaseSuite) assertBufferedEventTypesPresent(
	ctx context.Context,
	expectation bufferedEventExpectation,
) {
	s.T().Helper()
	await.Require(ctx, s.T(), func(t *await.T) {
		response, err := s.clusters[0].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: expectation.Namespace,
			Execution: expectation.Execution,
			Archetype: chasm.WorkflowArchetype,
		})
		require.NoError(t, err)
		bufferedEvents := response.GetCacheMutableState().GetBufferedEvents()
		for _, event := range bufferedEvents {
			require.Equal(t, common.BufferedEventID, event.EventId)
		}
		for _, expectedType := range expectation.EventTypes {
			require.NotNil(t, findHistoryEvent(bufferedEvents, expectedType, nil), "%s must be naturally buffered", expectedType)
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
	expectation bufferedInputsExpectation,
) {
	s.T().Helper()
	losingHistory := s.findNonCurrentHistoryBranch(ctx, expectation.Namespace.Name, expectation.Namespace.ID, expectation.Execution, func(history []*historypb.HistoryEvent) bool {
		return hasSignalNamed(history, "buffered-signal")
	})
	s.Require().True(hasUpdateAccepted(losingHistory, expectation.UpdateID))
	s.Require().True(hasWorkflowTaskFailedForFailover(losingHistory))
	s.Require().True(hasOptionsUpdatedRequest(losingHistory, expectation.OptionsRequestID))

	for _, eventType := range expectation.EventTypes {
		event := findHistoryEvent(losingHistory, eventType, nil)
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
	expectation bufferedInputsExpectation,
) bool {
	history := s.getWorkflowHistory(ctx, s.T(), 1, expectation.Namespace.Name, expectation.Execution)
	hasSourceSignal := false
	hasUpdate := false
	hasCancel := false
	hasOptionsUpdate := false
	for _, event := range history {
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			hasSourceSignal = hasSourceSignal || event.GetWorkflowExecutionSignaledEventAttributes().SignalName == "buffered-signal"
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
			hasUpdate = hasUpdate || event.GetWorkflowExecutionUpdateAdmittedEventAttributes().GetRequest().GetMeta().GetUpdateId() == expectation.UpdateID
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			hasCancel = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
			hasOptionsUpdate = hasOptionsUpdate || event.GetWorkflowExecutionOptionsUpdatedEventAttributes().AttachedRequestId == expectation.OptionsRequestID
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

func (s *NexusStateReplicationSuite) createBufferedNexusEndpoint(ctx context.Context, handler nexustest.Handler) string {
	s.T().Helper()
	listenAddress := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddress, handler)
	for _, cluster := range s.clusters {
		cluster.OverrideDynamicConfig(
			s.T(),
			nexusoperations.CallbackURLTemplate,
			"http://"+s.clusters[0].Host().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback",
		)
	}
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	for _, cluster := range s.clusters {
		_, err := cluster.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{Url: "http://" + listenAddress},
				}},
			},
		})
		s.Require().NoError(err)
	}
	return endpointName
}

func (s *NexusStateReplicationSuite) failNexusOperation(ctx context.Context, callback bufferedNexusCallback) {
	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{Serializer: commonnexus.PayloadSerializer})
	err := client.CompleteOperation(ctx, callback.url, nexusrpc.CompleteOperationOptions{
		Error: &nexus.OperationError{
			State: nexus.OperationStateFailed,
			Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "expected operation failure"}},
		},
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callback.token},
	})
	s.Require().NoError(err)
}

func (s *NexusStateReplicationSuite) completeBufferedNexusOperation(ctx context.Context, completion bufferedNexusOperationCompletion) {
	s.T().Helper()
	s.completeNexusOperation(ctx, completion.Result, completion.Callback.url, completion.Callback.token)
}

func (s *NexusStateReplicationSuite) cancelBufferedNexusOperation(ctx context.Context, callback bufferedNexusCallback) {
	s.T().Helper()
	s.cancelNexusOperation(ctx, callback.url, callback.token)
}

func (s *NexusStateReplicationSuite) setupBufferedNexusEndpoint(
	ctx context.Context,
) (string, <-chan bufferedNexusCallback, chan struct{}) {
	s.T().Helper()
	operationCallbacks := make(chan bufferedNexusCallback, 2)
	allowLosingOnlyOperationStart := make(chan struct{})
	handler := nexustest.Handler{
		OnStartOperation: func(
			_ context.Context,
			_, operation string,
			_ *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			operationCallbacks <- bufferedNexusCallback{
				operation: operation,
				url:       options.CallbackURL,
				token:     options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
			}
			if operation == "losing-only-operation" {
				select {
				case <-allowLosingOnlyOperationStart:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: operation}, nil
		},
	}
	listenAddress := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddress, handler)
	for _, cluster := range s.clusters {
		cluster.OverrideDynamicConfig(
			s.T(),
			nexusoperations.CallbackURLTemplate,
			"http://"+s.clusters[0].Host().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback",
		)
	}

	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	for _, cluster := range s.clusters {
		_, err := cluster.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{Url: "http://" + listenAddress},
				}},
			},
		})
		s.Require().NoError(err)
	}
	return endpointName, operationCallbacks, allowLosingOnlyOperationStart
}

func scheduleBufferedNexusOperationCommand(endpoint, operation string) *commandpb.Command {
	return scheduleBufferedNexusOperationCommandWithTimeout(endpoint, operation, time.Minute)
}

func scheduleBufferedNexusOperationCommandWithTimeout(endpoint, operation string, timeout time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:               endpoint,
				Service:                "service",
				Operation:              operation,
				ScheduleToCloseTimeout: durationpb.New(timeout),
			},
		},
	}
}

func requestCancelNexusOperationCommand(scheduledEventID int64) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
		Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
			RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
}

func receiveBufferedNexusCallback(
	ctx context.Context,
	t require.TestingT,
	operationCallbacks <-chan bufferedNexusCallback,
	expectedOperation string,
) bufferedNexusCallback {
	select {
	case operationCallback := <-operationCallbacks:
		require.Equal(t, expectedOperation, operationCallback.operation)
		return operationCallback
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for Nexus operation callback", expectedOperation)
		return bufferedNexusCallback{}
	}
}

func receiveAnyBufferedNexusCallback(
	ctx context.Context,
	t require.TestingT,
	operationCallbacks <-chan bufferedNexusCallback,
) bufferedNexusCallback {
	select {
	case operationCallback := <-operationCallbacks:
		return operationCallback
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for Nexus operation callback")
		return bufferedNexusCallback{}
	}
}

func (s *NexusStateReplicationSuite) findBufferedNexusLosingBranch(
	ctx context.Context,
	branch bufferedNexusLosingBranch,
) []*historypb.HistoryEvent {
	s.T().Helper()
	history := s.findNonCurrentHistoryBranch(ctx, branch.Namespace.Name, branch.Namespace.ID, branch.Execution, func(history []*historypb.HistoryEvent) bool {
		return hasNexusEventForScheduledID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, branch.ScheduledEventID)
	})
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED ||
			event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED {
			s.Require().NotEqual(common.BufferedEventID, event.EventId)
			s.Require().Positive(event.EventId)
		}
	}
	return history
}

func findNexusScheduledEventID(history []*historypb.HistoryEvent, operation string) int64 {
	for _, event := range history {
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED &&
			event.GetNexusOperationScheduledEventAttributes().Operation == operation {
			return event.EventId
		}
	}
	return common.EmptyEventID
}

func hasNexusEventForScheduledID(history []*historypb.HistoryEvent, eventType enumspb.EventType, scheduledEventID int64) bool {
	for _, event := range history {
		if event.EventType != eventType {
			continue
		}
		var eventScheduledID int64
		switch eventType {
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
			eventScheduledID = event.EventId
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:
			eventScheduledID = event.GetNexusOperationStartedEventAttributes().GetScheduledEventId()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			eventScheduledID = event.GetNexusOperationCompletedEventAttributes().GetScheduledEventId()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
			eventScheduledID = event.GetNexusOperationFailedEventAttributes().GetScheduledEventId()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
			eventScheduledID = event.GetNexusOperationCanceledEventAttributes().GetScheduledEventId()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
			eventScheduledID = event.GetNexusOperationTimedOutEventAttributes().GetScheduledEventId()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED:
			eventScheduledID = event.GetNexusOperationCancelRequestCompletedEventAttributes().GetScheduledEventId()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
			eventScheduledID = event.GetNexusOperationCancelRequestFailedEventAttributes().GetScheduledEventId()
		default:
			return false
		}
		if eventScheduledID == scheduledEventID {
			return true
		}
	}
	return false
}
