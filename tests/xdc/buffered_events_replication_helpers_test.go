package xdc

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"

	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
)

type blockedReplicationTask struct {
	execute func() error
	result  chan error
}

type bufferedNexusCallback struct {
	operation string
	url       string
	token     string
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
	losingBranchMarker := "losing-branch-marker-" + uuid.NewString()
	_, err := s.clusters[0].FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: execution,
		SignalName:        losingBranchMarker,
		RequestId:         uuid.NewString(),
		Identity:          "buffered-events-xdc-test",
	})
	s.Require().NoError(err)
	s.Require().True(s.hasBufferedEventType(ctx, 0, ns, execution, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED))

	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, namespaceID, ns, execution, winnerSignal)
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	losingHistory := s.findNonCurrentHistoryBranch(ctx, ns, namespaceID, execution, func(history []*historypb.HistoryEvent) bool {
		for _, eventType := range expectedTypes {
			if findHistoryEvent(history, eventType, nil) == nil {
				return false
			}
		}
		return true
	})
	for _, eventType := range expectedTypes {
		event := findHistoryEvent(losingHistory, eventType, nil)
		s.Require().NotNil(event, "%s must be written to the losing branch", eventType)
		s.Require().Positive(event.EventId)
		s.Require().NotEqual(common.BufferedEventID, event.EventId)
	}
	await.Require(ctx, s.T(), func(t *await.T) {
		s.tryReleaseReplicationTask(replicationToNewActive)
		require.True(
			t,
			hasSignalNamed(s.getWorkflowHistory(t.Context(), t.AssertionT(), 1, ns, execution), losingBranchMarker),
			"the losing branch marker must be reapplied before checking the rest of the batch",
		)
	}, replicationWaitTime, replicationCheckInterval)
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
	err := task.execute()
	task.result <- err
	var duplicateError *serviceerror.AlreadyExists
	var retryReplicationError *serviceerrors.RetryReplication
	s.Require().True(
		err == nil || errors.As(err, &duplicateError) || errors.As(err, &retryReplicationError),
		"replication task failed: %v",
		err,
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
	ns string,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	losingScheduledID int64,
) []*historypb.HistoryEvent {
	s.T().Helper()
	history := s.findNonCurrentHistoryBranch(ctx, ns, namespaceID, execution, func(history []*historypb.HistoryEvent) bool {
		return hasNexusEventForScheduledID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, losingScheduledID)
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
