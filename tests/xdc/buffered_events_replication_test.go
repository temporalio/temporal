package xdc

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
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
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
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

var allBufferedEventTypes = []enumspb.EventType{
	enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
	enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
	enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
	enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
	enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
	enumspb.EVENT_TYPE_TIMER_FIRED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
	enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
	enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
	enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
	enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
	enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
	enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
	enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
	enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
	enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
	enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
	enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
	enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY,
	enumspb.EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED,
}

func (s *FunctionalClustersTestSuite) TestAllBufferedEventTypesFlushedAndReappliedAfterFailover() {
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

	s.clusters[0].InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.HistorySignalWorkflowInjectEvents, func() []*historypb.HistoryEvent {
			return bufferedEventsRequiringInjection(updateID)
		}),
		namespacepkg.ID(namespace.NamespaceInfo.Id),
	)
	optionsRequestID := s.completeActivityAndBufferExternalEvents(ctx, ns, execution, taskQueue)
	s.assertBufferedEventTypes(ctx, 0, ns, execution, allBufferedEventTypes)

	err = s.syncWorkflowState(ctx, namespace.NamespaceInfo.Id, execution)
	var workflowNotReady *serviceerror.WorkflowNotReady
	s.Require().ErrorAs(err, &workflowNotReady)

	// Phase 3: fail over and create a winning branch on the new active cluster.
	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, namespace.NamespaceInfo.Id, ns, execution, "winner-signal")

	// Phase 4: resolve the conflict and verify losing-branch storage versus reapplication.
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	s.assertBufferedEventsPersistedOnLosingBranch(ctx, ns, namespace.NamespaceInfo.Id, execution, updateID, optionsRequestID, allBufferedEventTypes)
	err = s.syncWorkflowState(ctx, namespace.NamespaceInfo.Id, execution)
	s.Require().NoError(err)

	// The flush and conflict resolution can produce more than one state-based task.
	// Release only this workflow's tasks until the reapplied inputs reach the active cluster.
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
	assertOnlyExpectedBufferedEventsReapplied(s.T(), finalHistory)
}

func bufferedEventsRequiringInjection(updateID string) []*historypb.HistoryEvent {
	// Generate the common cases through public APIs. Inject the remaining event
	// records into the same live history transaction because many outcomes are
	// mutually exclusive, and the two "properties modified externally" events
	// do not currently have a production API emitter.
	directlyGenerated := map[enumspb.EventType]struct{}{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:               {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:             {},
		enumspb.EVENT_TYPE_TIMER_FIRED:                         {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED: {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:         {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:  {},
	}
	events := make([]*historypb.HistoryEvent, 0, len(allBufferedEventTypes)-len(directlyGenerated))
	for _, eventType := range allBufferedEventTypes {
		if _, ok := directlyGenerated[eventType]; ok {
			continue
		}
		events = append(events, newBufferedHistoryEvent(eventType, updateID))
	}
	return events
}

func newBufferedHistoryEvent(eventType enumspb.EventType, updateID string) *historypb.HistoryEvent {
	event := &historypb.HistoryEvent{EventType: eventType}

	// Every history event's attribute field follows the enum's snake-case name.
	// Allocate the generated attribute message so buffer flush ID wiring traverses
	// the same shape as a production event.
	eventName := eventType.String()
	attributeName := strings.ToLower(eventName[:1]) + eventName[1:] + "EventAttributes"
	message := event.ProtoReflect()
	field := message.Descriptor().Fields().ByJSONName(attributeName)
	if field == nil {
		panic("history attribute field not found for " + eventType.String())
	}
	message.Set(field, message.NewField(field))

	if eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED {
		attributes := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
		attributes.Request = &updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "buffered-update"},
		}
		attributes.Origin = enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY
	}
	return event
}

func assertOnlyExpectedBufferedEventsReapplied(t require.TestingT, history []*historypb.HistoryEvent) {
	expectedCounts := map[enumspb.EventType]int{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:         2,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:  1,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED: 1,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:  1,
	}
	for _, eventType := range allBufferedEventTypes {
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
	_, err := s.clusters[0].AdminClient().SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
		NamespaceId:     namespaceID,
		Execution:       execution,
		TargetClusterId: 2,
		ArchetypeId:     chasm.WorkflowArchetypeID,
	})
	return err
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
		s.Require().True(err == nil || errors.As(err, &duplicateError), "replication task failed: %v", err)
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
