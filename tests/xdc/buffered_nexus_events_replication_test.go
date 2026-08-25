package xdc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type bufferedNexusCallback struct {
	operation string
	url       string
	token     string
}

// TestBufferedNexusEventsReapplySharedOperationAndSkipLosingOnlyOperation covers the failure shape from
// temporalio/temporal#10986: a valid completion is followed in the same losing
// branch by a completion for an operation that does not exist on the winner.
// The missing operation must be skipped without aborting the batch containing the
// valid completion.
func (s *NexusStateReplicationSuite) TestBufferedNexusEventsReapplySharedOperationAndSkipLosingOnlyOperation() {
	if !s.enableTransitionHistory || s.chasmEnabled {
		s.T().Skip("this conflict-reapplication regression is specific to transition-history HSM Nexus operations")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.Require().NoError(err)

	// Phase 1: establish identical history with one shared, started Nexus operation.
	endpointName, operationCallbacks, allowLosingOnlyOperationStart := s.setupBufferedNexusEndpoint(ctx)

	workflowID := "buffered-nexus-conflict-" + uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: "buffered-nexus-conflict", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startResponse, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "buffered-nexus-conflict"},
		TaskQueue:           taskQueue,
		RequestId:           uuid.NewString(),
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(30 * time.Second),
		Identity:            "buffered-nexus-conflict-test",
	})
	s.Require().NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: startResponse.RunId}

	firstWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: firstWorkflowTask.TaskToken,
		Identity:  "buffered-nexus-conflict-test",
		Commands:  []*commandpb.Command{scheduleBufferedNexusOperationCommand(endpointName, "shared-operation")},
	})
	s.Require().NoError(err)
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
	_, err = s.clusters[0].FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: execution,
		SignalName:        "create-losing-operation",
		RequestId:         uuid.NewString(),
		Identity:          "buffered-nexus-conflict-test",
	})
	s.Require().NoError(err)
	scheduleLosingOnlyOperationTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  scheduleLosingOnlyOperationTask.TaskToken,
		Identity:                   "buffered-nexus-conflict-test",
		ForceCreateNewWorkflowTask: true,
		Commands:                   []*commandpb.Command{scheduleBufferedNexusOperationCommand(endpointName, "losing-only-operation")},
	})
	s.Require().NoError(err)
	heldWorkflowTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldWorkflowTask.TaskToken)
	losingOnlyOperationScheduledEventID := findNexusScheduledEventID(heldWorkflowTask.History.Events, "losing-only-operation")
	s.Require().Positive(losingOnlyOperationScheduledEventID)
	close(allowLosingOnlyOperationStart)
	losingOnlyOperationCallback := receiveBufferedNexusCallback(ctx, s.T(), operationCallbacks, "losing-only-operation")

	s.completeNexusOperation(ctx, "shared-result", sharedOperationCallback.url, sharedOperationCallback.token)
	s.completeNexusOperation(ctx, "losing-result", losingOnlyOperationCallback.url, losingOnlyOperationCallback.token)
	s.assertBufferedEventTypes(ctx, 0, ns, execution, []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
	})

	// Phase 3: fail over and create a winning branch on the new active cluster.
	s.failoverToNewActiveCluster(ctx, ns)
	s.writeSignalOnNewActive(ctx, namespace.NamespaceInfo.Id, ns, execution, "nexus-winner-signal")

	// Phase 4: resolve the conflict and verify losing-branch storage versus reapplication.
	s.releaseReplicationTask(ctx, replicationToOldActive)
	s.assertNoBufferedEvents(ctx, 0, ns, execution)
	losingHistory := s.findBufferedNexusLosingBranch(
		ctx,
		ns,
		namespace.NamespaceInfo.Id,
		execution,
		losingOnlyOperationScheduledEventID,
	)
	s.Require().True(hasNexusEventForScheduledID(losingHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, losingOnlyOperationScheduledEventID))
	s.Require().True(hasNexusEventForScheduledID(losingHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, losingOnlyOperationScheduledEventID))
	s.Require().Equal(2, countNexusEvents(losingHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED))

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
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  endpoint,
				Service:   "service",
				Operation: operation,
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
		default:
			return false
		}
		if eventScheduledID == scheduledEventID {
			return true
		}
	}
	return false
}

func countNexusEvents(history []*historypb.HistoryEvent, eventType enumspb.EventType) int {
	count := 0
	for _, event := range history {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}
