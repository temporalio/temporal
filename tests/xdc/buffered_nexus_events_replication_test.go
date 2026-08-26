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
	"go.temporal.io/server/common/nexus/nexusrpc"
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
	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)

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
	execution := s.startBufferedEventsWorkflow(ctx, ns, workflowID, taskQueue)
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
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: firstTask.TaskToken,
		Identity:  "buffered-nexus-outcomes-test",
		Commands:  commands,
	})
	s.Require().NoError(err)

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
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  triggerTask.TaskToken,
		Identity:                   "buffered-nexus-outcomes-test",
		ForceCreateNewWorkflowTask: true,
		Commands: []*commandpb.Command{
			requestCancelNexusOperationCommand(findNexusScheduledEventID(history, "cancel-failed")),
		},
	})
	s.Require().NoError(err)
	heldTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldTask.TaskToken)
	close(allowCancellationResponse)

	s.failNexusOperation(ctx, operationCallbacks["failed"])
	s.cancelNexusOperation(ctx, operationCallbacks["canceled"].url, operationCallbacks["canceled"].token)
	expectedTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
	}
	s.assertBufferedEventTypesPresent(ctx, 0, ns, execution, expectedTypes)

	losingHistory := s.finishNaturallyBufferedConflict(
		ctx,
		ns,
		namespace.NamespaceInfo.Id,
		execution,
		replicationToOldActive,
		replicationToNewActive,
		expectedTypes,
		"nexus-outcomes-winner-signal",
	)
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
	ns := s.createGlobalNamespace()
	namespace, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)

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
	execution := s.startBufferedEventsWorkflow(ctx, ns, workflowID, taskQueue)
	firstTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: firstTask.TaskToken,
		Identity:  "buffered-nexus-cancel-completed-test",
		Commands:  []*commandpb.Command{scheduleBufferedNexusOperationCommand(endpointName, "cancel-completed")},
	})
	s.Require().NoError(err)
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
	_, err = s.clusters[0].FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  triggerTask.TaskToken,
		Identity:                   "buffered-nexus-cancel-completed-test",
		ForceCreateNewWorkflowTask: true,
		Commands:                   []*commandpb.Command{requestCancelNexusOperationCommand(scheduledID)},
	})
	s.Require().NoError(err)
	heldTask := s.pollBufferedEventsWorkflowTask(ctx, 0, ns, taskQueue)
	s.Require().NotEmpty(heldTask.TaskToken)
	close(allowCancellationResponse)
	select {
	case <-canceled:
	case <-ctx.Done():
		s.FailNow("timed out waiting for Nexus cancellation handler")
	}
	expectedTypes := []enumspb.EventType{enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED}
	s.assertBufferedEventTypesPresent(ctx, 0, ns, execution, expectedTypes)

	s.finishNaturallyBufferedConflict(
		ctx,
		ns,
		namespace.NamespaceInfo.Id,
		execution,
		replicationToOldActive,
		replicationToNewActive,
		expectedTypes,
		"nexus-cancel-completed-winner-signal",
	)
	winningHistory := s.getWorkflowHistory(ctx, s.T(), 1, ns, execution)
	s.Require().False(
		hasNexusEventForScheduledID(winningHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED, scheduledID),
		"Nexus cancellation results are not cherry-pickable and must remain only on the losing branch",
	)
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
