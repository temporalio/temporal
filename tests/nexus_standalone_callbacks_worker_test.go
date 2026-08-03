package tests

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexusoperationpb "go.temporal.io/api/nexusoperation/v1"
	"go.temporal.io/api/notificationservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

// Worker-variant completion callbacks deliver a closed standalone Nexus operation's outcome as a Nexus
// task on a task queue in the operation's own namespace, instead of POSTing it to a URL like the
// Nexus-variant callbacks covered in nexus_standalone_callbacks_test.go. The tests here therefore run a
// real worker polling that task queue, and assert on what it receives.

// The service and operation a worker callback targets are picked by whoever attaches the callback. These
// are the names the completion handler below registers.
const (
	workerCallbackService   = "temporal.api.notificationservice.v1.NotificationService"
	workerCallbackOperation = "OnComplete"
)

// newWorkerCallbackTestEnv builds an env with standalone Nexus operations and their Worker-variant
// completion callbacks enabled. Note that no callback address allowlist is needed: a worker callback
// names a task queue rather than a URL.
func (s *NexusStandaloneTestSuite) newWorkerCallbackTestEnv(opts ...testcore.TestOption) *NexusTestEnv {
	return s.newTestEnv(append(opts,
		testcore.WithDynamicConfig(nexusoperation.EnableWorkerCallbacks, true),
	)...)
}

// workerCallbackHandler is the worker side of a Worker-variant callback: it records every completion
// delivered to it, and lets a test decide what the handler returns, which is how the callback's retry
// and terminal-failure paths are driven.
type workerCallbackHandler struct {
	mu       sync.Mutex
	received []*notificationservice.OnCompleteRequest
	// respondWith is called with the 1-based delivery count. A nil error accepts the completion.
	respondWith func(delivery int) error
}

func (h *workerCallbackHandler) handle(req *notificationservice.OnCompleteRequest) error {
	h.mu.Lock()
	h.received = append(h.received, req)
	delivery, respondWith := len(h.received), h.respondWith
	h.mu.Unlock()

	if respondWith == nil {
		return nil
	}
	return respondWith(delivery)
}

func (h *workerCallbackHandler) deliveries() []*notificationservice.OnCompleteRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.received)
}

// startWorkerCallbackHandler runs a worker polling taskQueue with the completion Nexus service
// registered on it, making that task queue a valid worker callback target.
func (s *NexusStandaloneTestSuite) startWorkerCallbackHandler(
	env *NexusTestEnv,
	taskQueue string,
	respondWith func(delivery int) error,
) *workerCallbackHandler {
	t := s.T()
	h := &workerCallbackHandler{respondWith: respondWith}

	service := nexus.NewService(workerCallbackService)
	service.MustRegister(nexus.NewSyncOperation(
		workerCallbackOperation,
		func(
			_ context.Context,
			req *notificationservice.OnCompleteRequest,
			_ nexus.StartOperationOptions,
		) (*notificationservice.OnCompleteResponse, error) {
			return &notificationservice.OnCompleteResponse{}, h.handle(req)
		},
	))

	w := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
	w.RegisterNexusService(service)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	return h
}

func workerCompletionCallback(taskQueue string, sourceContext *commonpb.Payload) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: taskQueue,
				Service:       workerCallbackService,
				Operation:     workerCallbackOperation,
				SourceContext: sourceContext,
			},
		},
	}
}

// awaitWorkerCallbackDeliveries waits until the handler has received wantCount completions and returns
// them. Assertions run against the returned values rather than inside the handler, so that a mismatch
// fails readably instead of stalling the callback.
func (s *NexusStandaloneTestSuite) awaitWorkerCallbackDeliveries(
	h *workerCallbackHandler,
	wantCount int,
) []*notificationservice.OnCompleteRequest {
	s.T().Helper()

	var got []*notificationservice.OnCompleteRequest
	s.Await(func(s *NexusStandaloneTestSuite) {
		got = h.deliveries()
		s.Len(got, wantCount)
	}, 20*time.Second, 100*time.Millisecond)
	return got
}

// syncNexusHandler builds a Nexus handler that completes every operation synchronously with the given
// result, closing the standalone operation as soon as it is dispatched.
func syncNexusHandler(result any) nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: result}, nil
		},
	}
}

// asyncNexusHandler builds a Nexus handler that leaves every operation STARTED, so that the standalone
// operation never closes and its callbacks stay in STANDBY.
func asyncNexusHandler() nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-operation-token"}, nil
		},
	}
}

// TestStandaloneNexusOperationWorkerCallbacks covers Worker-variant completion callbacks end to end:
// what the worker receives, and how its answer decides the callback's fate.
func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationWorkerCallbacks() {
	s.Run("DeliveredOnSuccess", func(s *NexusStandaloneTestSuite) {
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		taskQueue := testcore.RandomizeStr(t.Name())
		handler := s.startWorkerCallbackHandler(env, taskQueue, nil)
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, syncNexusHandler("operation-result"))

		sourceContext := payload.EncodeString("source-context-value")
		operationID := testvars.New(t).Any().String()
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(taskQueue, sourceContext)},
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		deliveries := s.awaitWorkerCallbackDeliveries(handler, 1)
		outcome := deliveries[0].GetOutcome()
		s.Nil(outcome.GetFailure())
		s.Require().Len(outcome.GetSuccess().GetPayloads(), 1)
		var result string
		s.NoError(payload.Decode(outcome.GetSuccess().GetPayloads()[0], &result))
		s.Equal("operation-result", result)
		// The source context is opaque to the server: it arrives at the handler exactly as attached.
		protorequire.ProtoEqual(t, sourceContext, deliveries[0].GetSourceContext())

		// The worker accepted the completion, so the callback is done.
		infos := s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
		worker := infos[0].GetInfo().GetCallback().GetWorker()
		s.Equal(taskQueue, worker.GetTaskQueueName())
		s.Equal(workerCallbackService, worker.GetService())
		s.Equal(workerCallbackOperation, worker.GetOperation())
		s.NotNil(infos[0].GetInfo().GetOutcome().GetSuccess())
		s.NotNil(infos[0].GetTrigger().GetOperationCompleted())
	})

	s.Run("DeliveredOnFailure", func(s *NexusStandaloneTestSuite) {
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		taskQueue := testcore.RandomizeStr(t.Name())
		handler := s.startWorkerCallbackHandler(env, taskQueue, nil)
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, nexustest.Handler{
			OnStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.OperationError{
					State: nexus.OperationStateFailed,
					Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "deliberate failure"}},
				}
			},
		})

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(taskQueue, nil)},
		})
		s.NoError(err)

		deliveries := s.awaitWorkerCallbackDeliveries(handler, 1)
		outcome := deliveries[0].GetOutcome()
		s.Nil(outcome.GetSuccess())
		s.Require().NotNil(outcome.GetFailure())
		// The operation error is unwrapped before delivery, so the handler receives the failure the
		// operation closed with: an OperationError carrying the Nexus handler's failure as its cause.
		s.Equal("OperationError", outcome.GetFailure().GetApplicationFailureInfo().GetType())
		s.Equal("deliberate failure", outcome.GetFailure().GetCause().GetMessage())

		s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
	})

	s.Run("RetriedWhenTheWorkerFailsTheTask", func(s *NexusStandaloneTestSuite) {
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		taskQueue := testcore.RandomizeStr(t.Name())
		// Failing the Nexus task is a delivery failure, not an answer, so the callback backs off and
		// redelivers rather than giving up.
		handler := s.startWorkerCallbackHandler(env, taskQueue, func(delivery int) error {
			if delivery == 1 {
				return &nexus.HandlerError{
					Type:          nexus.HandlerErrorTypeInternal,
					RetryBehavior: nexus.HandlerErrorRetryBehaviorRetryable,
					Cause:         &nexus.FailureError{Failure: nexus.Failure{Message: "not ready yet"}},
				}
			}
			return nil
		})
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, syncNexusHandler("operation-result"))

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(taskQueue, nil)},
		})
		s.NoError(err)

		deliveries := s.awaitWorkerCallbackDeliveries(handler, 2)
		// Every attempt carries the same completion, and the same Nexus request ID, so that a handler
		// can deduplicate a redelivery.
		protorequire.ProtoEqual(t, deliveries[0], deliveries[1])

		infos := s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
		s.EqualValues(2, infos[0].GetInfo().GetAttempt())
	})

	s.Run("FailedWhenTheWorkerFailsTheOperation", func(s *NexusStandaloneTestSuite) {
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		taskQueue := testcore.RandomizeStr(t.Name())
		// An operation error is the handler's answer, not a delivery problem: the callback fails
		// terminally instead of retrying.
		handler := s.startWorkerCallbackHandler(env, taskQueue, func(int) error {
			return &nexus.OperationError{
				State: nexus.OperationStateFailed,
				Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "completion rejected"}},
			}
		})
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, syncNexusHandler("operation-result"))

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(taskQueue, nil)},
		})
		s.NoError(err)

		infos := s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_FAILED)
		s.NotNil(infos[0].GetInfo().GetOutcome().GetFailure())
		s.Contains(infos[0].GetInfo().GetLastAttemptFailure().GetMessage(), "completion rejected")
		// FAILED is terminal, so the single delivery was the only one.
		s.Len(handler.deliveries(), 1)
	})

	s.Run("DeliveredAlongsideANexusCallback", func(s *NexusStandaloneTestSuite) {
		// Both variants can be attached to the same operation, and each is delivered over its own
		// transport.
		env := s.newCallbackTestEnv()
		env.OverrideDynamicConfig(nexusoperation.EnableWorkerCallbacks, true)
		t := s.T()

		taskQueue := testcore.RandomizeStr(t.Name())
		workerHandler := s.startWorkerCallbackHandler(env, taskQueue, nil)
		nexusHandler := newCompletionHandler(t)
		callbackURL := s.runNexusCompletionHTTPServer(t, nexusHandler)
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, syncNexusHandler("operation-result"))

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: operationID,
			Endpoint:    endpointName,
			CompletionCallbacks: []*commonpb.Callback{
				nexusCompletionCallback(callbackURL),
				workerCompletionCallback(taskQueue, nil),
			},
		})
		s.NoError(err)

		completion := s.awaitCompletion(nexusHandler)
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		s.awaitWorkerCallbackDeliveries(workerHandler, 1)
		s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_SUCCEEDED)
	})

	s.Run("DeliveredToEveryTaskQueue", func(s *NexusStandaloneTestSuite) {
		// Each callback is delivered independently, to the task queue it names.
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		firstTaskQueue := testcore.RandomizeStr(t.Name() + "-1")
		secondTaskQueue := testcore.RandomizeStr(t.Name() + "-2")
		firstHandler := s.startWorkerCallbackHandler(env, firstTaskQueue, nil)
		secondHandler := s.startWorkerCallbackHandler(env, secondTaskQueue, nil)
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, syncNexusHandler("operation-result"))

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: operationID,
			Endpoint:    endpointName,
			CompletionCallbacks: []*commonpb.Callback{
				workerCompletionCallback(firstTaskQueue, payload.EncodeString("first")),
				workerCompletionCallback(secondTaskQueue, payload.EncodeString("second")),
			},
		})
		s.NoError(err)

		first := s.awaitWorkerCallbackDeliveries(firstHandler, 1)
		second := s.awaitWorkerCallbackDeliveries(secondHandler, 1)
		// Each handler sees the source context of its own callback, not the other's.
		protorequire.ProtoEqual(t, payload.EncodeString("first"), first[0].GetSourceContext())
		protorequire.ProtoEqual(t, payload.EncodeString("second"), second[0].GetSourceContext())

		s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_SUCCEEDED)
	})

	s.Run("DeliveredOnTerminate", func(s *NexusStandaloneTestSuite) {
		// Termination closes the operation from the outside; the callback fires for it like any other
		// terminal state.
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		taskQueue := testcore.RandomizeStr(t.Name())
		handler := s.startWorkerCallbackHandler(env, taskQueue, nil)
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, asyncNexusHandler())

		operationID := testvars.New(t).Any().String()
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(taskQueue, nil)},
		})
		s.NoError(err)

		// The operation stays STARTED until it is terminated, so nothing is delivered before then.
		s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_STANDBY)
		s.Empty(handler.deliveries())

		_, err = env.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       startResp.GetRunId(),
			Identity:    "test-identity",
			Reason:      "terminated by the test",
		})
		s.NoError(err)

		deliveries := s.awaitWorkerCallbackDeliveries(handler, 1)
		s.Nil(deliveries[0].GetOutcome().GetSuccess())
		s.Require().NotNil(deliveries[0].GetOutcome().GetFailure())
		s.Contains(deliveries[0].GetOutcome().GetFailure().GetMessage(), "terminated by the test")

		s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
	})

	s.Run("AttachedOnConflict", func(s *NexusStandaloneTestSuite) {
		// A worker callback can also be attached to an operation that is already running.
		env := s.newWorkerCallbackTestEnv()
		t := s.T()

		firstTaskQueue := testcore.RandomizeStr(t.Name() + "-1")
		secondTaskQueue := testcore.RandomizeStr(t.Name() + "-2")
		firstHandler := s.startWorkerCallbackHandler(env, firstTaskQueue, nil)
		secondHandler := s.startWorkerCallbackHandler(env, secondTaskQueue, nil)
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, asyncNexusHandler())

		operationID := testvars.New(t).Any().String()
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           "first-request",
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(firstTaskQueue, nil)},
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		attachResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           "second-request",
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback(secondTaskQueue, nil)},
			IdConflictPolicy:    enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &nexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		})
		s.NoError(err)
		s.False(attachResp.GetStarted(), "the second request must not have created an operation")

		infos := s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_STANDBY)
		s.Equal(firstTaskQueue, infos[0].GetInfo().GetCallback().GetWorker().GetTaskQueueName())
		s.Equal(secondTaskQueue, infos[1].GetInfo().GetCallback().GetWorker().GetTaskQueueName())

		// Both are released together when the operation closes, including the one attached later.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       startResp.GetRunId(),
			Reason:      "terminated by the test",
		})
		s.NoError(err)

		s.awaitWorkerCallbackDeliveries(firstHandler, 1)
		s.awaitWorkerCallbackDeliveries(secondHandler, 1)
		s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_SUCCEEDED)
	})

	s.Run("RejectsIncompleteCallbacks", func(s *NexusStandaloneTestSuite) {
		env := s.newWorkerCallbackTestEnv()
		endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{})

		// A worker callback with no task queue has nowhere to be delivered, so it is rejected up front
		// rather than failing at delivery time.
		cb := workerCompletionCallback("", nil)
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{cb},
		})
		var invalidArgErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgErr)
		s.ErrorContains(err, "completion_callbacks[0].worker.task_queue_name is required")
	})
}

// TestStandaloneNexusOperationWorkerCallbacksDisabled confirms Worker callbacks have their own
// namespace flag, independent of the one gating Nexus callbacks.
func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationWorkerCallbacksDisabled() {
	// newCallbackTestEnv enables Nexus callbacks but leaves
	// nexusoperation.EnableWorkerCallbacks at its default of false.
	env := s.newCallbackTestEnv()
	// The operations started below stay STARTED, so no callback is ever delivered: these cases are only
	// about what the frontend accepts.
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), asyncNexusHandler())

	s.Run("StartWithWorkerCallbackFails", func(s *NexusStandaloneTestSuite) {
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback("completions-task-queue", nil)},
		})
		var invalidArgErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgErr)
		s.ErrorContains(err, "worker completion callbacks are not enabled for this namespace")
	})

	s.Run("OnConflictAttachWorkerCallbackFails", func(s *NexusStandaloneTestSuite) {
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{workerCompletionCallback("completions-task-queue", nil)},
			IdConflictPolicy:    enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &nexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		})
		s.ErrorContains(err, "worker completion callbacks are not enabled for this namespace")
	})

	s.Run("NexusCallbackStillAllowed", func(s *NexusStandaloneTestSuite) {
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb")},
		})
		s.NoError(err)
	})
}
