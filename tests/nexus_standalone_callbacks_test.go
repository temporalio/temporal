package tests

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	apinexusoperationpb "go.temporal.io/api/nexusoperation/v1"
	nexusoperationpb "go.temporal.io/api/nexusoperation/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

// NexusStandaloneCallbacksTestSuite covers standalone Nexus operation
// behavior with regard to completion callbacks.
type NexusStandaloneCallbacksTestSuite struct {
	parallelsuite.Suite[*NexusStandaloneCallbacksTestSuite]
}

func TestNexusStandaloneCallbacksTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusStandaloneCallbacksTestSuite{})
}

func (s *NexusStandaloneCallbacksTestSuite) newTestEnv(enableCallbacks bool) *NexusTestEnv {
	var kinds []callbacks.Kind
	if enableCallbacks {
		kinds = append(kinds, callbacks.KindNexus)
	}

	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, kinds),
	)
	if enableCallbacks {
		env.OverrideDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		)
	}
	return env
}

// awaitCallbackInfo polls DescribeNexusOperationExecution until the operation's single completion
// callback reaches wantState, then returns it.
func (s *NexusStandaloneCallbacksTestSuite) awaitCallbackInfo(
	env *NexusTestEnv,
	operationID string,
	wantState enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	s.T().Helper()

	var cbInfo *callbackpb.CallbackInfo
	await.Require(s.Context(), s.T(), func(c *await.T) {
		cbs := env.describeNexusOperation(c.Context(), c, operationID).GetCompletionCallbacks()
		require.Len(c, cbs, 1)
		cbInfo = cbs[0].GetInfo()
		require.NotNil(c, cbInfo)
		require.Equal(c, wantState, cbInfo.GetState())
	}, 10*time.Second, 100*time.Millisecond)
	return cbInfo
}

func (s *NexusStandaloneCallbacksTestSuite) TestCompletionCallbacks() {
	env := s.newTestEnv(true)
	alwaysSuccessEndpointName := env.createSyncSuccessEndpoint(s.Context(), s.T(), "operation-result")

	s.Run("DeliveredOnSuccess", func(s *NexusStandaloneCallbacksTestSuite) {
		ctx := s.Context()
		ch, callbackAddress := newNexusCompletionHandler(s.T())

		operationID := testvars.New(s.T()).Any().String()
		startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            alwaysSuccessEndpointName,
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback(callbackAddress)},
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		// Verify the callback was actually delivered with the operation's result.
		var completionBody []byte
		select {
		case completion := <-ch.requestCh:
			s.Equal(nexus.OperationStateSucceeded, completion.State)
			s.Nil(completion.Error)
			s.False(completion.StartTime.IsZero())
			s.False(completion.CloseTime.IsZero())
			body, readErr := io.ReadAll(completion.HTTPRequest.Body)
			_ = completion.HTTPRequest.Body.Close()
			s.NoError(readErr)
			s.JSONEq(`"operation-result"`, string(body))
			completionBody = body
			// The completion carries a back-link to the operation that produced it.
			wantLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
				Namespace:   env.Namespace().String(),
				OperationId: operationID,
				RunId:       startResp.GetRunId(),
			})
			s.Require().Len(completion.Links, 1)
			s.Equal(wantLink.URL.String(), completion.Links[0].URL.String())
			s.Equal(wantLink.Type, completion.Links[0].Type)
			// Unblock CompleteOperation so it returns 200 OK to the callback library.
			ch.requestCompleteCh <- nil
		case <-ctx.Done():
			s.FailNow("timed out waiting for the completion callback")
		}

		// Verify the operation is in completed state, and that the callback delivered the
		// same result the operation.
		descResp := env.describeNexusOperation(ctx, s.T(), operationID)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		s.Equal(string(descResp.GetResult().GetData()), string(completionBody))

		// Wait for the callback to complete and confirm it has a Success result.
		cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
		s.NotNil(cbInfo.GetSuccess())
		s.Equal(callbackAddress, cbInfo.GetCallback().GetNexus().GetUrl())
	})

	s.Run("DeliveredOnFailure", func(s *NexusStandaloneCallbacksTestSuite) {
		ctx := s.Context()
		alwaysFailingEndpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexustest.Handler{
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
		ch, callbackAddress := newNexusCompletionHandler(s.T())

		operationID := testvars.New(s.T()).Any().String()
		_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            alwaysFailingEndpointName,
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback(callbackAddress)},
		})
		s.NoError(err)

		// Verify the callback was actually delivered with failure state.
		select {
		case completion := <-ch.requestCh:
			s.Equal(nexus.OperationStateFailed, completion.State)
			s.False(completion.StartTime.IsZero())
			s.False(completion.CloseTime.IsZero())
			s.Require().NotNil(completion.Error)
			var failureErr *nexus.FailureError
			s.Require().ErrorAs(completion.Error.Cause, &failureErr)
			// The handler's error is wrapped as an OperationError whose cause carries the original
			// message, which is how a Nexus operation failure round-trips through a completion.
			tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(failureErr.Failure)
			s.NoError(convErr)
			s.Equal("OperationError", tFailure.GetApplicationFailureInfo().GetType())
			s.Equal("deliberate failure", tFailure.GetCause().GetMessage())
			ch.requestCompleteCh <- nil
		case <-ctx.Done():
			s.FailNow("timed out waiting for the completion callback")
		}

		// Verify the operation is in failed state.
		descResp := env.describeNexusOperation(ctx, s.T(), operationID)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())

		// The operation may have failed, but the callback reporting that failure was successful.
		cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
		s.NotNil(cbInfo.GetSuccess())
	})

	// Verify that if the callback fails to be delivered for some reason, that the failure is
	// persisted correctly and available from the Describe operation.
	s.Run("CallbackDeliveryFailure", func(s *NexusStandaloneCallbacksTestSuite) {
		ctx := s.Context()
		ch, callbackAddress := newNexusCompletionHandler(s.T())

		operationID := testvars.New(s.T()).Any().String()
		_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            alwaysSuccessEndpointName,
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback(callbackAddress)},
		})
		s.NoError(err)

		// Simulate the completion handler returning a retryable error followed by an unretryable
		// error. Confirm the SANO's CallbackInfo includes the terminal failure.
		for deliveryAttempt := 1; deliveryAttempt <= 2; deliveryAttempt++ {
			select {
			case completion := <-ch.requestCh:
				s.Equal(nexus.OperationStateSucceeded, completion.State)
				if deliveryAttempt == 1 {
					// While the handler is blocked here the callback is in flight: Describe reports
					// it as scheduled, with no attempts recorded yet.
					cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SCHEDULED)
					s.EqualValues(0, cbInfo.GetAttempt())
					s.Nil(cbInfo.GetLastAttemptFailure())
					s.Nil(cbInfo.GetResult())

					// Retryable error.
					ch.requestCompleteCh <- nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "delivery #1")
				} else {
					// The second delivery attempt: Describe now describes the previous attempt.
					cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SCHEDULED)
					s.EqualValues(1, cbInfo.GetAttempt()) // 1 attempt so far, the 2nd is in-progress.
					s.NotNil(cbInfo.GetLastAttemptFailure())
					s.Contains(cbInfo.GetLastAttemptFailure().GetMessage(), "delivery #1")
					s.Nil(cbInfo.GetResult())

					// Unretryable error.
					ch.requestCompleteCh <- nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "delivery #2")
				}
			case <-ctx.Done():
				s.FailNow("timed out waiting for the completion callback")
			}
		}

		// A failed callback delivery does not affect the operation itself.
		gotStatus := env.describeNexusOperation(ctx, s.T(), operationID).GetInfo().GetStatus()
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, gotStatus)

		// Verify the completion callback delivery has failed.
		cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_FAILED)
		// Both the last delivery failure and the terminal failure come from delivery #2.
		const lastDeliveryFailureMessage = "handler error (BAD_REQUEST): delivery #2"
		s.NotNil(cbInfo.GetFailure())
		s.Equal(lastDeliveryFailureMessage, cbInfo.GetFailure().GetMessage())
		s.Equal(lastDeliveryFailureMessage, cbInfo.GetLastAttemptFailure().GetMessage())
	})

	s.Run("DescribeReportsStandbyBeforeClose", func(s *NexusStandaloneCallbacksTestSuite) {
		ctx := s.Context()
		endpointName := env.createAsyncEndpoint(ctx, s.T())

		operationID := testvars.New(s.T()).Any().String()
		_, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: operationID,
			Endpoint:    endpointName,
			CompletionCallbacks: []*commonpb.Callback{
				nexusCompletionCallback("http://localhost/cb1"),
				nexusCompletionCallback("http://localhost/cb2"),
			},
		})
		s.NoError(err)

		// The operation stays STARTED, so its callbacks stay in STANDBY with no result.
		infos := env.describeNexusOperation(ctx, s.T(), operationID).GetCompletionCallbacks()
		s.Len(infos, 2)
		for _, info := range infos {
			// Every callback on a standalone operation is triggered by the operation completing.
			s.NotNil(info.GetTrigger().GetOperationCompleted())
			s.Equal(enumspb.CALLBACK_STATE_STANDBY, info.GetInfo().GetState())
			s.NotNil(info.GetInfo().GetRegistrationTime())
			s.Nil(info.GetInfo().GetResult())
		}
	})

	s.Run("AttachOnConflict", func(s *NexusStandaloneCallbacksTestSuite) {
		ctx := s.Context()
		endpointName := env.createAsyncEndpoint(ctx, s.T())

		operationID := testvars.New(s.T()).Any().String()
		startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           "first-request",
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb1")},
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		attachReq := &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           "second-request",
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb2")},
			IdConflictPolicy:    enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &nexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		}
		attachResp, err := env.startNexusOperation(ctx, attachReq)
		s.NoError(err)
		s.False(attachResp.GetStarted(), "the second request must not have created an operation")
		s.Equal(startResp.GetRunId(), attachResp.GetRunId())

		callbackInfos := env.describeNexusOperation(ctx, s.T(), operationID).GetCompletionCallbacks()
		s.Require().Len(callbackInfos, 2)
		s.Equal("http://localhost/cb1", callbackInfos[0].GetInfo().GetCallback().GetNexus().GetUrl())
		s.Equal("http://localhost/cb2", callbackInfos[1].GetInfo().GetCallback().GetNexus().GetUrl())
		s.Equal(enumspb.CALLBACK_STATE_STANDBY, callbackInfos[0].GetInfo().GetState())
		s.Equal(enumspb.CALLBACK_STATE_STANDBY, callbackInfos[1].GetInfo().GetState())

		// Replaying the same attach must not duplicate any of the callbacks.
		_, err = env.startNexusOperation(ctx, attachReq)
		s.NoError(err)
		newDescribeResp := env.describeNexusOperation(ctx, s.T(), operationID)
		s.Len(newDescribeResp.GetCompletionCallbacks(), 2)
	})

	// Confirm that SANO fails with callback kinds that are not enabled.
	s.Run("RejectNonNexusCallbacks", func(s *NexusStandaloneCallbacksTestSuite) {
		for _, tc := range []struct {
			name     string
			callback *commonpb.Callback
			errMsg   string
		}{
			{
				name: "worker",
				callback: &commonpb.Callback{Variant: &commonpb.Callback_Worker_{Worker: &commonpb.Callback_Worker{
					TaskQueueName: "completions-task-queue",
					Service:       "HTTPAdapter",
					Operation:     "DeliverAsWebhook",
				}}},
				// The callback is well-formed, but the Worker kind is not enabled. (See newTestEnv.)
				errMsg: "worker callbacks are not enabled for this execution type",
			},
			{
				name:     "unset",
				callback: &commonpb.Callback{},
				errMsg:   "unknown callback variant",
			},
		} {
			s.Run(tc.name, func(s *NexusStandaloneCallbacksTestSuite) {
				resp, err := env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
					OperationId:         testvars.New(s.T()).Any().String(),
					Endpoint:            alwaysSuccessEndpointName,
					CompletionCallbacks: []*commonpb.Callback{tc.callback},
				})
				s.Nil(resp)

				var unimplementedErr *serviceerror.Unimplemented
				s.ErrorAs(err, &unimplementedErr)
				s.ErrorContains(err, tc.errMsg)
			})
		}
	})

	// Links and callbacks are attached by independent options, so a single request may carry both.
	s.Run("AttachLinksAndCallbacksOnConflict", func(s *NexusStandaloneCallbacksTestSuite) {
		ctx := s.Context()
		t := s.T()

		endpointName := env.createRandomExternalNexusServer(s.Context(), t, nexustest.Handler{
			OnStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-operation-token"}, nil
			},
		})

		firstLink := standaloneNexusTestLink(env, "first-wf")
		secondLink := standaloneNexusTestLink(env, "second-wf")

		operationID := testvars.New(t).Any().String()
		startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           "first-request",
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb1")},
			Links:               []*commonpb.Link{firstLink},
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		attachResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			RequestId:           "second-request",
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb2")},
			Links:               []*commonpb.Link{secondLink},
			IdConflictPolicy:    enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &nexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			},
		})
		s.NoError(err)
		s.False(attachResp.GetStarted(), "the second request must not have created an operation")

		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
		})
		s.NoError(err)
		// Links are stored per request ID, so their relative order is non-deterministic.
		protorequire.ProtoElementsMatch(t,
			[]*commonpb.Link{firstLink, secondLink},
			descResp.GetInfo().GetLinks())
	})
}

// TestCallbacksDisabled confirms the per-namespace feature flag gates the whole surface, including
// the on-conflict attach path.
func (s *NexusStandaloneCallbacksTestSuite) TestCallbacksDisabled() {
	// Test environment with SANO not supporting completion callbacks.
	env := s.newTestEnv(false)
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{})
	cbs := []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb")}

	s.Run("StartWithCallbacksFails", func(s *NexusStandaloneCallbacksTestSuite) {
		_, err := env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: cbs,
		})
		s.ErrorContains(err, "completion callbacks are not enabled for this namespace")
	})

	s.Run("OnConflictAttachCallbacksFails", func(s *NexusStandaloneCallbacksTestSuite) {
		_, err := env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: cbs,
			IdConflictPolicy:    enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &nexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		})
		s.ErrorContains(err, "completion callbacks are not enabled for this namespace")
	})
}

// TestLinkingE2E follows the links along a standalone Nexus operation's whole lifecycle: the request
// the operation sends its own handler, the link that handler answers with, and finally the
// Worker-variant completion callback the operation's outcome is delivered to.
//
// Both handlers are driven with raw PollNexusTaskQueue/RespondNexusTaskCompleted calls instead of SDK
// workers. That is deliberate: the Go SDK (v1.44.0) drops link types it does not recognize, including
// the Link.Callback a worker callback delivery carries, so an SDK worker never sees the links this
// test exists to assert. Polling directly also keeps every link exactly as the server sent it, with
// no data converter or link converter in between.
func (s *NexusStandaloneCallbacksTestSuite) TestLinkingE2E() {
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		// Worker callbacks are only implemented by the CHASM callback library.
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnableCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
	)

	t := s.T()
	// Every wait in this test is a long poll, so the deadline is what fails the test rather than
	// hanging it.
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	// The operation's handler and the callback's handler poll their own task queues, so the two hops
	// can be observed independently of each other.
	operationTaskQueue := "sano-tq-" + uuid.NewString()
	callbackTaskQueue := "callback-tq-" + uuid.NewString()
	endpointName := env.createNexusEndpoint(
		ctx, t, testcore.RandomizedNexusEndpoint(t.Name()), operationTaskQueue).GetSpec().GetName()

	// A callback is identified by a server-generated request ID derived from the request that
	// registered it and the callback's index within that request. It ends up both in the Nexus request
	// ID of the delivery and in the link the delivery carries.
	requestID := uuid.NewString()
	callbackRequestID := requestID + "-0"
	operationID := testvars.New(t).Any().String()
	sourceContext := payload.EncodeString("source-context")
	workerCallback := &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: callbackTaskQueue,
				Service:       "completion-service",
				Operation:     "on-complete",
				SourceContext: sourceContext,
			},
		},
	}

	startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            endpointName,
		RequestId:           requestID,
		CompletionCallbacks: []*commonpb.Callback{workerCallback},
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	// Hop 1: the operation's own request carries a back-link to the operation, which is how the
	// handler can point back at what invoked it.
	operationTask := env.awaitNexusTask(ctx, t, operationTaskQueue)
	operationStart := operationTask.GetRequest().GetStartOperation()
	wantOperationLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
		Namespace:   env.Namespace().String(),
		OperationId: operationID,
		RunId:       startResp.GetRunId(),
	})
	s.Require().Len(operationStart.GetLinks(), 1)
	s.Equal(wantOperationLink.URL.String(), operationStart.GetLinks()[0].GetUrl())
	s.Equal(wantOperationLink.Type, operationStart.GetLinks()[0].GetType())

	// Hop 2: the handler completes the operation and answers with a link of its own, naming the
	// workflow it ran the operation on. The operation records it.
	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: "handler-workflow",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	operationResult := payload.EncodeString("operation-result")
	env.respondNexusTaskCompleted(ctx, t, operationTask.GetTaskToken(), &nexusTaskResponse{
		StartResult: &nexus.HandlerStartOperationResultSync[*commonpb.Payload]{Value: operationResult},
		Links:       []nexus.Link{commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink)},
	})

	// The handler's link is persisted on the operation rather than only travelling on the response,
	// so a caller that describes the operation can follow it to the handler. The operation closes
	// asynchronously to the response, so wait for the completion to land first.
	wantOperationLinks := []*commonpb.Link{
		{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: handlerLink}},
	}
	await.Require(ctx, t, func(c *await.T) {
		descResp := env.describeNexusOperation(c.Context(), c, operationID)
		require.Equal(c, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		protorequire.ProtoEqual(c, operationResult, descResp.GetResult())
		protorequire.ProtoSliceEqual(c, wantOperationLinks, descResp.GetInfo().GetLinks())
	}, 10*time.Second, 100*time.Millisecond)

	// Hop 3: completing the operation releases the callback, which is delivered as a Nexus task on
	// the callback's own task queue, naming the service and operation the callback was registered
	// with.
	callbackTask := env.awaitNexusTask(ctx, t, callbackTaskQueue)
	callbackStart := callbackTask.GetRequest().GetStartOperation()
	s.Equal("completion-service", callbackStart.GetService())
	s.Equal("on-complete", callbackStart.GetOperation())
	// The callback's own request ID doubles as the delivery's Nexus request ID, so a redelivery is
	// idempotent from the handler's perspective.
	s.Equal(callbackRequestID, callbackStart.GetRequestId())

	// The delivery carries the operation's outcome, along with the context the callback was
	// registered with.
	var onComplete notificationpb.OnCompleteRequest
	s.NoError(payload.Decode(callbackStart.GetPayload(), &onComplete))
	s.Nil(onComplete.GetFailure())
	protorequire.ProtoEqual(t, operationResult, onComplete.GetSuccess())
	protorequire.ProtoEqual(t, sourceContext, onComplete.GetSourceContext())

	// The delivery links to the callback attached to the operation rather than to the operation
	// itself: what the handler is being told about is this callback's completion, and the request ID
	// is what distinguishes callbacks registered on the same operation by different requests.
	wantCallbackLink := &commonpb.Link_Callback{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
			BusinessId: operationID,
			RunId:      startResp.GetRunId(),
		},
		RequestId: callbackRequestID,
	}
	wantCallbackNexusLink, err := commonnexus.ConvertLinkCallbackToNexusLink(wantCallbackLink)
	s.NoError(err)
	s.Require().Len(callbackStart.GetLinks(), 1)
	s.Equal(wantCallbackNexusLink.URL.String(), callbackStart.GetLinks()[0].GetUrl())
	s.Equal(wantCallbackNexusLink.Type, callbackStart.GetLinks()[0].GetType())
	// It also round-trips back into the Link_Callback a handler would resolve it to, so the link is
	// consumable and not merely well-formed.
	gotCallbackLink, err := commonnexus.ConvertNexusLinkToLinkCallback(
		commonnexus.ConvertLinksFromProto(callbackStart.GetLinks())[0])
	s.NoError(err)
	protorequire.ProtoEqual(t, wantCallbackLink, gotCallbackLink)

	// Hop 4: the callback handler starts an operation of its own to process the completion and
	// answers async, naming the workflow backing it. An async start counts as delivered: the handler
	// accepted the completion, and the callback does not wait for it to finish.
	callbackHandlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: "callback-handler-workflow",
		RunId:      "callback-handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	env.respondNexusTaskCompleted(ctx, t, callbackTask.GetTaskToken(), &nexusTaskResponse{
		StartResult: &nexus.HandlerStartOperationResultAsync{OperationToken: "callback-operation-token"},
		Links:       []nexus.Link{commonnexus.ConvertLinkWorkflowEventToNexusLink(callbackHandlerLink)},
	})

	cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
	s.NotNil(cbInfo.GetSuccess())
	// Describe reports the same request ID the delivery was made under.
	s.Equal(callbackRequestID, cbInfo.GetRequestId())

	// The links the handler answered with are recorded on the callback, naming the resources it
	// created to process the completion. They belong to the callback rather than to the operation, so
	// they surface on the callback's info and not in the operation's own links.
	protorequire.ProtoEqual(t, &commonpb.Callback{
		Variant: workerCallback.GetVariant(),
		Links: []*commonpb.Link{
			{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: callbackHandlerLink}},
		},
	}, cbInfo.GetCallback())

	// Delivering the callback leaves the operation's own links as they were: a callback delivery is a
	// notification, so nothing about it is linked back onto the operation.
	descResp := env.describeNexusOperation(ctx, t, operationID)
	protorequire.ProtoSliceEqual(t, wantOperationLinks, descResp.GetInfo().GetLinks())
}

// TestLinkingE2E_MultipleWorkerCallbacks covers an operation carrying callbacks registered by more
// than one request: one on the start request, and two more attached to the running operation via
// on_conflict_options. Each callback is delivered under its own server-generated request ID, which is
// what a handler has to tell concurrent deliveries apart, and the links every request attached all
// land on the operation.
//
// The operation is left running asynchronously so that it is still open when the second request
// arrives, which is what makes the conflict path reachable at all. Its handler is an external Nexus
// server rather than a task queue, so the test can complete the operation itself through the callback
// the server handed that handler; the worker callbacks are still served by polling directly, since
// that is the only way to see the links a delivery carries.
func (s *NexusStandaloneCallbacksTestSuite) TestLinkingE2E_MultipleWorkerCallbacks() {
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnableCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
	)

	t := s.T()
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	// What the operation's handler was invoked with, including the callback the server generated for
	// it. Completing the operation later is what releases the completion callbacks.
	type handlerInvocation struct {
		links         []nexus.Link
		callbackURL   string
		callbackToken string
	}
	// Buffered and dropped on overflow so a redelivered invocation cannot block the handler.
	invocations := make(chan handlerInvocation, 2)

	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: "handler-workflow",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case invocations <- handlerInvocation{
				links:         options.Links,
				callbackURL:   options.CallbackURL,
				callbackToken: options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
			}:
			default:
			}
			nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink))
			// Leaving the operation running is what keeps it open for the second request below.
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "handler-operation-token"}, nil
		},
	})

	// All three callbacks share a task queue and are told apart by the operation they name, so the
	// test can serve them from a single poll loop no matter which order they are delivered in.
	callbackTaskQueue := "callback-tq-" + uuid.NewString()
	const (
		firstCallbackOperation    = "on-complete-from-first-request"
		secondCallbackOperation   = "on-complete-from-second-request"
		thirdCallbackOperation    = "on-complete-also-from-second-request"
		completionCallbackService = "completion-service"
	)
	workerCallback := func(operation string) *commonpb.Callback {
		return &commonpb.Callback{
			Variant: &commonpb.Callback_Worker_{
				Worker: &commonpb.Callback_Worker{
					TaskQueueName: callbackTaskQueue,
					Service:       completionCallbackService,
					Operation:     operation,
					// Distinct per callback, so a delivery that reached the wrong handler is visible.
					SourceContext: payload.EncodeString(operation + "-context"),
				},
			},
		}
	}

	operationID := testvars.New(t).Any().String()
	firstLink := standaloneNexusTestLink(env, "first-caller-wf")
	secondLink := standaloneNexusTestLink(env, "second-caller-wf")

	startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            endpointName,
		RequestId:           "first-request",
		CompletionCallbacks: []*commonpb.Callback{workerCallback(firstCallbackOperation)},
		Links:               []*commonpb.Link{firstLink},
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	// Wait until the handler has answered, so the operation is in a stable STARTED state before the
	// second request tries to attach to it.
	_, err = env.FrontendClient().PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: operationID,
		RunId:       startResp.GetRunId(),
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
	})
	s.NoError(err)

	var invocation handlerInvocation
	select {
	case invocation = <-invocations:
	case <-ctx.Done():
		s.FailNow("timed out waiting for the operation's handler to be invoked")
	}

	// The handler is only ever told about the operation that invoked it. Links the caller attached to
	// the start request are recorded on the operation, but are not forwarded.
	wantOperationBackLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
		Namespace:   env.Namespace().String(),
		OperationId: operationID,
		RunId:       startResp.GetRunId(),
	})
	s.Require().Len(invocation.links, 1)
	s.Equal(wantOperationBackLink.URL.String(), invocation.links[0].URL.String())
	s.Equal(wantOperationBackLink.Type, invocation.links[0].Type)

	// A second request for the same operation ID, which must attach to the running operation rather
	// than start another one, bringing two more callbacks and another link with it.
	attachResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: operationID,
		Endpoint:    endpointName,
		RequestId:   "second-request",
		CompletionCallbacks: []*commonpb.Callback{
			workerCallback(secondCallbackOperation),
			workerCallback(thirdCallbackOperation),
		},
		Links:            []*commonpb.Link{secondLink},
		IdReusePolicy:    enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
		IdConflictPolicy: enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		OnConflictOptions: &apinexusoperationpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
			AttachLinks:               true,
		},
	})
	s.NoError(err)
	s.False(attachResp.GetStarted(), "the second request must not have created an operation")
	s.Equal(startResp.GetRunId(), attachResp.GetRunId())

	// Every callback carries its own server-generated request ID, assigned when it was registered and
	// derived from the request that registered it. Describe is the source of truth the deliveries are
	// held to below.
	callbackRequestIDs := make(map[string]string, 3) // callback operation -> request ID
	registeredBy := map[string]string{               // callback operation -> registering request ID
		firstCallbackOperation:  "first-request",
		secondCallbackOperation: "second-request",
		thirdCallbackOperation:  "second-request",
	}
	for _, cb := range env.describeNexusOperation(ctx, t, operationID).GetCompletionCallbacks() {
		info := cb.GetInfo()
		operation := info.GetCallback().GetWorker().GetOperation()
		s.Equal(enumspb.CALLBACK_STATE_STANDBY, info.GetState(), "callback %q", operation)
		s.NotEmpty(info.GetRequestId(), "callback %q", operation)
		s.NotContains(callbackRequestIDs, operation, "callbacks must not be reported twice")
		s.Contains(info.GetRequestId(), registeredBy[operation],
			"callback %q must be identified by the request that registered it", operation)
		callbackRequestIDs[operation] = info.GetRequestId()
	}
	s.Len(callbackRequestIDs, 3)
	// Callbacks registered by one request are as distinguishable as callbacks registered by different
	// ones: no two share an ID, including the two the second request brought.
	distinctRequestIDs := make(map[string]struct{}, 3)
	for _, requestID := range callbackRequestIDs {
		distinctRequestIDs[requestID] = struct{}{}
	}
	s.Len(distinctRequestIDs, 3, "every callback must have a unique request ID: %v", callbackRequestIDs)

	// Completing the in-flight operation is what releases all three callbacks.
	operationResult := payload.EncodeString("operation-result")
	completionClient := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(completionClient.CompleteOperation(ctx, invocation.callbackURL, nexusrpc.CompleteOperationOptions{
		Result: operationResult,
		Header: nexus.Header{commonnexus.CallbackTokenHeader: invocation.callbackToken},
	}))

	// Both requests' links are recorded on the operation, alongside the one its handler answered with.
	// Links are stored per request ID, so their relative order is not guaranteed.
	wantOperationLinks := []*commonpb.Link{
		firstLink,
		secondLink,
		{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: handlerLink}},
	}
	await.Require(ctx, t, func(c *await.T) {
		descResp := env.describeNexusOperation(c.Context(), c, operationID)
		require.Equal(c, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		protorequire.ProtoEqual(c, operationResult, descResp.GetResult())
		protorequire.ProtoElementsMatch(c, wantOperationLinks, descResp.GetInfo().GetLinks())
	}, 10*time.Second, 100*time.Millisecond)

	// Serve all three deliveries, recording the request ID each one arrived under.
	callbackHandlerLink := func(operation string) *commonpb.Link_WorkflowEvent {
		return &commonpb.Link_WorkflowEvent{
			Namespace:  env.Namespace().String(),
			WorkflowId: operation + "-handler-workflow",
			RunId:      "callback-handler-run-id",
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		}
	}
	deliveredRequestIDs := make(map[string]string, len(callbackRequestIDs)) // callback operation -> request ID
	for range callbackRequestIDs {
		task := env.awaitNexusTask(ctx, t, callbackTaskQueue)
		start := task.GetRequest().GetStartOperation()
		operation := start.GetOperation()
		s.Equal(completionCallbackService, start.GetService())
		s.Contains(callbackRequestIDs, operation, "delivery for an unregistered callback")
		s.NotContains(deliveredRequestIDs, operation, "callback %q was delivered twice", operation)
		deliveredRequestIDs[operation] = start.GetRequestId()

		// Each delivery carries the operation's outcome and the context its own callback was
		// registered with.
		var onComplete notificationpb.OnCompleteRequest
		s.NoError(payload.Decode(start.GetPayload(), &onComplete))
		protorequire.ProtoEqual(t, operationResult, onComplete.GetSuccess())
		protorequire.ProtoEqual(t, payload.EncodeString(operation+"-context"), onComplete.GetSourceContext())

		// The link names the callback being delivered, not the operation, so the three deliveries
		// carry three different links even though they all report the same operation's outcome.
		wantCallbackLink := &commonpb.Link_Callback{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.Execution{
				Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
				BusinessId: operationID,
				RunId:      startResp.GetRunId(),
			},
			RequestId: start.GetRequestId(),
		}
		s.Require().Len(start.GetLinks(), 1)
		gotCallbackLink, convErr := commonnexus.ConvertNexusLinkToLinkCallback(
			commonnexus.ConvertLinksFromProto(start.GetLinks())[0])
		s.NoError(convErr)
		protorequire.ProtoEqual(t, wantCallbackLink, gotCallbackLink)

		env.respondNexusTaskCompleted(ctx, t, task.GetTaskToken(), &nexusTaskResponse{
			StartResult: &nexus.HandlerStartOperationResultSync[*commonpb.Payload]{Value: nil},
			Links: []nexus.Link{
				commonnexus.ConvertLinkWorkflowEventToNexusLink(callbackHandlerLink(operation)),
			},
		})
	}

	// Every callback was delivered under exactly the request ID Describe reported for it before the
	// deliveries went out.
	s.Equal(callbackRequestIDs, deliveredRequestIDs)

	// All three handlers accepted their delivery, so all three callbacks succeed, each recording the
	// link its own handler answered with.
	await.Require(ctx, t, func(c *await.T) {
		cbs := env.describeNexusOperation(c.Context(), c, operationID).GetCompletionCallbacks()
		require.Len(c, cbs, len(callbackRequestIDs))
		for _, cb := range cbs {
			info := cb.GetInfo()
			operation := info.GetCallback().GetWorker().GetOperation()
			require.Equal(c, enumspb.CALLBACK_STATE_SUCCEEDED, info.GetState(), "callback %q", operation)
			require.NotNil(c, info.GetSuccess(), "callback %q", operation)
			require.Equal(c, callbackRequestIDs[operation], info.GetRequestId())
			protorequire.ProtoSliceEqual(c, []*commonpb.Link{
				{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: callbackHandlerLink(operation)}},
			}, info.GetCallback().GetLinks())
		}
	}, 10*time.Second, 100*time.Millisecond)
}
