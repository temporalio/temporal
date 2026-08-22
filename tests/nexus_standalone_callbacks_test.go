package tests

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexusoperationpb "go.temporal.io/api/nexusoperation/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
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
