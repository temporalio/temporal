package tests

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	apinexusoperationpb "go.temporal.io/api/nexusoperation/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

// newCallbackTestEnv builds an env with standalone Nexus operations and their completion callbacks
// enabled, and with the callback address allowlist opened up so that a local httptest server is a
// valid callback target.
func (s *NexusStandaloneTestSuite) newCallbackTestEnv() *NexusTestEnv {
	env := s.newTestEnv(testcore.WithDynamicConfig(nexusoperation.EnableNexusCallbacks, true))
	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)
	return env
}

// runNexusCompletionHTTPServer stands up a Nexus completion endpoint and returns its URL, for use as
// the target of a completion callback.
func (s *NexusStandaloneTestSuite) runNexusCompletionHTTPServer(t *testing.T, h *completionHandler) string {
	srv := httptest.NewServer(nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: h}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func newCompletionHandler(t *testing.T) *completionHandler {
	h := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	t.Cleanup(func() {
		close(h.requestCh)
		close(h.requestCompleteCh)
	})
	return h
}

// awaitCompletion returns the next completion delivered to the handler, acknowledging it before
// returning. Assertions must run against the returned value rather than inside the handler: leaving
// the handler blocked stalls the server's Close and makes the callback look like a delivery timeout,
// which turns any assertion failure into a test timeout instead of a readable failure.
func (s *NexusStandaloneTestSuite) awaitCompletion(h *completionHandler) *nexusrpc.CompletionRequest {
	s.T().Helper()

	select {
	case completion := <-h.requestCh:
		h.requestCompleteCh <- nil
		return completion
	case <-s.Context().Done():
		s.FailNow("timed out waiting for the completion callback")
		return nil
	}
}

func nexusCompletionCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}},
	}
}

// awaitCallbackInfos polls DescribeNexusOperationExecution until it reports wantCount callbacks all in
// wantState, and returns them.
func (s *NexusStandaloneTestSuite) awaitCallbackInfos(
	env *NexusTestEnv,
	operationID string,
	wantCount int,
	wantState enumspb.CallbackState,
) []*apinexusoperationpb.CallbackInfo {
	s.T().Helper()

	var infos []*apinexusoperationpb.CallbackInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
		})
		require.NoError(t, err)
		infos = descResp.GetCompletionCallbacks()
		require.Len(t, infos, wantCount)
		for _, info := range infos {
			require.Equal(t, wantState, info.GetInfo().GetState())
		}
	}, 15*time.Second, 100*time.Millisecond)
	return infos
}

// TestStandaloneNexusOperationCallbacks covers completion callbacks attached to a standalone Nexus
// operation: that they are delivered when the operation closes, that they are reported by describe,
// and that unsupported requests are rejected.
func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationCallbacks() {
	s.Run("DeliveredOnSuccess", func(s *NexusStandaloneTestSuite) {
		env := s.newCallbackTestEnv()
		t := s.T()

		// The handler completes the operation synchronously, so it closes as soon as it is dispatched.
		endpointName := env.createRandomExternalNexusServer(s.Context(), t, nexustest.Handler{
			OnStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultSync[any]{Value: "operation-result"}, nil
			},
		})

		handler := newCompletionHandler(t)
		callbackURL := s.runNexusCompletionHTTPServer(t, handler)

		operationID := testvars.New(t).Any().String()
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback(callbackURL)},
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		completion := s.awaitCompletion(handler)
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		s.Nil(completion.Error)
		s.False(completion.StartTime.IsZero())
		s.False(completion.CloseTime.IsZero())
		// The completion carries a back-link to the operation that produced it.
		wantLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       startResp.GetRunId(),
		})
		s.Len(completion.Links, 1)
		if len(completion.Links) == 1 {
			s.Equal(wantLink.URL.String(), completion.Links[0].URL.String())
			s.Equal(wantLink.Type, completion.Links[0].Type)
		}

		infos := s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
		s.Equal(callbackURL, infos[0].GetInfo().GetCallback().GetNexus().GetUrl())
		s.NotNil(infos[0].GetInfo().GetOutcome().GetSuccess())
		s.NotNil(infos[0].GetTrigger().GetOperationCompleted())
	})

	s.Run("DeliveredOnFailure", func(s *NexusStandaloneTestSuite) {
		env := s.newCallbackTestEnv()
		t := s.T()

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

		handler := newCompletionHandler(t)
		callbackURL := s.runNexusCompletionHTTPServer(t, handler)

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         operationID,
			Endpoint:            endpointName,
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback(callbackURL)},
		})
		s.NoError(err)

		completion := s.awaitCompletion(handler)
		s.Equal(nexus.OperationStateFailed, completion.State)
		s.Require().NotNil(completion.Error)
		var failureErr *nexus.FailureError
		s.Require().ErrorAs(completion.Error.Cause, &failureErr)
		// The handler's error is wrapped as an OperationError whose cause carries the original
		// message, which is how a Nexus operation failure round-trips through a completion.
		tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(failureErr.Failure)
		s.NoError(convErr)
		s.Equal("OperationError", tFailure.GetApplicationFailureInfo().GetType())
		s.Equal("deliberate failure", tFailure.GetCause().GetMessage())

		s.awaitCallbackInfos(env, operationID, 1, enumspb.CALLBACK_STATE_SUCCEEDED)
	})

	s.Run("DescribeReportsStandbyBeforeClose", func(s *NexusStandaloneTestSuite) {
		env := s.newCallbackTestEnv()
		t := s.T()

		// The operation stays STARTED, so its callbacks stay in STANDBY.
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

		operationID := testvars.New(t).Any().String()
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: operationID,
			Endpoint:    endpointName,
			CompletionCallbacks: []*commonpb.Callback{
				nexusCompletionCallback("http://localhost/cb1"),
				nexusCompletionCallback("http://localhost/cb2"),
			},
		})
		s.NoError(err)

		infos := s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_STANDBY)
		for _, info := range infos {
			s.Nil(info.GetInfo().GetOutcome())
			s.NotNil(info.GetInfo().GetRegistrationTime())
		}
	})

	s.Run("AttachOnConflict", func(s *NexusStandaloneTestSuite) {
		env := s.newCallbackTestEnv()
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

		operationID := testvars.New(t).Any().String()
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
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
			OnConflictOptions: &apinexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		}
		attachResp, err := s.startNexusOperation(env, attachReq)
		s.NoError(err)
		s.False(attachResp.GetStarted(), "the second request must not have created an operation")
		s.Equal(startResp.GetRunId(), attachResp.GetRunId())

		infos := s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_STANDBY)
		s.Equal("http://localhost/cb1", infos[0].GetInfo().GetCallback().GetNexus().GetUrl())
		s.Equal("http://localhost/cb2", infos[1].GetInfo().GetCallback().GetNexus().GetUrl())

		// Replaying the same attach must not duplicate the callback it already attached.
		_, err = s.startNexusOperation(env, attachReq)
		s.NoError(err)
		s.awaitCallbackInfos(env, operationID, 2, enumspb.CALLBACK_STATE_STANDBY)
	})

	s.Run("RejectsUnsupportedVariants", func(s *NexusStandaloneTestSuite) {
		env := s.newCallbackTestEnv()
		t := s.T()

		endpointName := env.createRandomExternalNexusServer(s.Context(), t, nexustest.Handler{})

		for name, cb := range map[string]*commonpb.Callback{
			// Worker callbacks are a newer API addition that standalone Nexus operations, like
			// standalone activities, do not support yet.
			"worker": {Variant: &commonpb.Callback_Worker_{Worker: &commonpb.Callback_Worker{
				TaskQueueName: "completions-task-queue",
				Service:       "HTTPAdapter",
				Operation:     "DeliverAsWebhook",
			}}},
			"internal": {Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{
				Data: []byte("data"),
			}}},
			"unset": {},
		} {
			s.Run(name, func(s *NexusStandaloneTestSuite) {
				_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
					OperationId:         testvars.New(s.T()).Any().String(),
					Endpoint:            endpointName,
					CompletionCallbacks: []*commonpb.Callback{cb},
				})
				var invalidArgErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidArgErr)
				s.ErrorContains(err, "unsupported callback variant")
			})
		}
	})

	s.Run("RejectsAttachLinks", func(s *NexusStandaloneTestSuite) {
		env := s.newCallbackTestEnv()

		// The start request carries no links, so attach_links cannot be honored.
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{}),
			CompletionCallbacks: []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb")},
			OnConflictOptions: &apinexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			},
		})
		var unimplementedErr *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplementedErr)
		s.ErrorContains(err, "attach_links is not supported")
	})
}

// TestStandaloneNexusOperationCallbacksDisabled confirms the per-namespace feature flag gates the
// whole surface, including the on-conflict attach path.
func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationCallbacksDisabled() {
	// newTestEnv leaves nexusoperation.EnableNexusCallbacks at its default of false.
	env := s.newTestEnv()
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{})
	cbs := []*commonpb.Callback{nexusCompletionCallback("http://localhost/cb")}

	s.Run("StartWithCallbacksFails", func(s *NexusStandaloneTestSuite) {
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: cbs,
		})
		s.ErrorContains(err, "completion callbacks are not enabled for this namespace")
	})

	s.Run("OnConflictAttachCallbacksFails", func(s *NexusStandaloneTestSuite) {
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:         testvars.New(s.T()).Any().String(),
			Endpoint:            endpointName,
			CompletionCallbacks: cbs,
			IdConflictPolicy:    enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions: &apinexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		})
		s.ErrorContains(err, "completion callbacks are not enabled for this namespace")
	})
}
