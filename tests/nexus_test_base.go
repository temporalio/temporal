package tests

import (
	"cmp"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusTestEnv struct {
	*testcore.TestEnv
	useTemporalFailures bool
}

func newNexusTestEnv(t *testing.T, useTemporalFailures bool, opts ...testcore.TestOption) *NexusTestEnv {
	return &NexusTestEnv{
		TestEnv:             testcore.NewEnv(t, opts...),
		useTemporalFailures: useTemporalFailures,
	}
}

// startNexusOperation starts a standalone Nexus operation, applying defaults for
// required fields tests usually don't care about.
func (env *NexusTestEnv) startNexusOperation(
	ctx context.Context,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	req.Namespace = cmp.Or(req.Namespace, env.Namespace().String())
	req.Service = cmp.Or(req.Service, "test-service")
	req.Operation = cmp.Or(req.Operation, "test-operation")
	req.RequestId = cmp.Or(req.RequestId, env.Tv().RequestID())
	if req.ScheduleToCloseTimeout == nil {
		req.ScheduleToCloseTimeout = durationpb.New(10 * time.Minute)
	}

	return env.FrontendClient().StartNexusOperationExecution(ctx, req)
}

// describeNexusOperation describes a standalone Nexus operation by ID, including its outcome.
func (env *NexusTestEnv) describeNexusOperation(
	ctx context.Context,
	t require.TestingT,
	operationID string,
) *workflowservice.DescribeNexusOperationExecutionResponse {
	descResp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
		Namespace:      env.Namespace().String(),
		OperationId:    operationID,
		IncludeOutcome: true,
	})
	require.NoError(t, err)
	return descResp
}

func (env *NexusTestEnv) createNexusEndpoint(ctx context.Context, t *testing.T, name string, taskQueue string) *nexuspb.Endpoint {
	resp, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Using a fresh context here in case 'ctx' is tied to a test's lifetime which could cancel this deletion request.
	t.Cleanup(func() {
		_, _ = env.OperatorClient().DeleteNexusEndpoint(testcore.NewContext(), &operatorservice.DeleteNexusEndpointRequest{
			Id:      resp.Endpoint.Id,
			Version: resp.Endpoint.Version,
		})
	})

	return resp.Endpoint
}

func (env *NexusTestEnv) createRandomNexusEndpoint(ctx context.Context, t *testing.T) *nexuspb.Endpoint {
	return env.createNexusEndpoint(ctx, t, testcore.RandomizedNexusEndpoint(t.Name()), "unused")
}

// createRandomExternalNexusServer creates a mock nexus server that listens via a randomized endpointName and return this name to the caller.
func (env *NexusTestEnv) createRandomExternalNexusServer(ctx context.Context, t *testing.T, handler nexustest.Handler) string {
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(t, listenAddr, handler)
	return env.createExternalNexusEndpoint(ctx, t, "http://"+listenAddr)
}

func (env *NexusTestEnv) createExternalNexusEndpoint(ctx context.Context, t *testing.T, url string) string {
	endpointName := testcore.RandomizedNexusEndpoint(t.Name())
	resp, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: url,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Using a fresh context here in case 'ctx' is tied to a test's lifetime which could cancel this deletion request.
	t.Cleanup(func() {
		_, _ = env.OperatorClient().DeleteNexusEndpoint(testcore.NewContext(), &operatorservice.DeleteNexusEndpointRequest{
			Id:      resp.Endpoint.Id,
			Version: resp.Endpoint.Version,
		})
	})

	return endpointName
}

func (env *NexusTestEnv) dispatchByEndpointURL(endpoint string) string {
	return "http://" + env.HttpAPIAddress() + "/" + cnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint)
}

func (env *NexusTestEnv) dispatchByTaskQueueURL(taskQueue string) string {
	return env.dispatchByNamespaceAndTaskQueueURL(env.Namespace().String(), taskQueue)
}

func (env *NexusTestEnv) dispatchByNamespaceAndTaskQueueURL(namespace string, taskQueue string) string {
	return "http://" + env.HttpAPIAddress() + "/" + cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.
		Path(cnexus.NamespaceAndTaskQueue{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
}

// createSyncSuccessEndpoint registers an endpoint whose handler completes every operation
// synchronously with the supplied result payload. Shutdown as part with the test's Cleanup.
func (env *NexusTestEnv) createSyncSuccessEndpoint(ctx context.Context, t *testing.T, result string) string {
	return env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: result}, nil
		},
	})
}

// createSyncFailureEndpoint registers an endpoint whose handler fails every operation
// synchronously with the supplied message. Shutdown as part with the test's Cleanup.
func (env *NexusTestEnv) createSyncFailureEndpoint(ctx context.Context, t *testing.T, message string) string {
	return env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			return nil, &nexus.OperationError{
				State: nexus.OperationStateFailed,
				Cause: &nexus.FailureError{Failure: nexus.Failure{Message: message}},
			}
		},
	})
}

// awaitCallbackInfo polls DescribeNexusOperationExecution until the operation's single completion
// callback reaches wantState, then returns it.
func (env *NexusTestEnv) awaitCallbackInfo(
	ctx context.Context,
	t testing.TB,
	operationID string,
	wantState enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	t.Helper()

	var cbInfo *callbackpb.CallbackInfo
	await.Require(ctx, t, func(c *await.T) {
		cbs := env.describeNexusOperation(c.Context(), c, operationID).GetCompletionCallbacks()
		require.Len(c, cbs, 1)
		cbInfo = cbs[0].GetInfo()
		require.NotNil(c, cbInfo)
		require.Equal(c, wantState, cbInfo.GetState())
	}, 10*time.Second, 100*time.Millisecond)
	return cbInfo
}

// createAsyncEndpoint registers an endpoint whose handler leaves every operation running async,
// so calls to the endpoint remain in the STARTED state until it is completed by other means.
// (e.g. the Nexus operation gets canceled or terminated.)
func (env *NexusTestEnv) createAsyncEndpoint(ctx context.Context, t *testing.T) string {
	return env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-operation-token"}, nil
		},
	})
}

// nexusTaskResponse represents a successful response from a nexus task handler.
// A nil response indicates no response should be sent (e.g., handler timed out).
type nexusTaskResponse struct {
	// StartResult, if set, indicates a start operation response.
	// Use HandlerStartOperationResultSync for sync success or
	// HandlerStartOperationResultAsync for async success.
	// If nil, the response is a cancel operation acknowledgement.
	StartResult  nexus.HandlerStartOperationResult[*commonpb.Payload]
	CancelResult *struct{}
	// Links to include in async start operation responses.
	Links []nexus.Link
}

type nexusTaskHandler func(t *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error)

func (env *NexusTestEnv) nexusTaskPoller(ctx context.Context, t *testing.T, taskQueue string, handler nexusTaskHandler) <-chan error {
	return env.versionedNexusTaskPoller(ctx, t, taskQueue, "", handler)
}

func (env *NexusTestEnv) versionedNexusTaskPoller(ctx context.Context, t *testing.T, taskQueue, buildID string, handler nexusTaskHandler) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- env.versionedNexusTaskPollerDo(ctx, t, taskQueue, buildID, handler)
	}()
	return errCh
}

func (env *NexusTestEnv) versionedNexusTaskPollerDo(ctx context.Context, t *testing.T, taskQueue, buildID string, handler nexusTaskHandler) error {
	var vc *commonpb.WorkerVersionCapabilities
	if buildID != "" {
		vc = &commonpb.WorkerVersionCapabilities{
			BuildId:       buildID,
			UseVersioning: true,
		}
	}
	res, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: env.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		WorkerVersionCapabilities: vc,
	})
	// The test is written in a way that it doesn't expect the poll to be unblocked and it may cancel this context when it completes.
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	if res.TaskToken == nil {
		return nil
	}
	if res.Request.GetStartOperation().GetService() != "test-service" && res.Request.GetCancelOperation().GetService() != "test-service" {
		return errors.New("expected service to be test-service")
	}
	result, handlerErr := handler(t, res)
	if handlerErr != nil {
		if opErr, ok := errors.AsType[*nexus.OperationError](handlerErr); ok {
			return env.respondNexusTaskCompletedWithOperationError(ctx, res.TaskToken, opErr)
		} else if he, ok := errors.AsType[*nexus.HandlerError](handlerErr); ok {
			return env.respondNexusTaskFailed(ctx, res.TaskToken, he)
		}
		return handlerErr
	}
	if result == nil {
		return nil
	}
	var response *nexuspb.Response
	if result.CancelResult != nil {
		response = &nexuspb.Response{
			Variant: &nexuspb.Response_CancelOperation{
				CancelOperation: &nexuspb.CancelOperationResponse{},
			},
		}
	} else {
		switch r := result.StartResult.(type) {
		case *nexus.HandlerStartOperationResultSync[*commonpb.Payload]:
			syncResp := &nexuspb.StartOperationResponse_Sync{
				Payload: r.Value,
			}
			for _, l := range result.Links {
				syncResp.Links = append(syncResp.Links, &nexuspb.Link{
					Url:  l.URL.String(),
					Type: l.Type,
				})
			}
			response = &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: syncResp,
						},
					},
				},
			}
		case *nexus.HandlerStartOperationResultAsync:
			asyncResp := &nexuspb.StartOperationResponse_Async{
				OperationToken: r.OperationToken,
			}
			for _, l := range result.Links {
				asyncResp.Links = append(asyncResp.Links, &nexuspb.Link{
					Url:  l.URL.String(),
					Type: l.Type,
				})
			}
			response = &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
							AsyncSuccess: asyncResp,
						},
					},
				},
			}
		default:
			panic("unreachable") // nolint:revive // all implementations of HandlerStartOperationResult must be covered here, so this should be unreachable.
		}
	}
	_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: res.TaskToken,
		Response:  response,
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); err != nil && ctx.Err() == nil && !ok {
		return err
	}
	return nil
}

func (env *NexusTestEnv) respondNexusTaskFailed(ctx context.Context, taskToken []byte, he *nexus.HandlerError) error {
	if env.useTemporalFailures {
		nexusFailure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(he)
		if err != nil {
			return err
		}
		temporalFailure, err := cnexus.NexusFailureToTemporalFailure(nexusFailure)
		if err != nil {
			return err
		}
		_, err = env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: env.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: taskToken,
			Failure:   temporalFailure,
		})
		if _, ok := errors.AsType[*serviceerror.NotFound](err); err != nil && ctx.Err() == nil && !ok {
			return err
		}
		return nil
	}
	// Legacy path: convert handler error to proto HandlerError.
	var protoFailure *nexuspb.Failure
	if he.Cause != nil {
		causeFailure, convertErr := nexusrpc.DefaultFailureConverter().ErrorToFailure(he.Cause)
		if convertErr != nil {
			return convertErr
		}
		protoFailure = cnexus.NexusFailureToProtoFailure(causeFailure)
	} else {
		protoFailure = &nexuspb.Failure{Message: he.Message}
	}
	protoError := &nexuspb.HandlerError{
		ErrorType: string(he.Type),
		Failure:   protoFailure,
	}
	// nolint:exhaustive // only two valid values other than unspecified.
	switch he.RetryBehavior {
	case nexus.HandlerErrorRetryBehaviorRetryable:
		protoError.RetryBehavior = enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
	case nexus.HandlerErrorRetryBehaviorNonRetryable:
		protoError.RetryBehavior = enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
	default:
	}
	_, err := env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: env.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: taskToken,
		Error:     protoError,
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); err != nil && ctx.Err() == nil && !ok {
		return err
	}
	return nil
}

func (env *NexusTestEnv) respondNexusTaskCompletedWithOperationError(ctx context.Context, taskToken []byte, opErr *nexus.OperationError) error {
	if env.useTemporalFailures {
		nexusFailure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(opErr)
		if err != nil {
			return err
		}
		temporalFailure, err := cnexus.NexusFailureToTemporalFailure(nexusFailure)
		if err != nil {
			return err
		}
		response := &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_Failure{
						Failure: temporalFailure,
					},
				},
			},
		}
		_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: taskToken,
			Response:  response,
		})
		if _, ok := errors.AsType[*serviceerror.NotFound](err); err != nil && ctx.Err() == nil && !ok {
			return err
		}
		return nil
	}
	// Legacy path: convert operation error to proto UnsuccessfulOperationError.
	var protoFailure *nexuspb.Failure
	if opErr.Cause != nil {
		causeFailure, convertErr := nexusrpc.DefaultFailureConverter().ErrorToFailure(opErr.Cause)
		if convertErr != nil {
			return convertErr
		}
		protoFailure = cnexus.NexusFailureToProtoFailure(causeFailure)
	} else {
		protoFailure = &nexuspb.Failure{Message: opErr.Message}
	}
	response := &nexuspb.Response{
		Variant: &nexuspb.Response_StartOperation{
			StartOperation: &nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_OperationError{
					OperationError: &nexuspb.UnsuccessfulOperationError{
						OperationState: string(opErr.State),
						Failure:        protoFailure,
					},
				},
			},
		},
	}
	_, err := env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: taskToken,
		Response:  response,
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); err != nil && ctx.Err() == nil && !ok {
		return err
	}
	return nil
}

// completionHandler is a nexusrpc completion handler that hands each delivered completion to the
// test on requestCh, then waits on requestCompleteCh for the error to return to the caller.
type completionHandler struct {
	requestCh         chan *nexusrpc.CompletionRequest
	requestCompleteCh chan error
	doneCh            chan struct{}
}

func (h *completionHandler) CompleteOperation(ctx context.Context, request *nexusrpc.CompletionRequest) error {
	// Push the request to the requests channel.
	select {
	case h.requestCh <- request:
	case <-h.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

	// Pull from the rsponse channel.
	select {
	case err := <-h.requestCompleteCh:
		return err
	case <-h.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// newNexusCompletionHandler returns a completion handler along with the URL of an HTTP server that
// delivers completions to it, for use as the target of a completion callback. The server shuts
// down when t cleans up.
func newNexusCompletionHandler(t *testing.T) (*completionHandler, string) {
	// Buffered so the server can deliver several completions (or retries) before the test drains them.
	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 4),
		requestCompleteCh: make(chan error, 4),
		doneCh:            make(chan struct{}),
	}

	httpHandler := nexusrpc.CompletionHandlerOptions{Handler: ch}
	srv := httptest.NewServer(nexusrpc.NewCompletionHTTPHandler(httpHandler))

	t.Cleanup(func() {
		// Unblock any calls to CompleteOperation; srv.Close waits for in-flight requests.
		close(ch.doneCh)
		srv.Close()
	})
	return ch, srv.URL
}

// nexusCompletionCallback builds a Nexus-variant completion callback targeting url, typically the URL
// returned by [newNexusCompletionHandler].
func nexusCompletionCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}},
	}
}
