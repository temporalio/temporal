package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/tests/testcore"
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

func (env *NexusTestEnv) createNexusEndpoint(t *testing.T, name string, taskQueue string) *nexuspb.Endpoint {
	resp, err := env.OperatorClient().CreateNexusEndpoint(testcore.NewContext(), &operatorservice.CreateNexusEndpointRequest{
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

	t.Cleanup(func() {
		// Delete the endpoint so the cluster can be safely reused by subsequent tests.
		_, _ = env.OperatorClient().DeleteNexusEndpoint(testcore.NewContext(), &operatorservice.DeleteNexusEndpointRequest{
			Id:      resp.Endpoint.Id,
			Version: resp.Endpoint.Version,
		})
	})

	// Wait for the endpoint to be visible to StartNexusOperationExecution.
	require.Eventually(t, func() bool {
		_, err := env.FrontendClient().StartNexusOperationExecution(testcore.NewContext(), &workflowservice.StartNexusOperationExecutionRequest{
			Namespace: env.Namespace().String(),
			Endpoint:  name,
			Service:   "probe",
			Operation: "probe",
			RequestId: "probe",
		})
		if notFound, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			msg := notFound.Error()
			return msg != "endpoint not registered" && !strings.HasPrefix(msg, "could not find Nexus endpoint by name:")
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "endpoint should become visible")
	return resp.Endpoint
}

func (env *NexusTestEnv) createRandomNexusEndpoint(t *testing.T) *nexuspb.Endpoint {
	return env.createNexusEndpoint(t, testcore.RandomizedNexusEndpoint(t.Name()), "unused")
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
