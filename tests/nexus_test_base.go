package tests

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/tests/testcore"
)

type NexusTestBaseSuite struct {
	testcore.FunctionalTestBase
	useTemporalFailures bool
}

func (s *NexusTestBaseSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}

func (s *NexusTestBaseSuite) createNexusEndpoint(name string, taskQueue string) *nexuspb.Endpoint {
	resp, err := s.OperatorClient().CreateNexusEndpoint(testcore.NewContext(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)
	return resp.Endpoint
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

type nexusTaskHandler func(res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error)

func (s *NexusTestBaseSuite) nexusTaskPoller(ctx context.Context, taskQueue string, handler nexusTaskHandler) <-chan error {
	return s.versionedNexusTaskPoller(ctx, taskQueue, "", handler)
}

func (s *NexusTestBaseSuite) versionedNexusTaskPoller(ctx context.Context, taskQueue, buildID string, handler nexusTaskHandler) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.versionedNexusTaskPollerDo(ctx, taskQueue, buildID, handler)
	}()
	return errCh
}

func (s *NexusTestBaseSuite) versionedNexusTaskPollerDo(ctx context.Context, taskQueue, buildID string, handler nexusTaskHandler) error {
	var vc *commonpb.WorkerVersionCapabilities
	if buildID != "" {
		vc = &commonpb.WorkerVersionCapabilities{
			BuildId:       buildID,
			UseVersioning: true,
		}
	}
	res, err := s.GetTestCluster().FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.Namespace().String(),
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
	result, handlerErr := handler(res)
	if handlerErr != nil {
		var opErr *nexus.OperationError
		var he *nexus.HandlerError
		if errors.As(handlerErr, &opErr) {
			return s.respondNexusTaskCompletedWithOperationError(ctx, res.TaskToken, opErr)
		} else if errors.As(handlerErr, &he) {
			return s.respondNexusTaskFailed(ctx, res.TaskToken, he)
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
	_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: res.TaskToken,
		Response:  response,
	})
	if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
		return err
	}
	return nil
}

func (s *NexusTestBaseSuite) respondNexusTaskFailed(ctx context.Context, taskToken []byte, he *nexus.HandlerError) error {
	if s.useTemporalFailures {
		nexusFailure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(he)
		if err != nil {
			return err
		}
		temporalFailure, err := cnexus.NexusFailureToTemporalFailure(nexusFailure)
		if err != nil {
			return err
		}
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: taskToken,
			Failure:   temporalFailure,
		})
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
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
	_, err := s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: taskToken,
		Error:     protoError,
	})
	if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
		return err
	}
	return nil
}

func (s *NexusTestBaseSuite) respondNexusTaskCompletedWithOperationError(ctx context.Context, taskToken []byte, opErr *nexus.OperationError) error {
	if s.useTemporalFailures {
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
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: taskToken,
			Response:  response,
		})
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
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
	_, err := s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: taskToken,
		Response:  response,
	})
	if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
		return err
	}
	return nil
}
