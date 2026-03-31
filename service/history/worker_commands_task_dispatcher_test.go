package history

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestDispatchResponseToError_SyncSuccess(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
						},
					},
				},
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestDispatchResponseToError_AsyncSuccess(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
							AsyncSuccess: &nexuspb.StartOperationResponse_Async{
								OperationId: "test-op-id",
							},
						},
					},
				},
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestDispatchResponseToError_RequestTimeout(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	assert.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
}

func TestDispatchResponseToError_DeprecatedHandlerError(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
			HandlerError: &nexuspb.HandlerError{
				ErrorType:     string(nexus.HandlerErrorTypeBadRequest),
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	assert.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, handlerErr.RetryBehavior)
}

func TestDispatchResponseToError_FailureWithHandlerErrorInfo(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "bad request from worker",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type:          string(nexus.HandlerErrorTypeBadRequest),
						RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
					},
				},
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	assert.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
}

func TestDispatchResponseToError_OperationErrorVariant(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_OperationError{
							OperationError: &nexuspb.UnsuccessfulOperationError{
								OperationState: string(nexus.OperationStateFailed),
								Failure: &nexuspb.Failure{
									Message: "operation failed",
								},
							},
						},
					},
				},
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var opErr *nexus.OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, nexus.OperationStateFailed, opErr.State)
}

func TestDispatchResponseToError_FailureVariant_OperationFailure(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_Failure{
							Failure: &failurepb.Failure{
								Message: "activity failed",
								FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
									ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
										Type: "SomeError",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var opErr *nexus.OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, nexus.OperationStateFailed, opErr.State)
}

func TestDispatchResponseToError_FailureVariant_CanceledFailure(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_Failure{
							Failure: &failurepb.Failure{
								Message: "canceled",
								FailureInfo: &failurepb.Failure_CanceledFailureInfo{
									CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
								},
							},
						},
					},
				},
			},
		},
	}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var opErr *nexus.OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, nexus.OperationStateCanceled, opErr.State)
}

func TestDispatchResponseToError_EmptyOutcome(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{}
	err := dispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}

func TestHandleError_OperationError_ReturnNil(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: handler,
		logger:         log.NewNoopLogger(),
	}

	opErr := &nexus.OperationError{
		State:   nexus.OperationStateFailed,
		Message: "worker bug",
	}
	err := d.handleError(opErr, "/temporal-sys/worker-commands/test-ns/key1")
	require.NoError(t, err, "operation errors are permanent and should be swallowed")

	snap := capture.Snapshot()
	assert.Len(t, snap[metrics.WorkerCommandsOperationFailure.Name()], 1)
}

func TestHandleError_UpstreamTimeout_ReturnRetryable(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: handler,
		logger:         log.NewNoopLogger(),
	}

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
	err := d.handleError(handlerErr, "/temporal-sys/worker-commands/test-ns/key1")
	require.Error(t, err, "upstream timeout should be retried")

	var he *nexus.HandlerError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, he.Type)

	snap := capture.Snapshot()
	assert.Len(t, snap[metrics.WorkerCommandsDispatchNoPoller.Name()], 1)
}

func TestHandleError_OtherHandlerError_ReturnRetryable(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: handler,
		logger:         log.NewNoopLogger(),
	}

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "something broke")
	err := d.handleError(handlerErr, "/temporal-sys/worker-commands/test-ns/key1")
	require.Error(t, err, "transport errors should be retried")

	snap := capture.Snapshot()
	assert.Len(t, snap[metrics.WorkerCommandsDispatchFailure.Name()], 1)
}
