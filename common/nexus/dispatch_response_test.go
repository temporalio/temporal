package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestMatchingDispatchResponseToError_SyncSuccess(t *testing.T) {
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
	err := MatchingDispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestMatchingDispatchResponseToError_AsyncSuccess(t *testing.T) {
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
	err := MatchingDispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestMatchingDispatchResponseToError_RequestTimeout(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
}

func TestMatchingDispatchResponseToError_WorkerFailure(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "bad request from worker",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						Type: "SomeError",
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "SomeError", appErr.Type())
}

func TestMatchingDispatchResponseToError_OperationFailure_ApplicationError(t *testing.T) {
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
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "SomeError", appErr.Type())
}

func TestMatchingDispatchResponseToError_OperationFailure_CanceledError(t *testing.T) {
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
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var cancelErr *temporal.CanceledError
	require.ErrorAs(t, err, &cancelErr)
}

func TestMatchingDispatchResponseToError_EmptyOutcome(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}

func TestStartOperationResponseToError_EmptyVariant(t *testing.T) {
	resp := &nexuspb.StartOperationResponse{}
	err := StartOperationResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}
