package workercommands

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func testCommands() []*workerpb.WorkerCommand {
	return []*workerpb.WorkerCommand{
		{Type: &workerpb.WorkerCommand_CancelActivity{
			CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("token1")},
		}},
	}
}

func requireMetric(t *testing.T, snap map[string][]*metricstest.CapturedRecording, expectedOutcome string) {
	t.Helper()
	recordings := snap[metrics.WorkerCommandsSent.Name()]
	require.Len(t, recordings, 1, "expected exactly 1 metric recording")
	require.Equal(t, expectedOutcome, recordings[0].Tags["outcome"])
}

// ---- DispatchToWorker tests ----

func TestDispatchToWorker_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	var capturedReq *matchingservice.DispatchNexusTaskRequest
	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *matchingservice.DispatchNexusTaskRequest, _ ...any) (*matchingservice.DispatchNexusTaskResponse, error) {
			capturedReq = req
			return syncSuccessResponse(), nil
		})

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", testCommands(),
	)
	require.NoError(t, err)
	requireMetric(t, capture.Snapshot(), "success")

	// Verify request shape.
	require.NotNil(t, capturedReq)
	require.Equal(t, "test-ns-id", capturedReq.NamespaceId)
	require.Equal(t, "test-control-queue", capturedReq.TaskQueue.Name)
	require.Equal(t, enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS, capturedReq.TaskQueue.Kind)

	// Verify Nexus request contents.
	startOp := capturedReq.Request.GetStartOperation()
	require.NotNil(t, startOp)
	require.Equal(t, ServiceName, startOp.Service)
	require.Equal(t, OperationName, startOp.Operation)

	// Verify payload is binary/protobuf-encoded ExecuteCommandsRequest.
	require.Equal(t, "binary/protobuf", string(startOp.Payload.Metadata["encoding"]))
	var decoded workerservicepb.ExecuteCommandsRequest
	require.NoError(t, proto.Unmarshal(startOp.Payload.Data, &decoded))
	require.Len(t, decoded.Commands, 1)
	require.NotNil(t, decoded.Commands[0].GetCancelActivity())
	require.Equal(t, []byte("token1"), decoded.Commands[0].GetCancelActivity().TaskToken)
}

func TestDispatchToWorker_RPCError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("connection refused"))

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", testCommands(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
	requireMetric(t, capture.Snapshot(), "rpc_error")
}

func TestDispatchToWorker_UpstreamTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		&matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
				RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
			},
		}, nil)

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", testCommands(),
	)
	require.Error(t, err)

	var he *nexus.HandlerError
	require.ErrorAs(t, err, &he)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, he.Type)
	requireMetric(t, capture.Snapshot(), "no_poller")
}

func TestDispatchToWorker_WorkerFailure_SwallowedAsPermanent(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		workerFailureResponse("worker bug"), nil)

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", testCommands(),
	)
	require.NoError(t, err, "worker-returned failures are permanent and should be swallowed")
	requireMetric(t, capture.Snapshot(), "worker_error")
}

func TestDispatchToWorker_NonRetryableHandlerError_SwallowedAsPermanent(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		handlerErrorResponse(nexus.HandlerErrorTypeBadRequest, "bad request"), nil)

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", testCommands(),
	)
	require.NoError(t, err, "non-retryable handler errors should be swallowed")
	requireMetric(t, capture.Snapshot(), "non_retryable_error")
}

func TestDispatchToWorker_RetryableTransportError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		handlerErrorResponse(nexus.HandlerErrorTypeInternal, "something broke"), nil)

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", testCommands(),
	)
	require.Error(t, err, "internal handler errors are retryable")
	requireMetric(t, capture.Snapshot(), "transport_error")
}

func TestDispatchToWorker_MultipleCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	var capturedReq *matchingservice.DispatchNexusTaskRequest
	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *matchingservice.DispatchNexusTaskRequest, _ ...any) (*matchingservice.DispatchNexusTaskResponse, error) {
			capturedReq = req
			return syncSuccessResponse(), nil
		})

	commands := []*workerpb.WorkerCommand{
		{Type: &workerpb.WorkerCommand_CancelActivity{
			CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("token1")},
		}},
		{Type: &workerpb.WorkerCommand_CancelActivity{
			CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("token2")},
		}},
	}

	err := DispatchToWorker(
		context.Background(), mockClient, metricsHandler, log.NewNoopLogger(),
		"test-ns-id", "test-control-queue", commands,
	)
	require.NoError(t, err)

	// Verify both commands are in the payload.
	startOp := capturedReq.Request.GetStartOperation()
	var decoded workerservicepb.ExecuteCommandsRequest
	require.NoError(t, proto.Unmarshal(startOp.Payload.Data, &decoded))
	require.Len(t, decoded.Commands, 2)
	require.Equal(t, []byte("token1"), decoded.Commands[0].GetCancelActivity().TaskToken)
	require.Equal(t, []byte("token2"), decoded.Commands[1].GetCancelActivity().TaskToken)
}

// ---- HandleDispatchError tests ----

func TestHandleDispatchError_WorkerError_ReturnsNil(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	workerErr := temporal.NewApplicationError("worker bug", "SomeType", nil)
	err := HandleDispatchError(workerErr, "test-control-queue", metricsHandler, log.NewNoopLogger())
	require.NoError(t, err, "worker-returned errors are permanent and should be swallowed")
	requireMetric(t, capture.Snapshot(), "worker_error")
}

func TestHandleDispatchError_UpstreamTimeout_ReturnsError(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
	err := HandleDispatchError(handlerErr, "test-control-queue", metricsHandler, log.NewNoopLogger())
	require.Error(t, err, "upstream timeout should be retried")

	var he *nexus.HandlerError
	require.ErrorAs(t, err, &he)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, he.Type)
	requireMetric(t, capture.Snapshot(), "no_poller")
}

func TestHandleDispatchError_NonRetryableHandlerError_ReturnsNil(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad request")
	err := HandleDispatchError(handlerErr, "test-control-queue", metricsHandler, log.NewNoopLogger())
	require.NoError(t, err, "non-retryable handler errors should be swallowed")
	requireMetric(t, capture.Snapshot(), "non_retryable_error")
}

func TestHandleDispatchError_RetryableHandlerError_ReturnsError(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "something broke")
	err := HandleDispatchError(handlerErr, "test-control-queue", metricsHandler, log.NewNoopLogger())
	require.Error(t, err, "retryable handler errors should be returned")
	requireMetric(t, capture.Snapshot(), "transport_error")
}

func TestHandleDispatchError_CanceledError_ReturnsNil(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	canceledErr := temporal.NewCanceledError()
	err := HandleDispatchError(canceledErr, "test-control-queue", metricsHandler, log.NewNoopLogger())
	require.NoError(t, err, "CanceledError is permanent and should be swallowed")
	requireMetric(t, capture.Snapshot(), "worker_error")
}

// ---- helpers ----

func syncSuccessResponse() *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
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
}

func workerFailureResponse(message string) *matchingservice.DispatchNexusTaskResponse {
	failure := temporal.GetDefaultFailureConverter().ErrorToFailure(
		temporal.NewApplicationError(message, "TestErrorType", nil),
	)
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: failure,
		},
	}
}

func handlerErrorResponse(errType nexus.HandlerErrorType, message string) *matchingservice.DispatchNexusTaskResponse {
	// Simulate the response shape that DispatchResponseToError converts into a HandlerError.
	// For handler-level errors, matching returns a Response with a StartOperation failure
	// containing the error type in the failure metadata.
	handlerErr := nexus.NewHandlerErrorf(errType, "%s", message)
	failure := temporal.GetDefaultFailureConverter().ErrorToFailure(handlerErr)
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: failure,
		},
	}
}
