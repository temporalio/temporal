package history

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func testWorkerCommandsTask() *tasks.WorkerCommandsTask {
	return &tasks.WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey("test-ns-id", "test-wf-id", "test-run-id"),
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("token1")},
			}},
		},
		Destination: "/temporal-sys/worker-commands/test-ns/key1",
	}
}

func requireMetricValue(t *testing.T, snap map[string][]*metricstest.CapturedRecording, expectedOutcome string) {
	t.Helper()
	recordings := snap[metrics.WorkerCommandsSent.Name()]
	require.Len(t, recordings, 1, "expected exactly 1 dispatch metric recording")
	require.Equal(t, expectedOutcome, recordings[0].Tags["outcome"])
}

func TestExecute_FeatureFlagOff_DropsTask(t *testing.T) {
	d := &workerCommandsTaskDispatcher{
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return false },
		},
		logger: log.NewNoopLogger(),
	}

	task := testWorkerCommandsTask()
	err := d.execute(context.Background(), task, 1 /* attempt */)
	require.NoError(t, err, "task should be silently dropped when feature flag is off")
}

func TestExecute_EmptyCommands_DropsTask(t *testing.T) {
	d := &workerCommandsTaskDispatcher{
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return true },
		},
		logger: log.NewNoopLogger(),
	}

	task := testWorkerCommandsTask()
	task.Commands = nil
	err := d.execute(context.Background(), task, 1 /* attempt */)
	require.NoError(t, err, "task with no commands should be dropped")
}

func TestExecute_ExceedsMaxAttempts_DropsTask(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return true },
		},
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	task := testWorkerCommandsTask()
	err := d.execute(context.Background(), task, workerCommandsMaxTaskAttempt+1)
	require.NoError(t, err, "task should be dropped when max attempts exceeded")

	requireMetricValue(t, capture.Snapshot(), "max_attempts_exceeded")
}

func TestExecute_AtMaxAttempt_StillExecutes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		matchingClient: mockClient,
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return true },
		},
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		&matchingservice.DispatchNexusTaskResponse{
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
		}, nil)

	task := testWorkerCommandsTask()
	err := d.execute(context.Background(), task, workerCommandsMaxTaskAttempt)
	require.NoError(t, err, "task at exactly max attempt should still execute")

	requireMetricValue(t, capture.Snapshot(), "success")
}

func TestExecute_DispatchSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		matchingClient: mockClient,
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return true },
		},
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	var capturedReq *matchingservice.DispatchNexusTaskRequest
	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *matchingservice.DispatchNexusTaskRequest, _ ...any) (*matchingservice.DispatchNexusTaskResponse, error) {
			capturedReq = req
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
			}, nil
		})

	task := testWorkerCommandsTask()
	err := d.execute(context.Background(), task, 1 /* attempt */)
	require.NoError(t, err)

	require.NotNil(t, capturedReq)
	require.Equal(t, enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS, capturedReq.TaskQueue.Kind,
		"dispatch request must use TASK_QUEUE_KIND_WORKER_COMMANDS, not TASK_QUEUE_KIND_NORMAL")
	require.Equal(t, task.Destination, capturedReq.TaskQueue.Name)

	requireMetricValue(t, capture.Snapshot(), "success")
}

func TestExecute_DispatchRPCError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		matchingClient: mockClient,
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return true },
		},
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("connection refused"))

	task := testWorkerCommandsTask()
	err := d.execute(context.Background(), task, 1 /* attempt */)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")

	requireMetricValue(t, capture.Snapshot(), "rpc_error")
}

func TestExecute_UpstreamTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		matchingClient: mockClient,
		config: &configs.Config{
			EnableCancelActivityWorkerCommand: func() bool { return true },
		},
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	mockClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(
		&matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
				RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
			},
		}, nil)

	task := testWorkerCommandsTask()
	err := d.execute(context.Background(), task, 1 /* attempt */)
	require.Error(t, err)

	var he *nexus.HandlerError
	require.ErrorAs(t, err, &he)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, he.Type)

	requireMetricValue(t, capture.Snapshot(), "no_poller")
}

func TestHandleError_WorkerError_ReturnNil(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	// Worker-returned errors (ApplicationError, CanceledError) are permanent.
	workerErr := temporal.NewApplicationError("worker bug", "SomeType", nil)
	task := testWorkerCommandsTask()
	err := d.handleError(workerErr, task)
	require.NoError(t, err, "worker-returned errors are permanent and should be swallowed")

	requireMetricValue(t, capture.Snapshot(), "worker_error")
}

func TestHandleError_UpstreamTimeout_ReturnRetryable(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
	task := testWorkerCommandsTask()
	err := d.handleError(handlerErr, task)
	require.Error(t, err, "upstream timeout should be retried")

	var he *nexus.HandlerError
	require.ErrorAs(t, err, &he)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, he.Type)

	requireMetricValue(t, capture.Snapshot(), "no_poller")
}

func TestHandleError_NonRetryableHandlerError_ReturnNil(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad request")
	task := testWorkerCommandsTask()
	err := d.handleError(handlerErr, task)
	require.NoError(t, err, "non-retryable handler errors should be swallowed")

	requireMetricValue(t, capture.Snapshot(), "non_retryable_error")
}

func TestHandleError_OtherHandlerError_ReturnRetryable(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	d := &workerCommandsTaskDispatcher{
		metricsHandler: metricsHandler,
		logger:         log.NewNoopLogger(),
	}

	handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "something broke")
	task := testWorkerCommandsTask()
	err := d.handleError(handlerErr, task)
	require.Error(t, err, "transport errors should be retried")

	requireMetricValue(t, capture.Snapshot(), "transport_error")
}
