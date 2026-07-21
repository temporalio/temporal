package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	historyservicemock "go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWatchRunningStart_AttachesOnlyToDescribedChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{
		Config:        callbackTestConfig(),
		HistoryClient: historyClient,
	})
	scheduler := callbackTestScheduler()
	start := &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}

	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("successor-run", "first-run"), nil)
	historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.StartWorkflowExecutionRequest, _ ...any) (*historyservice.StartWorkflowExecutionResponse, error) {
			require.Equal(t, "first-run", request.GetExpectedFirstExecutionRunId())
			require.Equal(t, "workflow-id", request.GetStartRequest().GetWorkflowId())
			return &historyservice.StartWorkflowExecutionResponse{RunId: "successor-run"}, nil
		},
	)

	result, err := handler.watchRunningStart(context.Background(), scheduler, start, []byte("scheduler-ref"))
	require.NoError(t, err)
	require.Nil(t, result.completed)
}

func TestWatchRunningStart_RecordsActualStatusAfterAttachRace(t *testing.T) {
	for _, status := range []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{Config: callbackTestConfig(), HistoryClient: historyClient})

			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", "first-run"), nil)
			historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, context.DeadlineExceeded)
			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(closedDescribeResponse(status), nil)

			result, err := handler.watchRunningStart(context.Background(), callbackTestScheduler(), &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}, []byte("scheduler-ref"))
			require.NoError(t, err)
			require.Equal(t, status, result.completed.GetStatus())
		})
	}
}

func TestWatchRunningStart_RefusesLegacyExecutionWithoutChainID(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{Config: callbackTestConfig(), HistoryClient: historyClient})
	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", ""), nil)

	_, err := handler.watchRunningStart(context.Background(), callbackTestScheduler(), &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}, []byte("scheduler-ref"))
	require.Error(t, err)
}

func callbackTestConfig() *Config {
	return &Config{EncodeInternalTokenWithEnvelope: func(string) bool { return true }}
}

func callbackTestScheduler() *Scheduler {
	return &Scheduler{SchedulerState: &schedulerpb.SchedulerState{
		Namespace: "namespace", NamespaceId: "namespace-id", ScheduleId: "schedule-id",
		Schedule: &schedulepb.Schedule{Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "workflow-id", WorkflowType: &commonpb.WorkflowType{Name: "workflow-type"}},
		}}},
	}}
}

func runningDescribeResponse(runID, firstRunID string) *historyservice.DescribeWorkflowExecutionResponse {
	return &historyservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: runID}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, FirstRunId: firstRunID},
	}
}

func closedDescribeResponse(status enumspb.WorkflowExecutionStatus) *historyservice.DescribeWorkflowExecutionResponse {
	return &historyservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, CloseTime: timestamppb.New(time.Now())},
	}
}
