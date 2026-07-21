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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	workflowservicemock "go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWatchRunningStart_AttachesToCurrentRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{
		Config:         callbackTestConfig(),
		HistoryClient:  historyClient,
		FrontendClient: frontendClient,
	})

	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", "first-run"), nil)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&workflowservice.StartWorkflowExecutionResponse{RunId: "first-run", FirstExecutionRunId: "first-run"}, nil)

	result, err := handler.watchRunningStart(context.Background(), callbackTestScheduler(), &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}, []byte("scheduler-ref"))
	require.NoError(t, err)
	require.Nil(t, result.completed)
	require.Equal(t, "first-run", result.firstExecutionRunID)
}

func TestWatchRunningStart_AttachesToSameChainSuccessor(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{
		Config:         callbackTestConfig(),
		HistoryClient:  historyClient,
		FrontendClient: frontendClient,
	})
	scheduler := callbackTestScheduler()
	start := &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}

	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", "first-run"), nil)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...any) (*workflowservice.StartWorkflowExecutionResponse, error) {
			require.Equal(t, "workflow-id", request.GetWorkflowId())
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "successor-run", FirstExecutionRunId: "first-run"}, nil
		},
	)

	result, err := handler.watchRunningStart(context.Background(), scheduler, start, []byte("scheduler-ref"))
	require.NoError(t, err)
	require.Nil(t, result.completed)
	require.Equal(t, "first-run", result.firstExecutionRunID)
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
			frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
			handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{Config: callbackTestConfig(), HistoryClient: historyClient, FrontendClient: frontendClient})

			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", "first-run"), nil)
			frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&workflowservice.StartWorkflowExecutionResponse{RunId: "unrelated-run", FirstExecutionRunId: "unrelated-run"}, nil)
			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(closedDescribeResponse(status), nil)

			result, err := handler.watchRunningStart(context.Background(), callbackTestScheduler(), &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}, []byte("scheduler-ref"))
			require.NoError(t, err)
			require.Equal(t, status, result.completed.GetStatus())
		})
	}
}

func TestWatchRunningStart_LegacyExecutionRequiresExactRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{Config: callbackTestConfig(), HistoryClient: historyClient, FrontendClient: frontendClient})
	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", ""), nil)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&workflowservice.StartWorkflowExecutionResponse{RunId: "first-run"}, nil)

	result, err := handler.watchRunningStart(context.Background(), callbackTestScheduler(), &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}, []byte("scheduler-ref"))
	require.NoError(t, err)
	require.Nil(t, result.completed)
}

func TestWatchRunningStart_LegacyExecutionRejectsDifferentRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	handler := NewSchedulerCallbacksTaskHandler(SchedulerCallbacksTaskHandlerOptions{Config: callbackTestConfig(), HistoryClient: historyClient, FrontendClient: frontendClient})
	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(runningDescribeResponse("first-run", ""), nil)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&workflowservice.StartWorkflowExecutionResponse{RunId: "different-run"}, nil)
	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		closedDescribeResponse(enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED), nil)

	result, err := handler.watchRunningStart(context.Background(), callbackTestScheduler(), &schedulespb.BufferedStart{WorkflowId: "workflow-id", RunId: "first-run", RequestId: "request-id"}, []byte("scheduler-ref"))
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, result.completed.GetStatus())
	require.Empty(t, result.firstExecutionRunID)
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
