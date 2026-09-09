package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// watchRunningStart decides, per buffered start, whether a completion callback was attached to
// a still-running action or whether the action had already finished. Two of the "finished"
// paths *synthesize* a result rather than reading one off the workflow -- a target that is
// gone entirely is recorded TERMINATED, and one that closes mid-attach is recorded COMPLETED.
// Those are the paths that were previously invisible, so the reason each result carries is
// what ScheduleCallbackReattach is built on and is worth pinning down.
func TestWatchRunningStart_ReattachClassification(t *testing.T) {
	closeTime := timestamppb.New(time.Now().UTC())

	cases := []struct {
		name          string
		setupHistory  func(*historyservicemock.MockHistoryServiceClient)
		setupFrontend func(*workflowservicemock.MockWorkflowServiceClient)
		wantReason    metrics.ReasonString
		// wantStatus is zero when the callback was attached and the action is still running.
		wantStatus enumspb.WorkflowExecutionStatus
		wantErr    bool
	}{
		{
			name: "attached to running workflow",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&historyservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						},
					}, nil)
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {
				c.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&workflowservice.StartWorkflowExecutionResponse{}, nil)
			},
			wantReason: reasonNone,
		},
		{
			name: "paused workflow still counts as progressing",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&historyservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status: enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
						},
					}, nil)
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {
				c.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&workflowservice.StartWorkflowExecutionResponse{}, nil)
			},
			wantReason: reasonNone,
		},
		{
			name: "target gone entirely synthesizes TERMINATED",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					nil, serviceerror.NewNotFound("execution not found"))
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {},
			wantReason:    reasonReattachNotFound,
			wantStatus:    enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		},
		{
			name: "already closed reports the observed status",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&historyservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
							CloseTime: closeTime,
						},
					}, nil)
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {},
			wantReason:    reasonReattachAlreadyClosed,
			wantStatus:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		{
			name: "closed mid-attach synthesizes COMPLETED",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&historyservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						},
					}, nil)
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {
				c.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "req-id", "run-id"))
			},
			wantReason: reasonReattachRace,
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			name: "unexpected describe error is surfaced, not classified",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					nil, serviceerror.NewUnavailable("history unavailable"))
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {},
			wantErr:       true,
		},
		{
			name: "unexpected attach error is surfaced, not classified",
			setupHistory: func(c *historyservicemock.MockHistoryServiceClient) {
				c.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&historyservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						},
					}, nil)
			},
			setupFrontend: func(c *workflowservicemock.MockWorkflowServiceClient) {
				c.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					nil, errors.New("boom"))
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
			tc.setupHistory(historyClient)
			tc.setupFrontend(frontendClient)

			handler := &SchedulerCallbacksTaskHandler{
				config: &Config{
					EncodeInternalTokenWithEnvelope: func(string) bool { return true },
				},
				historyClient:  resource.HistoryClient(historyClient),
				frontendClient: frontendClient,
				metricsHandler: metrics.NoopMetricsHandler,
			}

			sched := &Scheduler{
				SchedulerState: &schedulerpb.SchedulerState{
					Namespace:   "ns",
					NamespaceId: "ns-id",
					ScheduleId:  "sched-id",
					Schedule: &schedulepb.Schedule{
						Action: &schedulepb.ScheduleAction{
							Action: &schedulepb.ScheduleAction_StartWorkflow{
								StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
									WorkflowId:   "scheduled-wf",
									WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
								},
							},
						},
					},
				},
			}

			start := &schedulespb.BufferedStart{
				RequestId:  "req-id",
				WorkflowId: "scheduled-wf",
				RunId:      "run-id",
			}

			result, err := handler.watchRunningStart(context.Background(), sched, start, []byte("ref"))
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.wantReason, result.reason)

			if tc.wantStatus == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
				require.Nil(t, result.completed,
					"a successful attach must leave the start running so the callback reports its completion")
				return
			}
			require.NotNil(t, result.completed)
			require.Equal(t, tc.wantStatus, result.completed.Status)
		})
	}
}
