package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCompletedResultFromNexusCompletion(t *testing.T) {
	closeTime := timestamppb.New(time.Now().UTC())
	tests := []struct {
		name       string
		completion *persistencespb.ChasmNexusCompletion
		wantStatus enumspb.WorkflowExecutionStatus
		wantErr    bool
	}{
		{name: "nil completion", wantErr: true},
		{
			name:       "missing outcome",
			completion: &persistencespb.ChasmNexusCompletion{CloseTime: closeTime},
			wantErr:    true,
		},
		{
			name: "missing failure",
			completion: &persistencespb.ChasmNexusCompletion{
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{},
			},
			wantErr: true,
		},
		{
			name: "success",
			completion: &persistencespb.ChasmNexusCompletion{
				Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
				CloseTime: closeTime,
			},
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			name: "failure",
			completion: &persistencespb.ChasmNexusCompletion{
				Outcome:   &persistencespb.ChasmNexusCompletion_Failure{Failure: &failurepb.Failure{}},
				CloseTime: closeTime,
			},
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		{
			name: "canceled",
			completion: &persistencespb.ChasmNexusCompletion{
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{Failure: &failurepb.Failure{
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{},
				}},
				CloseTime: closeTime,
			},
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		},
		{
			name: "timed out",
			completion: &persistencespb.ChasmNexusCompletion{
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{Failure: &failurepb.Failure{
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{},
				}},
				CloseTime: closeTime,
			},
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		},
		{
			name: "terminated",
			completion: &persistencespb.ChasmNexusCompletion{
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{Failure: &failurepb.Failure{
					FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
				}},
				CloseTime: closeTime,
			},
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completed, err := completedResultFromNexusCompletion(tt.completion)
			if tt.wantErr {
				var invalidArgument *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgument)
				require.Nil(t, completed)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, completed.GetStatus())
			require.Equal(t, closeTime, completed.GetCloseTime())
		})
	}
}

func TestCompletedResultFromWorkflowInfo(t *testing.T) {
	closeTime := timestamppb.New(time.Now().UTC())
	tests := []struct {
		name      string
		status    enumspb.WorkflowExecutionStatus
		completed bool
		wantErr   bool
	}{
		{name: "unspecified", status: enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, wantErr: true},
		{name: "running", status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		{name: "completed", status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, completed: true},
		{name: "failed", status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, completed: true},
		{name: "canceled", status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, completed: true},
		{name: "terminated", status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, completed: true},
		{name: "continued as new", status: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW},
		{name: "timed out", status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, completed: true},
		{name: "paused", status: enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED},
		{name: "unknown", status: enumspb.WorkflowExecutionStatus(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completed, err := completedResultFromWorkflowInfo(&workflowpb.WorkflowExecutionInfo{
				Status:    tt.status,
				CloseTime: closeTime,
			})
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, completed)
				return
			}
			require.NoError(t, err)
			if !tt.completed {
				require.Nil(t, completed)
				return
			}
			require.Equal(t, tt.status, completed.GetStatus())
			require.Equal(t, closeTime, completed.GetCloseTime())
		})
	}
}
