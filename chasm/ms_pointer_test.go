package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestMSPointer_LifecycleState(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		status   enumspb.WorkflowExecutionStatus
		expected LifecycleState
	}{
		{enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, LifecycleStateRunning},
		{enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, LifecycleStateRunning},
		{enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, LifecycleStateRunning},
		{enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, LifecycleStateCompleted},
		{enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, LifecycleStateCompleted},
		{enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, LifecycleStateFailed},
		{enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, LifecycleStateFailed},
		{enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, LifecycleStateFailed},
		{enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, LifecycleStateFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.status.String(), func(t *testing.T) {
			t.Parallel()

			backend := &MockNodeBackend{
				HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
					return &persistencespb.WorkflowExecutionState{Status: tc.status}
				},
			}
			require.Equal(t, tc.expected, NewMSPointer(backend).LifecycleState())
		})
	}
}

func TestMSPointer_LifecycleState_UnknownStatus(t *testing.T) {
	t.Parallel()

	backend := &MockNodeBackend{
		HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
			return &persistencespb.WorkflowExecutionState{Status: enumspb.WorkflowExecutionStatus(9999)}
		},
	}
	require.Equal(t, LifecycleStateRunning, NewMSPointer(backend).LifecycleState())
}
