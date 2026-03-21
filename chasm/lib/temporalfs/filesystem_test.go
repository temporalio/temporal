package temporalfs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
)

func TestLifecycleState(t *testing.T) {
	testCases := []struct {
		name     string
		status   temporalfspb.FilesystemStatus
		expected chasm.LifecycleState
	}{
		{"UNSPECIFIED is Running", temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED, chasm.LifecycleStateRunning},
		{"RUNNING is Running", temporalfspb.FILESYSTEM_STATUS_RUNNING, chasm.LifecycleStateRunning},
		{"ARCHIVED is Completed", temporalfspb.FILESYSTEM_STATUS_ARCHIVED, chasm.LifecycleStateCompleted},
		{"DELETED is Completed", temporalfspb.FILESYSTEM_STATUS_DELETED, chasm.LifecycleStateCompleted},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{Status: tc.status},
			}
			require.Equal(t, tc.expected, fs.LifecycleState(nil))
		})
	}
}

func TestTerminate(t *testing.T) {
	ctx := newMockMutableContext()
	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}

	resp, err := fs.Terminate(ctx, chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	require.Equal(t, chasm.TerminateComponentResponse{}, resp)
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_DELETED, fs.Status)
	// Verify DataCleanupTask is scheduled.
	require.Len(t, ctx.Tasks, 1)
	require.IsType(t, &temporalfspb.DataCleanupTask{}, ctx.Tasks[0].Payload)
}

func TestSearchAttributes(t *testing.T) {
	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}

	attrs := fs.SearchAttributes(nil)
	require.Len(t, attrs, 1)
}

func TestStateMachineState(t *testing.T) {
	// Nil FilesystemState returns UNSPECIFIED.
	fs := &Filesystem{}
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED, fs.StateMachineState())

	// Non-nil returns the actual status.
	fs.FilesystemState = &temporalfspb.FilesystemState{
		Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
	}
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_RUNNING, fs.StateMachineState())

	// SetStateMachineState works.
	fs.SetStateMachineState(temporalfspb.FILESYSTEM_STATUS_ARCHIVED)
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_ARCHIVED, fs.Status)
}
