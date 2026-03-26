package temporalzfs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	temporalzfspb "go.temporal.io/server/chasm/lib/temporalzfs/gen/temporalzfspb/v1"
)

func TestLifecycleState(t *testing.T) {
	testCases := []struct {
		name     string
		status   temporalzfspb.FilesystemStatus
		expected chasm.LifecycleState
	}{
		{"UNSPECIFIED is Running", temporalzfspb.FILESYSTEM_STATUS_UNSPECIFIED, chasm.LifecycleStateRunning},
		{"RUNNING is Running", temporalzfspb.FILESYSTEM_STATUS_RUNNING, chasm.LifecycleStateRunning},
		{"ARCHIVED is Completed", temporalzfspb.FILESYSTEM_STATUS_ARCHIVED, chasm.LifecycleStateCompleted},
		{"DELETED is Completed", temporalzfspb.FILESYSTEM_STATUS_DELETED, chasm.LifecycleStateCompleted},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &Filesystem{
				FilesystemState: &temporalzfspb.FilesystemState{Status: tc.status},
			}
			require.Equal(t, tc.expected, fs.LifecycleState(nil))
		})
	}
}

func TestTerminate(t *testing.T) {
	ctx := newMockMutableContext()
	fs := &Filesystem{
		FilesystemState: &temporalzfspb.FilesystemState{
			Status: temporalzfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}

	resp, err := fs.Terminate(ctx, chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	require.Equal(t, chasm.TerminateComponentResponse{}, resp)
	require.Equal(t, temporalzfspb.FILESYSTEM_STATUS_DELETED, fs.Status)
	// Verify DataCleanupTask is scheduled.
	require.Len(t, ctx.Tasks, 1)
	require.IsType(t, &temporalzfspb.DataCleanupTask{}, ctx.Tasks[0].Payload)
}

func TestSearchAttributes(t *testing.T) {
	fs := &Filesystem{
		FilesystemState: &temporalzfspb.FilesystemState{
			Status: temporalzfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}

	attrs := fs.SearchAttributes(nil)
	require.Len(t, attrs, 1)
}

func TestStateMachineState(t *testing.T) {
	// Nil FilesystemState returns UNSPECIFIED.
	fs := &Filesystem{}
	require.Equal(t, temporalzfspb.FILESYSTEM_STATUS_UNSPECIFIED, fs.StateMachineState())

	// Non-nil returns the actual status.
	fs.FilesystemState = &temporalzfspb.FilesystemState{
		Status: temporalzfspb.FILESYSTEM_STATUS_RUNNING,
	}
	require.Equal(t, temporalzfspb.FILESYSTEM_STATUS_RUNNING, fs.StateMachineState())

	// SetStateMachineState works.
	fs.SetStateMachineState(temporalzfspb.FILESYSTEM_STATUS_ARCHIVED)
	require.Equal(t, temporalzfspb.FILESYSTEM_STATUS_ARCHIVED, fs.Status)
}
