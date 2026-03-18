package temporalfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

var defaultTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func newMockMutableContext() *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{
					NamespaceID: "test-namespace-id",
					BusinessID:  "test-filesystem-id",
				}
			},
		},
	}
}

func TestTransitionCreate(t *testing.T) {
	testCases := []struct {
		name              string
		config            *temporalfspb.FilesystemConfig
		ownerWorkflowID   string
		expectDefaultConf bool
		expectGCTask      bool
	}{
		{
			name: "with custom config",
			config: &temporalfspb.FilesystemConfig{
				ChunkSize:  512 * 1024,
				MaxSize:    2 << 30,
				MaxFiles:   50_000,
				GcInterval: durationpb.New(10 * time.Minute),
			},
			ownerWorkflowID:   "wf-123",
			expectDefaultConf: false,
			expectGCTask:      true,
		},
		{
			name:              "with nil config uses defaults",
			config:            nil,
			ownerWorkflowID:   "wf-456",
			expectDefaultConf: true,
			expectGCTask:      true,
		},
		{
			name: "with zero GC interval schedules no task",
			config: &temporalfspb.FilesystemConfig{
				ChunkSize:  256 * 1024,
				GcInterval: durationpb.New(0),
			},
			ownerWorkflowID:   "",
			expectDefaultConf: false,
			expectGCTask:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newMockMutableContext()

			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{},
			}

			err := TransitionCreate.Apply(fs, ctx, CreateEvent{
				Config:          tc.config,
				OwnerWorkflowID: tc.ownerWorkflowID,
			})
			require.NoError(t, err)

			// Verify status.
			require.Equal(t, temporalfspb.FILESYSTEM_STATUS_RUNNING, fs.Status)

			// Verify inode and txn IDs.
			require.EqualValues(t, 2, fs.NextInodeId)
			require.EqualValues(t, 1, fs.NextTxnId)

			// Verify stats initialized.
			require.NotNil(t, fs.Stats)

			// Verify owner workflow ID.
			require.Equal(t, tc.ownerWorkflowID, fs.OwnerWorkflowId)

			// Verify config.
			require.NotNil(t, fs.Config)
			if tc.expectDefaultConf {
				require.EqualValues(t, defaultChunkSize, fs.Config.ChunkSize)
				require.EqualValues(t, defaultMaxSize, fs.Config.MaxSize)
				require.EqualValues(t, defaultMaxFiles, fs.Config.MaxFiles)
				require.Equal(t, defaultGCInterval, fs.Config.GcInterval.AsDuration())
				require.Equal(t, defaultSnapshotRetention, fs.Config.SnapshotRetention.AsDuration())
			} else {
				require.Equal(t, tc.config.ChunkSize, fs.Config.ChunkSize)
			}

			// Verify GC task.
			if tc.expectGCTask {
				require.Len(t, ctx.Tasks, 1)
				task := ctx.Tasks[0]
				require.IsType(t, &temporalfspb.ChunkGCTask{}, task.Payload)
				expectedTime := defaultTime.Add(fs.Config.GcInterval.AsDuration())
				require.Equal(t, expectedTime, task.Attributes.ScheduledTime)
			} else {
				require.Empty(t, ctx.Tasks)
			}
		})
	}
}

func TestTransitionCreate_InvalidSourceState(t *testing.T) {
	for _, status := range []temporalfspb.FilesystemStatus{
		temporalfspb.FILESYSTEM_STATUS_RUNNING,
		temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
		temporalfspb.FILESYSTEM_STATUS_DELETED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newMockMutableContext()
			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{Status: status},
			}
			err := TransitionCreate.Apply(fs, ctx, CreateEvent{})
			require.ErrorIs(t, err, chasm.ErrInvalidTransition)
		})
	}
}

func TestTransitionArchive(t *testing.T) {
	ctx := newMockMutableContext()
	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}

	err := TransitionArchive.Apply(fs, ctx, nil)
	require.NoError(t, err)
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_ARCHIVED, fs.Status)
}

func TestTransitionArchive_InvalidSourceStates(t *testing.T) {
	for _, status := range []temporalfspb.FilesystemStatus{
		temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED,
		temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
		temporalfspb.FILESYSTEM_STATUS_DELETED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newMockMutableContext()
			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{Status: status},
			}
			err := TransitionArchive.Apply(fs, ctx, nil)
			require.ErrorIs(t, err, chasm.ErrInvalidTransition)
		})
	}
}

func TestTransitionDelete_FromRunning(t *testing.T) {
	ctx := newMockMutableContext()
	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}

	err := TransitionDelete.Apply(fs, ctx, nil)
	require.NoError(t, err)
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_DELETED, fs.Status)
}

func TestTransitionDelete_FromArchived(t *testing.T) {
	ctx := newMockMutableContext()
	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
		},
	}

	err := TransitionDelete.Apply(fs, ctx, nil)
	require.NoError(t, err)
	require.Equal(t, temporalfspb.FILESYSTEM_STATUS_DELETED, fs.Status)
}

func TestTransitionDelete_InvalidSourceStates(t *testing.T) {
	for _, status := range []temporalfspb.FilesystemStatus{
		temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED,
		temporalfspb.FILESYSTEM_STATUS_DELETED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newMockMutableContext()
			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{Status: status},
			}
			err := TransitionDelete.Apply(fs, ctx, nil)
			require.ErrorIs(t, err, chasm.ErrInvalidTransition)
		})
	}
}
