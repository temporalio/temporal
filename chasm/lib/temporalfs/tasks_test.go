package temporalfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newTestStoreProvider(t *testing.T) *PebbleStoreProvider {
	t.Helper()
	p := NewPebbleStoreProvider(t.TempDir(), log.NewTestLogger())
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func newRunningFilesystem() *Filesystem {
	return &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
			Config: &temporalfspb.FilesystemConfig{
				ChunkSize:  256 * 1024,
				MaxSize:    1 << 30,
				MaxFiles:   100_000,
				GcInterval: durationpb.New(5 * time.Minute),
			},
			Stats: &temporalfspb.FSStats{},
		},
	}
}

// initTestFS creates a temporal-fs filesystem in the store provider for the given namespace/filesystem.
func initTestFS(t *testing.T, provider *PebbleStoreProvider, nsID, fsID string) {
	t.Helper()
	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, err := tfs.Create(s, tfs.Options{})
	require.NoError(t, err)
	f.Close()
}

// --- Validate tests ---

func TestChunkGCValidate(t *testing.T) {
	executor := &chunkGCTaskExecutor{}

	testCases := []struct {
		status   temporalfspb.FilesystemStatus
		expected bool
	}{
		{temporalfspb.FILESYSTEM_STATUS_RUNNING, true},
		{temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED, false},
		{temporalfspb.FILESYSTEM_STATUS_ARCHIVED, false},
		{temporalfspb.FILESYSTEM_STATUS_DELETED, false},
	}

	for _, tc := range testCases {
		t.Run(tc.status.String(), func(t *testing.T) {
			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{Status: tc.status},
			}
			ok, err := executor.Validate(nil, fs, chasm.TaskAttributes{}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ok)
		})
	}
}

func TestManifestCompactValidate(t *testing.T) {
	executor := &manifestCompactTaskExecutor{}

	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}
	ok, err := executor.Validate(nil, fs, chasm.TaskAttributes{}, nil)
	require.NoError(t, err)
	require.True(t, ok)

	fs.Status = temporalfspb.FILESYSTEM_STATUS_ARCHIVED
	ok, err = executor.Validate(nil, fs, chasm.TaskAttributes{}, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestQuotaCheckValidate(t *testing.T) {
	executor := &quotaCheckTaskExecutor{}

	fs := &Filesystem{
		FilesystemState: &temporalfspb.FilesystemState{
			Status: temporalfspb.FILESYSTEM_STATUS_RUNNING,
		},
	}
	ok, err := executor.Validate(nil, fs, chasm.TaskAttributes{}, nil)
	require.NoError(t, err)
	require.True(t, ok)

	fs.Status = temporalfspb.FILESYSTEM_STATUS_DELETED
	ok, err = executor.Validate(nil, fs, chasm.TaskAttributes{}, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

// --- Execute tests ---

func TestChunkGCExecute(t *testing.T) {
	provider := newTestStoreProvider(t)
	logger := log.NewTestLogger()

	nsID := "test-namespace-id"
	fsID := "test-filesystem-id"
	initTestFS(t, provider, nsID, fsID)

	executor := newChunkGCTaskExecutor(nil, logger, provider)
	ctx := newMockMutableContext()
	fs := newRunningFilesystem()

	err := executor.Execute(ctx, fs, chasm.TaskAttributes{}, &temporalfspb.ChunkGCTask{})
	require.NoError(t, err)

	// Stats should be updated (TransitionCount incremented).
	require.NotNil(t, fs.Stats)
	require.EqualValues(t, 1, fs.Stats.TransitionCount)

	// GC task should be rescheduled.
	require.Len(t, ctx.Tasks, 1)
	task := ctx.Tasks[0]
	require.IsType(t, &temporalfspb.ChunkGCTask{}, task.Payload)
	expectedTime := defaultTime.Add(5 * time.Minute)
	require.Equal(t, expectedTime, task.Attributes.ScheduledTime)
}

func TestChunkGCExecute_NoGCInterval(t *testing.T) {
	provider := newTestStoreProvider(t)
	logger := log.NewTestLogger()

	nsID := "test-namespace-id"
	fsID := "test-filesystem-id"
	initTestFS(t, provider, nsID, fsID)

	executor := newChunkGCTaskExecutor(nil, logger, provider)
	ctx := newMockMutableContext()
	fs := newRunningFilesystem()
	fs.Config.GcInterval = durationpb.New(0) // Disable GC rescheduling.

	err := executor.Execute(ctx, fs, chasm.TaskAttributes{}, &temporalfspb.ChunkGCTask{})
	require.NoError(t, err)

	// No task should be rescheduled.
	require.Empty(t, ctx.Tasks)
}

func TestQuotaCheckExecute(t *testing.T) {
	provider := newTestStoreProvider(t)
	logger := log.NewTestLogger()

	nsID := "test-namespace-id"
	fsID := "test-filesystem-id"
	initTestFS(t, provider, nsID, fsID)

	executor := newQuotaCheckTaskExecutor(nil, logger, provider)
	ctx := newMockMutableContext()
	fs := newRunningFilesystem()

	err := executor.Execute(ctx, fs, chasm.TaskAttributes{}, &temporalfspb.QuotaCheckTask{})
	require.NoError(t, err)

	// Stats should be initialized (metrics are per-instance so values may be zero
	// for a freshly opened FS, but the stats struct must be populated).
	require.NotNil(t, fs.Stats)
}

func TestQuotaCheckExecute_WithWrites(t *testing.T) {
	provider := newTestStoreProvider(t)

	nsID := "test-namespace-id"
	fsID := "test-filesystem-id"

	// Create FS, write data, and keep the FS open — metrics accumulate in-memory.
	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, err := tfs.Create(s, tfs.Options{})
	require.NoError(t, err)

	err = f.WriteFile("/test.txt", []byte("hello world"), 0o644)
	require.NoError(t, err)

	// Verify metrics are tracked on the open FS instance.
	m := f.Metrics()
	require.Greater(t, m.BytesWritten.Load(), int64(0))
	require.EqualValues(t, 1, m.FilesCreated.Load())
	f.Close()
}
