package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func backlogTaskWithExpiry(t *testing.T, expiry *timestamppb.Timestamp) *internalTask {
	t.Helper()
	return newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		TaskId: 1,
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.Now(),
			ExpiryTime: expiry,
		},
	}, func(*internalTask, taskResponse) {})
}

func TestGetInvalidTaskTag(t *testing.T) {
	t.Run("expired -> memory stage", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, timestamppb.New(time.Now().Add(-time.Minute)))
		require.Equal(t, metrics.TaskExpireStageMemoryTag, getInvalidTaskTag(task))
	})

	t.Run("not expired -> invalid", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, nil)
		require.Equal(t, metrics.TaskInvalidTag, getInvalidTaskTag(task))
	})
}

func TestGetDroppedTaskExpiryReasonTag(t *testing.T) {
	t.Run("expired -> expired_memory", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, timestamppb.New(time.Now().Add(-time.Minute)))
		require.Equal(t, metrics.DroppedTaskReasonExpiredMemoryTag, getDroppedTaskExpiryReasonTag(task))
	})

	t.Run("not expired -> invalid", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, nil)
		require.Equal(t, metrics.DroppedTaskReasonInvalidTag, getDroppedTaskExpiryReasonTag(task))
	})
}

// TestRecordDroppedTask_ExcludesSyncMatch verifies the policy that backs every
// tasks_dropped emission site: recordDroppedTask counts a backlog task but skips a
// sync-match task (which would otherwise be miscounted as lost backlog). The engine
// drop sites all funnel through this helper, and the *_DroppedTaskMetric suites cover
// the backlog wiring end-to-end.
func TestRecordDroppedTask_ExcludesSyncMatch(t *testing.T) {
	taskInfo := &persistencespb.TaskInfo{CreateTime: timestamppb.Now()}

	syncMatchTask := newInternalTaskForSyncMatch(taskInfo, nil, 0, nil)
	require.True(t, syncMatchTask.isSyncMatchTask(), "precondition: must be a sync-match task")

	backlogTask := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		TaskId: 1,
		Data:   taskInfo,
	}, func(*internalTask, taskResponse) {})
	require.False(t, backlogTask.isSyncMatchTask(), "precondition: must be a backlog task")

	capture := metricstest.NewCaptureHandler()
	c := capture.StartCapture()
	defer capture.StopCapture(c)

	// Sync-match task: must NOT be counted.
	recordDroppedTask(capture, syncMatchTask, metrics.DroppedTaskReasonNotFoundTag)
	require.Empty(t, c.Snapshot()[metrics.DroppedTasksCounter.Name()], "sync-match task must not be counted")

	// Backlog task: must be counted with the given reason.
	recordDroppedTask(capture, backlogTask, metrics.DroppedTaskReasonNotFoundTag)
	recordings := c.Snapshot()[metrics.DroppedTasksCounter.Name()]
	require.Len(t, recordings, 1, "backlog task must be counted")
	require.Equal(t, metrics.DroppedTaskReasonNotFoundTag.Value, recordings[0].Tags["reason"])
}
