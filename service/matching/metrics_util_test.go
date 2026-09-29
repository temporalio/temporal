package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
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

func TestGetDroppedTaskExpiryReason(t *testing.T) {
	t.Run("expired -> expired_memory", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, timestamppb.New(time.Now().Add(-time.Minute)))
		require.Equal(t, dropReasonExpiredMemory, getDroppedTaskExpiryReason(task))
	})

	t.Run("not expired -> invalid", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, nil)
		require.Equal(t, dropReasonInvalid, getDroppedTaskExpiryReason(task))
	})
}

// TestRecordDroppedTask verifies the single tasks_dropped entry point records the counter
// with the reason and fairness_key tags, and is a no-op when the reason is dropReasonUnspecified.
func TestRecordDroppedTask(t *testing.T) {
	capture := metricstest.NewCaptureHandler()
	c := capture.StartCapture()
	defer capture.StopCapture(c)

	breakdownOn := &taskQueueConfig{BreakdownMetricsByFairnessKey: func() bool { return true }}
	breakdownOff := &taskQueueConfig{BreakdownMetricsByFairnessKey: func() bool { return false }}
	pri := &commonpb.Priority{FairnessKey: "orders"}

	// not dropped (normal completion): no-op.
	recordDroppedTask(capture, breakdownOn, dropReasonUnspecified, pri)
	require.Empty(t, c.Snapshot()[metrics.DroppedTasksCounter.Name()])

	// dropped, breakdown enabled: real fairness key tagged.
	recordDroppedTask(capture, breakdownOn, dropReasonNotFound, pri)
	// dropped, breakdown disabled: fairness key omitted.
	recordDroppedTask(capture, breakdownOff, dropReasonNotFound, pri)

	recordings := c.Snapshot()[metrics.DroppedTasksCounter.Name()]
	require.Len(t, recordings, 2)
	require.Equal(t, dropReasonNotFound.tag().Value, recordings[0].Tags["reason"])
	require.Equal(t, "orders", recordings[0].Tags[metrics.FairnessKeyTagName])
	require.Equal(t, "__omitted__", recordings[1].Tags[metrics.FairnessKeyTagName])
}
