package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/tasks"
)

func TestCanCoalesceVerifyTasks(t *testing.T) {
	prev := newVerifyTaskForTest(1, "run-a", 2, eventHistoryForTest(10, 1), "")
	testCases := []struct {
		name     string
		prev     *replicationspb.ReplicationTask
		next     *replicationspb.ReplicationTask
		expected bool
	}{
		{
			name:     "same run, newer transition, same branch",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-a", 3, eventHistoryForTest(12, 1), ""),
			expected: true,
		},
		{
			name:     "same run, newer transition on a newer failover version",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-a", 3, eventHistoryForTest(10, 1, 15, 2), ""),
			expected: true,
		},
		{
			name:     "next carries a new run id",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-a", 3, eventHistoryForTest(12, 1), "run-a-next"),
			expected: true,
		},
		{
			name:     "prev without events",
			prev:     newVerifyTaskForTest(1, "run-a", 2, nil, ""),
			next:     newVerifyTaskForTest(2, "run-a", 3, eventHistoryForTest(12, 1), ""),
			expected: true,
		},
		{
			name:     "different run",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-b", 3, eventHistoryForTest(12, 1), ""),
			expected: false,
		},
		{
			name:     "older transition",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-a", 1, eventHistoryForTest(12, 1), ""),
			expected: false,
		},
		{
			name:     "prev carries a new run id",
			prev:     newVerifyTaskForTest(1, "run-a", 2, eventHistoryForTest(10, 1), "run-a-next"),
			next:     newVerifyTaskForTest(2, "run-a", 3, eventHistoryForTest(12, 1), ""),
			expected: false,
		},
		{
			name:     "prev events on another branch",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-a", 3, eventHistoryForTest(5, 1, 20, 2), ""),
			expected: false,
		},
		{
			name:     "next without events",
			prev:     prev,
			next:     newVerifyTaskForTest(2, "run-a", 3, nil, ""),
			expected: false,
		},
		{
			name:     "next is not a verify task",
			prev:     prev,
			next:     newSyncTaskForTest(2),
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, canCoalesceVerifyTasks(tc.prev, tc.next))
		})
	}
}

func TestStreamSenderTaskBatcher(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)
	batcher := newStreamSenderTaskBatcher(true, metricsHandler, 1, 2, enumsspb.TASK_PRIORITY_HIGH)
	first := convertedReplicationTask{
		sourceTask: &tasks.SyncVersionedTransitionTask{},
		task:       newVerifyTaskForTest(1, "run-a", 1, eventHistoryForTest(10, 1), ""),
	}
	second := convertedReplicationTask{
		sourceTask: &tasks.SyncVersionedTransitionTask{},
		task:       newVerifyTaskForTest(2, "run-a", 2, eventHistoryForTest(12, 1), ""),
	}
	otherRun := convertedReplicationTask{
		sourceTask: &tasks.SyncVersionedTransitionTask{},
		task:       newVerifyTaskForTest(3, "run-b", 1, eventHistoryForTest(5, 1), ""),
	}

	ready := batcher.Batch(first)
	require.Empty(t, ready)

	ready = batcher.Batch(second)
	require.Empty(t, ready)
	recordings := capture.SnapshotMetric(metrics.ReplicationTaskVerifyCoalesced.Name())
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Contains(t, recordings[0].Tags, metrics.OperationTagName)

	ready = batcher.Batch(otherRun)
	require.Len(t, ready, 1)
	require.Same(t, second.task, ready[0].task)

	ready = batcher.Flush()
	require.Len(t, ready, 1)
	require.Same(t, otherRun.task, ready[0].task)
	require.Empty(t, batcher.Flush())
}

func TestStreamSenderTaskBatcherDisabled(t *testing.T) {
	batcher := newStreamSenderTaskBatcher(false, metrics.NoopMetricsHandler, 1, 2, enumsspb.TASK_PRIORITY_HIGH)
	task := convertedReplicationTask{task: newVerifyTaskForTest(1, "run-a", 1, eventHistoryForTest(10, 1), "")}

	ready := batcher.Batch(task)
	require.Len(t, ready, 1)
	require.Same(t, task.task, ready[0].task)
	require.Empty(t, batcher.Flush())
}
