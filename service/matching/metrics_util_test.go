package matching

import (
	"slices"
	"sync"
	"testing"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	enumspb "go.temporal.io/api/enums/v1"
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

// newTallyPrometheusHandler returns a metrics.Handler backed by the same tally
// Prometheus reporter the server uses in production, plus a flush() that forces a
// final report (where counters are registered) and returns any registration errors
// the reporter surfaced via its OnError callback.
func newTallyPrometheusHandler(t *testing.T) (metrics.Handler, func() []error) {
	t.Helper()
	var mu sync.Mutex
	var regErrs []error
	// Use the low-level reporter constructor (not Configuration.NewReporter, which
	// registers an HTTP handler on the default mux and panics when called twice).
	reporter := tallyprom.NewReporter(tallyprom.Options{
		Registerer: promclient.NewRegistry(),
		OnRegisterError: func(e error) {
			mu.Lock()
			defer mu.Unlock()
			regErrs = append(regErrs, e)
		},
	})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter: reporter,
		Separator:      tallyprom.DefaultSeparator,
	}, time.Hour)
	flush := func() []error {
		require.NoError(t, closer.Close()) // forces a final report -> counter registration
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(regErrs)
	}
	return metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope), flush
}

// TestDroppedTasksCounter_PrometheusLabelConsistency reproduces the root cause of
// #10468 against the real tally Prometheus reporter: emitting tasks_dropped from two
// handlers whose label-key sets differ makes the reporter reject the second descriptor
// ("different label names"), while a uniform key set registers cleanly. The matching
// code keeps the poll path (matchingEngineImpl.droppedTaskMetricsHandler) and the
// backlog path (the physical-queue handler) on the same key set.
func TestDroppedTasksCounter_PrometheusLabelConsistency(t *testing.T) {
	// The per-task-queue labels both paths always carry.
	canonical := []metrics.Tag{
		metrics.OperationTag("PollWorkflowTaskQueue"),
		metrics.NamespaceTag("ns"),
		metrics.UnsafeTaskQueueTag("tq"),
		metrics.TaskQueueTypeTag(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		metrics.PartitionTag("0"),
	}
	// The labels the backlog/physical-queue handler adds; the poll path must match them.
	versionAndState := []metrics.Tag{
		metrics.NamespaceStateTag(metrics.ActiveNamespaceStateTagValue),
		metrics.WorkerVersionTag("", false),
		metrics.WorkerDeploymentNameTag("", false),
		metrics.WorkerDeploymentBuildIDTag("", false),
	}

	t.Run("divergent label keys are rejected", func(t *testing.T) {
		h, flush := newTallyPrometheusHandler(t)
		// Poll path (pre-fix): canonical labels only.
		metrics.DroppedTasksCounter.With(h.WithTags(canonical...)).Record(1, metrics.DroppedTaskReasonNotFoundTag)
		// Backlog path: canonical + version/state -> different label keys, rejected.
		rich := append(slices.Clone(canonical), versionAndState...)
		metrics.DroppedTasksCounter.With(h.WithTags(rich...)).Record(1, metrics.DroppedTaskReasonExpiredReadTag)
		errs := flush()
		require.NotEmpty(t, errs, "expected Prometheus to reject the second descriptor")
		require.Contains(t, errs[0].Error(), "label names")
	})

	t.Run("uniform label keys register cleanly", func(t *testing.T) {
		h, flush := newTallyPrometheusHandler(t)
		pollLike := append(slices.Clone(canonical), versionAndState...)
		// Same key set; only the operation tag value differs, as it does in practice.
		backlogLike := append(slices.Clone(canonical), versionAndState...)
		backlogLike[0] = metrics.OperationTag("matchingTaskQueueMgr")
		metrics.DroppedTasksCounter.With(h.WithTags(pollLike...)).Record(1, metrics.DroppedTaskReasonNotFoundTag)
		metrics.DroppedTasksCounter.With(h.WithTags(backlogLike...)).Record(1, metrics.DroppedTaskReasonExpiredReadTag)
		require.Empty(t, flush(), "uniform label keys must register without error")
	})
}
