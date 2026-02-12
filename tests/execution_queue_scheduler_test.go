package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/tests/testcore"
)

// sumMetricValues sums all values for a given metric name across all tags.
func sumMetricValues(snapshot map[string][]*metricstest.CapturedRecording, metricName string) int64 {
	var total int64
	for _, recording := range snapshot[metricName] {
		if v, ok := recording.Value.(int64); ok {
			total += v
		}
	}
	return total
}

// getTimerStats returns count, total, average, min, max, p50, p90, p99 for a timer metric.
func getTimerStats(snapshot map[string][]*metricstest.CapturedRecording, metricName string) (count int, avg, min, max, p50, p90, p99 time.Duration) {
	recordings := snapshot[metricName]
	if len(recordings) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	var durations []time.Duration
	for _, recording := range recordings {
		if v, ok := recording.Value.(time.Duration); ok {
			durations = append(durations, v)
		}
	}

	if len(durations) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	// Sort for percentiles
	sortDurations(durations)

	count = len(durations)
	var total time.Duration
	min = durations[0]
	max = durations[count-1]

	for _, d := range durations {
		total += d
	}
	avg = total / time.Duration(count)

	// Percentiles
	p50 = durations[int(float64(count)*0.50)]
	p90 = durations[int(float64(count)*0.90)]
	p99Index := int(float64(count) * 0.99)
	if p99Index >= count {
		p99Index = count - 1
	}
	p99 = durations[p99Index]

	return count, avg, min, max, p50, p90, p99
}

// sortDurations sorts a slice of durations in ascending order.
func sortDurations(durations []time.Duration) {
	slices.Sort(durations)
}

// TestExecutionQueueSchedulerDisabled verifies behavior when the feature is disabled.
type ExecutionQueueSchedulerDisabledTestSuite struct {
	testcore.FunctionalTestBase
}

func TestExecutionQueueSchedulerDisabledTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ExecutionQueueSchedulerDisabledTestSuite))
}

func (s *ExecutionQueueSchedulerDisabledTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// DISABLE ExecutionQueueScheduler for comparison
		dynamicconfig.TaskSchedulerEnableExecutionQueueScheduler.Key(): false,
		// Use same settings as enabled contention test for fair comparison
		dynamicconfig.NumPendingActivitiesLimitError.Key():            5000,
		dynamicconfig.TransferProcessorSchedulerWorkerCount.Key():     50,
		dynamicconfig.HistoryCacheNonUserContextLockTimeout.Key():     2 * time.Millisecond,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// TestActivityWorkflowWithFeatureDisabled runs the same 2000-activity workload as the enabled test
// but with WQS disabled, for direct performance comparison.
func (s *ExecutionQueueSchedulerDisabledTestSuite) TestActivityWorkflowWithFeatureDisabled() {
	s.T().Skip("benchmark test — run manually with -run TestExecutionQueueSchedulerDisabledTestSuite/TestActivityWorkflowWithFeatureDisabled")
	const activityCount = 2000

	// Simple activity that returns immediately
	noopActivity := func(ctx context.Context, input int) (int, error) {
		return input, nil
	}

	// Workflow that runs many activities in parallel
	activityWorkflow := func(ctx workflow.Context) (int, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 60 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		// Start all activities in parallel
		var futures []workflow.Future
		for i := 0; i < activityCount; i++ {
			f := workflow.ExecuteActivity(ctx, noopActivity, i)
			futures = append(futures, f)
		}

		// Wait for all activities to complete
		completed := 0
		for _, f := range futures {
			var result int
			if err := f.Get(ctx, &result); err != nil {
				return completed, err
			}
			completed++
		}
		return completed, nil
	}

	s.Worker().RegisterWorkflowWithOptions(activityWorkflow, workflow.RegisterOptions{Name: "contention-workflow-disabled"})
	s.Worker().RegisterActivityWithOptions(noopActivity, activity.RegisterOptions{Name: "noop-activity-disabled"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	// Use a longer timeout context for workflows with many activities
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	workflowID := testcore.RandomizeStr("wf-contention-disabled")
	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.TaskQueue(),
	}, activityWorkflow)
	s.NoError(err)

	// Wait for workflow to complete
	var result int
	err = run.Get(ctx, &result)
	s.NoError(err)
	s.Equal(activityCount, result, "Workflow should complete all activities")

	// Check ExecutionQueueScheduler metrics (should be 0 when disabled)
	snapshot := capture.Snapshot()
	wqsTasksSubmitted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksSubmitted.Name())
	wqsTasksCompleted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksCompleted.Name())
	wqsTasksFailed := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksFailed.Name())
	wqsTasksAborted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksAborted.Name())
	wqsSubmitRejected := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerSubmitRejected.Name())

	// Get FIFO scheduler metrics
	fifoTasksCompleted := sumMetricValues(snapshot, metrics.FIFOSchedulerTasksCompleted.Name())
	fifoTasksFailed := sumMetricValues(snapshot, metrics.FIFOSchedulerTasksFailed.Name())

	s.Logger.Info("Activity workflow metrics with ExecutionQueueScheduler DISABLED",
		tag.NewInt64("wqs_tasks_submitted", wqsTasksSubmitted),
		tag.NewInt64("wqs_tasks_completed", wqsTasksCompleted),
		tag.NewInt64("wqs_tasks_failed", wqsTasksFailed),
		tag.NewInt64("wqs_tasks_aborted", wqsTasksAborted),
		tag.NewInt64("wqs_submit_rejected", wqsSubmitRejected),
		tag.NewInt64("fifo_tasks_completed", fifoTasksCompleted),
		tag.NewInt64("fifo_tasks_failed", fifoTasksFailed))

	// Get latency metrics
	taskQueueLatencyCount, taskQueueLatencyAvg, taskQueueLatencyMin, taskQueueLatencyMax, taskQueueLatencyP50, taskQueueLatencyP90, taskQueueLatencyP99 := getTimerStats(snapshot, metrics.TaskQueueLatency.Name())

	s.Logger.Info("Task end-to-end latency (task_latency_queue) - DISABLED",
		tag.NewInt("count", taskQueueLatencyCount),
		tag.NewDurationTag("avg", taskQueueLatencyAvg),
		tag.NewDurationTag("min", taskQueueLatencyMin),
		tag.NewDurationTag("max", taskQueueLatencyMax),
		tag.NewDurationTag("p50", taskQueueLatencyP50),
		tag.NewDurationTag("p90", taskQueueLatencyP90),
		tag.NewDurationTag("p99", taskQueueLatencyP99))

	// Verify no tasks were routed to WQS
	s.Equal(int64(0), wqsTasksSubmitted, "No tasks should be submitted to ExecutionQueueScheduler when disabled")
}

// ExecutionQueueSchedulerActivityTestSuite tests that the ExecutionQueueScheduler feature works
// correctly with activity-based workflows.
type ExecutionQueueSchedulerActivityTestSuite struct {
	testcore.FunctionalTestBase
}

func TestExecutionQueueSchedulerActivityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ExecutionQueueSchedulerActivityTestSuite))
}

func (s *ExecutionQueueSchedulerActivityTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.TaskSchedulerEnableExecutionQueueScheduler.Key(): true,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// ExecutionQueueSchedulerContentionTestSuite tests that tasks are routed to the
// ExecutionQueueScheduler when lock contention occurs.
type ExecutionQueueSchedulerContentionTestSuite struct {
	testcore.FunctionalTestBase
}

func TestExecutionQueueSchedulerContentionTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ExecutionQueueSchedulerContentionTestSuite))
}

func (s *ExecutionQueueSchedulerContentionTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// Enable ExecutionQueueScheduler
		dynamicconfig.TaskSchedulerEnableExecutionQueueScheduler.Key(): true,
		// Increase pending activities limit to allow more parallel activities
		dynamicconfig.NumPendingActivitiesLimitError.Key(): 5000,
		// Set FIFO worker count to 50 to increase contention
		dynamicconfig.TransferProcessorSchedulerWorkerCount.Key(): 50,
		// Use very short lock timeout to trigger contention - tasks waiting for the lock
		// will fail with ErrResourceExhaustedBusyWorkflow and get routed to ExecutionQueueScheduler
		dynamicconfig.HistoryCacheNonUserContextLockTimeout.Key():                      2 * time.Millisecond,
		dynamicconfig.TaskSchedulerExecutionQueueSchedulerQueueConcurrency.Key():        2,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// TestActivityWorkflowWithMetricsObservability runs a workflow with many activities
// and verifies that ExecutionQueueScheduler routes tasks under contention.
// With 2ms lock timeout and 50 concurrent FIFO workers processing 2000 activities,
// lock contention is reliably triggered, causing tasks to be routed to the EQS.
func (s *ExecutionQueueSchedulerContentionTestSuite) TestActivityWorkflowWithMetricsObservability() {
	s.T().Skip("benchmark test — run manually with -run TestExecutionQueueSchedulerContentionTestSuite/TestActivityWorkflowWithMetricsObservability")
	const activityCount = 2000

	// Simple activity that returns immediately
	noopActivity := func(ctx context.Context, input int) (int, error) {
		return input, nil
	}

	// Workflow that runs many activities in parallel
	activityWorkflow := func(ctx workflow.Context) (int, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 60 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		// Start all activities in parallel
		var futures []workflow.Future
		for i := 0; i < activityCount; i++ {
			f := workflow.ExecuteActivity(ctx, noopActivity, i)
			futures = append(futures, f)
		}

		// Wait for all activities to complete
		completed := 0
		for _, f := range futures {
			var result int
			if err := f.Get(ctx, &result); err != nil {
				return completed, err
			}
			completed++
		}
		return completed, nil
	}

	s.Worker().RegisterWorkflowWithOptions(activityWorkflow, workflow.RegisterOptions{Name: "contention-workflow"})
	s.Worker().RegisterActivityWithOptions(noopActivity, activity.RegisterOptions{Name: "noop-activity"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	// Use a longer timeout context for workflows with many activities
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	workflowID := testcore.RandomizeStr("wf-contention")
	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.TaskQueue(),
	}, activityWorkflow)
	s.NoError(err)

	// Wait for workflow to complete
	var result int
	err = run.Get(ctx, &result)
	s.NoError(err)
	s.Equal(activityCount, result, "Workflow should complete all activities")

	// Check ExecutionQueueScheduler metrics
	snapshot := capture.Snapshot()
	wqsTasksSubmitted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksSubmitted.Name())
	wqsTasksCompleted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksCompleted.Name())
	wqsTasksFailed := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksFailed.Name())
	wqsTasksAborted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksAborted.Name())
	wqsSubmitRejected := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerSubmitRejected.Name())

	// Get FIFO scheduler metrics
	fifoTasksCompleted := sumMetricValues(snapshot, metrics.FIFOSchedulerTasksCompleted.Name())
	fifoTasksFailed := sumMetricValues(snapshot, metrics.FIFOSchedulerTasksFailed.Name())

	s.Logger.Info("Activity workflow metrics with ExecutionQueueScheduler enabled",
		tag.NewInt64("wqs_tasks_submitted", wqsTasksSubmitted),
		tag.NewInt64("wqs_tasks_completed", wqsTasksCompleted),
		tag.NewInt64("wqs_tasks_failed", wqsTasksFailed),
		tag.NewInt64("wqs_tasks_aborted", wqsTasksAborted),
		tag.NewInt64("wqs_submit_rejected", wqsSubmitRejected),
		tag.NewInt64("fifo_tasks_completed", fifoTasksCompleted),
		tag.NewInt64("fifo_tasks_failed", fifoTasksFailed))

	// Get latency metrics
	// TaskQueueLatency: End-to-end latency from task generation to completion
	taskQueueLatencyCount, taskQueueLatencyAvg, taskQueueLatencyMin, taskQueueLatencyMax, taskQueueLatencyP50, taskQueueLatencyP90, taskQueueLatencyP99 := getTimerStats(snapshot, metrics.TaskQueueLatency.Name())

	// ExecutionQueueSchedulerTaskLatency: Latency for tasks processed by WQS
	wqsLatencyCount, wqsLatencyAvg, wqsLatencyMin, wqsLatencyMax, wqsLatencyP50, wqsLatencyP90, wqsLatencyP99 := getTimerStats(snapshot, metrics.ExecutionQueueSchedulerTaskLatency.Name())

	// ExecutionQueueSchedulerQueueWaitTime: Time tasks spend waiting in WQS queue
	wqsWaitCount, wqsWaitAvg, wqsWaitMin, wqsWaitMax, wqsWaitP50, wqsWaitP90, wqsWaitP99 := getTimerStats(snapshot, metrics.ExecutionQueueSchedulerQueueWaitTime.Name())

	s.Logger.Info("Task end-to-end latency (task_latency_queue)",
		tag.NewInt("count", taskQueueLatencyCount),
		tag.NewDurationTag("avg", taskQueueLatencyAvg),
		tag.NewDurationTag("min", taskQueueLatencyMin),
		tag.NewDurationTag("max", taskQueueLatencyMax),
		tag.NewDurationTag("p50", taskQueueLatencyP50),
		tag.NewDurationTag("p90", taskQueueLatencyP90),
		tag.NewDurationTag("p99", taskQueueLatencyP99))

	s.Logger.Info("WQS task latency (execution_queue_scheduler_task_latency)",
		tag.NewInt("count", wqsLatencyCount),
		tag.NewDurationTag("avg", wqsLatencyAvg),
		tag.NewDurationTag("min", wqsLatencyMin),
		tag.NewDurationTag("max", wqsLatencyMax),
		tag.NewDurationTag("p50", wqsLatencyP50),
		tag.NewDurationTag("p90", wqsLatencyP90),
		tag.NewDurationTag("p99", wqsLatencyP99))

	s.Logger.Info("WQS queue wait time (execution_queue_scheduler_queue_wait_time)",
		tag.NewInt("count", wqsWaitCount),
		tag.NewDurationTag("avg", wqsWaitAvg),
		tag.NewDurationTag("min", wqsWaitMin),
		tag.NewDurationTag("max", wqsWaitMax),
		tag.NewDurationTag("p50", wqsWaitP50),
		tag.NewDurationTag("p90", wqsWaitP90),
		tag.NewDurationTag("p99", wqsWaitP99))

	// With 2ms lock timeout and 50 concurrent workers processing 2000 activities,
	// some tasks should hit contention and be routed to ExecutionQueueScheduler.
	s.Greater(wqsTasksSubmitted, int64(0),
		"With 2ms lock timeout and 50 workers, some tasks should be routed to ExecutionQueueScheduler")
}

// TestActivityWorkflowWithFeatureEnabled verifies that workflows with activities work correctly
// when the ExecutionQueueScheduler feature is enabled.
//
// This is a smoke test that ensures the feature doesn't break activity-based workflows.
// The actual routing of tasks through the ExecutionQueueScheduler only occurs when
// ErrResourceExhaustedBusyWorkflow errors happen (lock contention), which requires
// specific timing conditions that are hard to trigger reliably in functional tests.
// The unit tests in execution_aware_scheduler_test.go verify the routing logic directly.
func (s *ExecutionQueueSchedulerActivityTestSuite) TestActivityWorkflowWithFeatureEnabled() {
	const activityCount = 10

	// Simple activity that does nothing (just returns)
	noopActivity := func(ctx context.Context) error {
		return nil
	}

	// Workflow that schedules multiple activities concurrently
	activityWorkflow := func(ctx workflow.Context) (int, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 60 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		// Schedule all activities concurrently using futures
		var futures []workflow.Future
		for range activityCount {
			f := workflow.ExecuteActivity(ctx, noopActivity)
			futures = append(futures, f)
		}

		// Wait for all activities to complete
		completed := 0
		for _, f := range futures {
			if err := f.Get(ctx, nil); err != nil {
				return completed, err
			}
			completed++
		}
		return completed, nil
	}

	s.Worker().RegisterWorkflowWithOptions(activityWorkflow, workflow.RegisterOptions{Name: "activity-workflow"})
	s.Worker().RegisterActivityWithOptions(noopActivity, activity.RegisterOptions{Name: "noop-activity"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	workflowID := testcore.RandomizeStr("wf-activity")
	run, err := s.SdkClient().ExecuteWorkflow(testcore.NewContext(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.TaskQueue(),
	}, activityWorkflow)
	s.NoError(err)

	var result int
	err = run.Get(testcore.NewContext(), &result)
	s.NoError(err)
	s.Equal(activityCount, result, "Workflow should complete all activities")

	// Log ExecutionQueueScheduler metrics for observability
	// Note: These will typically be 0 in functional tests because lock contention is hard to trigger
	snapshot := capture.Snapshot()
	tasksSubmitted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksSubmitted.Name())
	tasksCompleted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksCompleted.Name())

	s.Logger.Info("Activity test with ExecutionQueueScheduler enabled",
		tag.NewInt64("tasks_submitted", tasksSubmitted),
		tag.NewInt64("tasks_completed", tasksCompleted))
}

// TestActivityWorkflowRoutesToEQSUnderContention verifies that tasks are routed to the
// ExecutionQueueScheduler when lock contention occurs. Uses moderate load (200 activities)
// to trigger contention without heavy benchmarking.
func (s *ExecutionQueueSchedulerContentionTestSuite) TestActivityWorkflowRoutesToEQSUnderContention() {
	const activityCount = 200

	noopActivity := func(ctx context.Context, input int) (int, error) {
		return input, nil
	}

	activityWorkflow := func(ctx workflow.Context) (int, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 60 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var futures []workflow.Future
		for i := 0; i < activityCount; i++ {
			f := workflow.ExecuteActivity(ctx, noopActivity, i)
			futures = append(futures, f)
		}

		completed := 0
		for _, f := range futures {
			var result int
			if err := f.Get(ctx, &result); err != nil {
				return completed, err
			}
			completed++
		}
		return completed, nil
	}

	s.Worker().RegisterWorkflowWithOptions(activityWorkflow, workflow.RegisterOptions{Name: "contention-functional-workflow"})
	s.Worker().RegisterActivityWithOptions(noopActivity, activity.RegisterOptions{Name: "noop-activity-functional"})

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	workflowID := testcore.RandomizeStr("wf-contention-functional")
	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.TaskQueue(),
	}, activityWorkflow)
	s.NoError(err)

	var result int
	err = run.Get(ctx, &result)
	s.NoError(err)
	s.Equal(activityCount, result, "All activities should complete")

	snapshot := capture.Snapshot()
	wqsTasksSubmitted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksSubmitted.Name())
	wqsTasksCompleted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksCompleted.Name())
	wqsTasksAborted := sumMetricValues(snapshot, metrics.ExecutionQueueSchedulerTasksAborted.Name())

	s.Greater(wqsTasksSubmitted, int64(0),
		"With 2ms lock timeout and 50 workers, some tasks should be routed to ExecutionQueueScheduler")
	s.Greater(wqsTasksCompleted, int64(0),
		"ExecutionQueueScheduler should complete some tasks")
	s.Equal(int64(0), wqsTasksAborted,
		"No tasks should be aborted")
}
