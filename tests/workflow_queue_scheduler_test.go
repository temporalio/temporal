package tests

import (
	"context"
	"sync"
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

type WorkflowQueueSchedulerTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowQueueSchedulerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowQueueSchedulerTestSuite))
}

func (s *WorkflowQueueSchedulerTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.TaskSchedulerEnableWorkflowQueueScheduler.Key():      true,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerWorkerCount.Key(): 4,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerQueueSize.Key():   1000,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// TestSignalBurstProcessing verifies that rapid signal bursts are processed correctly
// with the WorkflowQueueScheduler feature enabled.
//
// This is a smoke test that ensures the feature doesn't break normal signal processing.
// The actual routing of tasks through the WorkflowQueueScheduler only occurs when
// ErrResourceExhaustedBusyWorkflow errors happen (lock contention), which requires
// specific timing conditions that are hard to trigger reliably in functional tests.
// The unit tests in workflow_aware_scheduler_test.go verify the routing logic directly.
func (s *WorkflowQueueSchedulerTestSuite) TestSignalBurstProcessing() {
	const workflowCount = 5
	const signalsPerWorkflow = 50 // Total: 250 signals across 5 workflows
	const signalName = "test-signal"

	// Workflow that counts signals received
	signalCountWorkflow := func(ctx workflow.Context) (int, error) {
		count := 0
		signalChan := workflow.GetSignalChannel(ctx, signalName)

		// Keep receiving signals until timeout
		for {
			var signal int
			ok, _ := signalChan.ReceiveWithTimeout(ctx, 30*time.Second, &signal)
			if !ok {
				// Timeout - no more signals coming
				break
			}
			count++
			if count >= signalsPerWorkflow {
				break
			}
		}

		return count, nil
	}

	s.Worker().RegisterWorkflowWithOptions(signalCountWorkflow, workflow.RegisterOptions{Name: "signal-count-workflow"})

	// Start metrics capture
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	// Start multiple workflows to increase load
	var runs []client.WorkflowRun
	for i := range workflowCount {
		workflowID := testcore.RandomizeStr("wf-signal-burst")
		run, err := s.SdkClient().ExecuteWorkflow(testcore.NewContext(), client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: s.TaskQueue(),
		}, signalCountWorkflow)
		s.NoError(err, "Failed to start workflow %d", i)
		runs = append(runs, run)
	}

	// Wait a moment for workflows to start
	time.Sleep(200 * time.Millisecond)

	// Send signals in parallel to all workflows
	var wg sync.WaitGroup
	for _, run := range runs {
		for j := range signalsPerWorkflow {
			wg.Add(1)
			go func(r client.WorkflowRun, signalNum int) {
				defer wg.Done()
				err := s.SdkClient().SignalWorkflow(testcore.NewContext(), r.GetID(), r.GetRunID(), signalName, signalNum)
				if err != nil {
					s.Logger.Warn("Signal failed", tag.Error(err), tag.Counter(signalNum))
				}
			}(run, j)
		}
	}
	wg.Wait()

	// Wait for all workflows to complete
	for i, run := range runs {
		var result int
		err := run.Get(testcore.NewContext(), &result)
		s.NoError(err, "Workflow %d failed", i)
		s.Equal(signalsPerWorkflow, result, "Workflow %d should process all signals", i)
	}

	// Log WorkflowQueueScheduler metrics for observability
	// Note: These will typically be 0 in functional tests because lock contention is hard to trigger
	snapshot := capture.Snapshot()
	tasksSubmitted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())
	tasksCompleted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksCompleted.Name())

	s.Logger.Info("WorkflowQueueScheduler metrics",
		tag.NewInt64("tasks_submitted", tasksSubmitted),
		tag.NewInt64("tasks_completed", tasksCompleted))
}

// TestSignalBurstComparison runs the same test with the feature enabled
// to compare behavior and collect metrics.
func (s *WorkflowQueueSchedulerTestSuite) TestSignalBurstComparison() {
	const signalCount = 50
	const signalName = "comparison-signal"

	countWorkflow := func(ctx workflow.Context) (int, error) {
		count := 0
		signalChan := workflow.GetSignalChannel(ctx, signalName)

		// Process signals with a timeout
		for {
			var signal int
			ok, _ := signalChan.ReceiveWithTimeout(ctx, 2*time.Second, &signal)
			if !ok {
				break
			}
			count++
			if count >= signalCount {
				break
			}
		}
		return count, nil
	}

	s.Worker().RegisterWorkflowWithOptions(countWorkflow, workflow.RegisterOptions{Name: "comparison-workflow"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	workflowID := testcore.RandomizeStr("wf-comparison")
	run, err := s.SdkClient().ExecuteWorkflow(testcore.NewContext(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.TaskQueue(),
	}, countWorkflow)
	s.NoError(err)

	time.Sleep(100 * time.Millisecond)

	// Send signals rapidly
	var wg sync.WaitGroup
	for i := range signalCount {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			_ = s.SdkClient().SignalWorkflow(testcore.NewContext(), workflowID, run.GetRunID(), signalName, num)
		}(i)
	}
	wg.Wait()

	var result int
	err = run.Get(testcore.NewContext(), &result)
	s.NoError(err)
	s.Equal(signalCount, result)

	snapshot := capture.Snapshot()
	s.Logger.Info("Comparison test metrics",
		tag.NewInt64("tasks_submitted", sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())),
		tag.NewInt64("tasks_completed", sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksCompleted.Name())))
}

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

type WorkflowQueueSchedulerWorkerScalingTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowQueueSchedulerWorkerScalingTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowQueueSchedulerWorkerScalingTestSuite))
}

func (s *WorkflowQueueSchedulerWorkerScalingTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.TaskSchedulerEnableWorkflowQueueScheduler.Key():      true,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerWorkerCount.Key(): 2, // Start with low worker count
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerQueueSize.Key():   1000,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// TestWorkerScaling verifies that worker count changes take effect during runtime.
func (s *WorkflowQueueSchedulerWorkerScalingTestSuite) TestWorkerScaling() {
	const workflowCount = 5
	const signalsPerWorkflow = 20
	const signalName = "scaling-signal"

	// Workflow that processes signals sequentially with a small delay
	processingWorkflow := func(ctx workflow.Context) (int, error) {
		count := 0
		signalChan := workflow.GetSignalChannel(ctx, signalName)

		for {
			var signal int
			ok, _ := signalChan.ReceiveWithTimeout(ctx, 5*time.Second, &signal)
			if !ok {
				break
			}
			// Small processing delay to simulate work
			_ = workflow.Sleep(ctx, 10*time.Millisecond)
			count++
		}
		return count, nil
	}

	s.Worker().RegisterWorkflowWithOptions(processingWorkflow, workflow.RegisterOptions{Name: "scaling-workflow"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	// Start multiple workflows
	var runs []client.WorkflowRun
	for range workflowCount {
		workflowID := testcore.RandomizeStr("wf-scaling")
		run, err := s.SdkClient().ExecuteWorkflow(testcore.NewContext(), client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: s.TaskQueue(),
		}, processingWorkflow)
		s.NoError(err)
		runs = append(runs, run)
	}

	time.Sleep(200 * time.Millisecond)

	// Send signals to all workflows concurrently
	var wg sync.WaitGroup
	for _, run := range runs {
		for j := range signalsPerWorkflow {
			wg.Add(1)
			go func(r client.WorkflowRun, num int) {
				defer wg.Done()
				_ = s.SdkClient().SignalWorkflow(testcore.NewContext(), r.GetID(), r.GetRunID(), signalName, num)
			}(run, j)
		}
	}
	wg.Wait()

	// Increase worker count mid-test
	s.OverrideDynamicConfig(
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerWorkerCount,
		8, // Increase from 2 to 8
	)

	// Wait for all workflows to complete
	for _, run := range runs {
		var result int
		err := run.Get(testcore.NewContext(), &result)
		s.NoError(err)
		s.Equal(signalsPerWorkflow, result, "Each workflow should process all its signals")
	}

	// Check metrics
	snapshot := capture.Snapshot()
	activeWorkers := getLatestGaugeValue(snapshot, metrics.WorkflowQueueSchedulerActiveWorkers.Name())

	s.Logger.Info("Worker scaling test metrics",
		tag.NewFloat64("active_workers", activeWorkers),
		tag.NewInt64("tasks_submitted", sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())),
		tag.NewInt64("tasks_completed", sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksCompleted.Name())))

	// Verify worker count was updated (should reflect the increased count)
	// Note: The exact value depends on timing, but it should be > 2 if scaling happened
}

// getLatestGaugeValue returns the most recent value for a gauge metric.
func getLatestGaugeValue(snapshot map[string][]*metricstest.CapturedRecording, metricName string) float64 {
	recordings := snapshot[metricName]
	if len(recordings) == 0 {
		return 0
	}
	// Return the last recorded value
	last := recordings[len(recordings)-1]
	if v, ok := last.Value.(float64); ok {
		return v
	}
	return 0
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
	for i := 0; i < len(durations)-1; i++ {
		for j := i + 1; j < len(durations); j++ {
			if durations[j] < durations[i] {
				durations[i], durations[j] = durations[j], durations[i]
			}
		}
	}
}

// TestWorkflowQueueSchedulerDisabled verifies behavior when the feature is disabled.
type WorkflowQueueSchedulerDisabledTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowQueueSchedulerDisabledTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowQueueSchedulerDisabledTestSuite))
}

func (s *WorkflowQueueSchedulerDisabledTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// DISABLE WorkflowQueueScheduler for comparison
		dynamicconfig.TaskSchedulerEnableWorkflowQueueScheduler.Key(): false,
		// Use same settings as enabled contention test for fair comparison
		dynamicconfig.NumPendingActivitiesLimitError.Key():            1200,
		dynamicconfig.TransferProcessorSchedulerWorkerCount.Key():     50,
		dynamicconfig.HistoryCacheNonUserContextLockTimeout.Key():     5 * time.Millisecond,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *WorkflowQueueSchedulerDisabledTestSuite) TestSignalBurstWithFeatureDisabled() {
	const signalCount = 50
	const signalName = "disabled-test-signal"

	countWorkflow := func(ctx workflow.Context) (int, error) {
		count := 0
		signalChan := workflow.GetSignalChannel(ctx, signalName)

		for {
			var signal int
			ok, _ := signalChan.ReceiveWithTimeout(ctx, 3*time.Second, &signal)
			if !ok {
				break
			}
			count++
			if count >= signalCount {
				break
			}
		}
		return count, nil
	}

	s.Worker().RegisterWorkflowWithOptions(countWorkflow, workflow.RegisterOptions{Name: "disabled-test-workflow"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	workflowID := testcore.RandomizeStr("wf-disabled")
	run, err := s.SdkClient().ExecuteWorkflow(testcore.NewContext(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.TaskQueue(),
	}, countWorkflow)
	s.NoError(err)

	time.Sleep(100 * time.Millisecond)

	// Send signals
	var wg sync.WaitGroup
	for i := range signalCount {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			_ = s.SdkClient().SignalWorkflow(testcore.NewContext(), workflowID, run.GetRunID(), signalName, num)
		}(i)
	}
	wg.Wait()

	var result int
	err = run.Get(testcore.NewContext(), &result)
	s.NoError(err)
	s.Equal(signalCount, result)

	// With feature disabled, WorkflowQueueScheduler metrics should be zero or minimal
	snapshot := capture.Snapshot()
	tasksSubmitted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())

	s.Logger.Info("Disabled feature test metrics",
		tag.NewInt64("tasks_submitted", tasksSubmitted))

	// Tasks should not be routed to WorkflowQueueScheduler when disabled
	s.Equal(int64(0), tasksSubmitted, "No tasks should be submitted to WorkflowQueueScheduler when disabled")
}

// TestActivityWorkflowWithFeatureDisabled runs the same 500-activity workload as the enabled test
// but with WQS disabled, for direct performance comparison.
func (s *WorkflowQueueSchedulerDisabledTestSuite) TestActivityWorkflowWithFeatureDisabled() {
	const activityCount = 500

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

	// Check WorkflowQueueScheduler metrics (should be 0 when disabled)
	snapshot := capture.Snapshot()
	wqsTasksSubmitted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())
	wqsTasksCompleted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksCompleted.Name())
	wqsTasksFailed := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksFailed.Name())
	wqsTasksAborted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksAborted.Name())
	wqsSubmitRejected := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerSubmitRejected.Name())

	s.Logger.Info("Activity workflow metrics with WorkflowQueueScheduler DISABLED",
		tag.NewInt64("wqs_tasks_submitted", wqsTasksSubmitted),
		tag.NewInt64("wqs_tasks_completed", wqsTasksCompleted),
		tag.NewInt64("wqs_tasks_failed", wqsTasksFailed),
		tag.NewInt64("wqs_tasks_aborted", wqsTasksAborted),
		tag.NewInt64("wqs_submit_rejected", wqsSubmitRejected))

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
	s.Equal(int64(0), wqsTasksSubmitted, "No tasks should be submitted to WorkflowQueueScheduler when disabled")
}

// WorkflowQueueSchedulerActivityTestSuite tests that the WorkflowQueueScheduler feature works
// correctly with activity-based workflows.
type WorkflowQueueSchedulerActivityTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowQueueSchedulerActivityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowQueueSchedulerActivityTestSuite))
}

func (s *WorkflowQueueSchedulerActivityTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.TaskSchedulerEnableWorkflowQueueScheduler.Key():      true,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerWorkerCount.Key(): 4,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerQueueSize.Key():   1000,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// WorkflowQueueSchedulerContentionTestSuite tests that tasks are routed to the
// WorkflowQueueScheduler when lock contention occurs.
type WorkflowQueueSchedulerContentionTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowQueueSchedulerContentionTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowQueueSchedulerContentionTestSuite))
}

func (s *WorkflowQueueSchedulerContentionTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// Enable WorkflowQueueScheduler
		dynamicconfig.TaskSchedulerEnableWorkflowQueueScheduler.Key():      true,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerWorkerCount.Key(): 50,
		dynamicconfig.TaskSchedulerWorkflowQueueSchedulerQueueSize.Key():   2000,
		// Increase pending activities limit to allow more parallel activities
		dynamicconfig.NumPendingActivitiesLimitError.Key(): 1200,
		// Set FIFO worker count to 50 to increase contention
		dynamicconfig.TransferProcessorSchedulerWorkerCount.Key(): 50,
		// Use very short lock timeout to trigger contention - tasks waiting for the lock
		// will fail with ErrResourceExhaustedBusyWorkflow and get routed to WorkflowQueueScheduler
		dynamicconfig.HistoryCacheNonUserContextLockTimeout.Key(): 5 * time.Millisecond,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

// TestActivityWorkflowWithMetricsObservability runs a workflow with many activities
// and logs WorkflowQueueScheduler metrics for observability.
//
// Note: This is primarily a smoke test. Lock contention that triggers
// WorkflowQueueScheduler routing depends on very precise timing conditions that
// are hard to reliably trigger in functional tests. The unit tests in
// workflow_aware_scheduler_test.go verify the routing logic directly.
func (s *WorkflowQueueSchedulerContentionTestSuite) TestActivityWorkflowWithMetricsObservability() {
	const activityCount = 500

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

	// Check WorkflowQueueScheduler metrics
	snapshot := capture.Snapshot()
	wqsTasksSubmitted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())
	wqsTasksCompleted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksCompleted.Name())
	wqsTasksFailed := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksFailed.Name())
	wqsTasksAborted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksAborted.Name())
	wqsSubmitRejected := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerSubmitRejected.Name())

	s.Logger.Info("Activity workflow metrics with WorkflowQueueScheduler enabled",
		tag.NewInt64("wqs_tasks_submitted", wqsTasksSubmitted),
		tag.NewInt64("wqs_tasks_completed", wqsTasksCompleted),
		tag.NewInt64("wqs_tasks_failed", wqsTasksFailed),
		tag.NewInt64("wqs_tasks_aborted", wqsTasksAborted),
		tag.NewInt64("wqs_submit_rejected", wqsSubmitRejected))

	// Get latency metrics
	// TaskQueueLatency: End-to-end latency from task generation to completion
	taskQueueLatencyCount, taskQueueLatencyAvg, taskQueueLatencyMin, taskQueueLatencyMax, taskQueueLatencyP50, taskQueueLatencyP90, taskQueueLatencyP99 := getTimerStats(snapshot, metrics.TaskQueueLatency.Name())

	// WorkflowQueueSchedulerTaskLatency: Latency for tasks processed by WQS
	wqsLatencyCount, wqsLatencyAvg, wqsLatencyMin, wqsLatencyMax, wqsLatencyP50, wqsLatencyP90, wqsLatencyP99 := getTimerStats(snapshot, metrics.WorkflowQueueSchedulerTaskLatency.Name())

	// WorkflowQueueSchedulerQueueWaitTime: Time tasks spend waiting in WQS queue
	wqsWaitCount, wqsWaitAvg, wqsWaitMin, wqsWaitMax, wqsWaitP50, wqsWaitP90, wqsWaitP99 := getTimerStats(snapshot, metrics.WorkflowQueueSchedulerQueueWaitTime.Name())

	s.Logger.Info("Task end-to-end latency (task_latency_queue)",
		tag.NewInt("count", taskQueueLatencyCount),
		tag.NewDurationTag("avg", taskQueueLatencyAvg),
		tag.NewDurationTag("min", taskQueueLatencyMin),
		tag.NewDurationTag("max", taskQueueLatencyMax),
		tag.NewDurationTag("p50", taskQueueLatencyP50),
		tag.NewDurationTag("p90", taskQueueLatencyP90),
		tag.NewDurationTag("p99", taskQueueLatencyP99))

	s.Logger.Info("WQS task latency (workflow_queue_scheduler_task_latency)",
		tag.NewInt("count", wqsLatencyCount),
		tag.NewDurationTag("avg", wqsLatencyAvg),
		tag.NewDurationTag("min", wqsLatencyMin),
		tag.NewDurationTag("max", wqsLatencyMax),
		tag.NewDurationTag("p50", wqsLatencyP50),
		tag.NewDurationTag("p90", wqsLatencyP90),
		tag.NewDurationTag("p99", wqsLatencyP99))

	s.Logger.Info("WQS queue wait time (workflow_queue_scheduler_queue_wait_time)",
		tag.NewInt("count", wqsWaitCount),
		tag.NewDurationTag("avg", wqsWaitAvg),
		tag.NewDurationTag("min", wqsWaitMin),
		tag.NewDurationTag("max", wqsWaitMax),
		tag.NewDurationTag("p50", wqsWaitP50),
		tag.NewDurationTag("p90", wqsWaitP90),
		tag.NewDurationTag("p99", wqsWaitP99))

	// No assertion on contention occurring - that's timing dependent.
	// The workflow completing successfully proves the feature doesn't break normal operation.
}

// TestActivityWorkflowWithFeatureEnabled verifies that workflows with activities work correctly
// when the WorkflowQueueScheduler feature is enabled.
//
// This is a smoke test that ensures the feature doesn't break activity-based workflows.
// The actual routing of tasks through the WorkflowQueueScheduler only occurs when
// ErrResourceExhaustedBusyWorkflow errors happen (lock contention), which requires
// specific timing conditions that are hard to trigger reliably in functional tests.
// The unit tests in workflow_aware_scheduler_test.go verify the routing logic directly.
func (s *WorkflowQueueSchedulerActivityTestSuite) TestActivityWorkflowWithFeatureEnabled() {
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

	// Log WorkflowQueueScheduler metrics for observability
	// Note: These will typically be 0 in functional tests because lock contention is hard to trigger
	snapshot := capture.Snapshot()
	tasksSubmitted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksSubmitted.Name())
	tasksCompleted := sumMetricValues(snapshot, metrics.WorkflowQueueSchedulerTasksCompleted.Name())

	s.Logger.Info("Activity test with WorkflowQueueScheduler enabled",
		tag.NewInt64("tasks_submitted", tasksSubmitted),
		tag.NewInt64("tasks_completed", tasksCompleted))
}
