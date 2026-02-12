package tests

import (
	"context"
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
