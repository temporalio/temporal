package xdc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
)

type (
	replicationSchedulerMetricsTestSuite struct {
		xdcBaseSuite
		namespaceName string
		namespaceID   string
	}
)

func TestReplicationSchedulerMetricsTestSuite(t *testing.T) {
	t.Parallel()
	s := &replicationSchedulerMetricsTestSuite{
		namespaceName: "scheduler-metrics-test-" + common.GenerateRandomString(5),
	}
	suite.Run(t, s)
}

func (s *replicationSchedulerMetricsTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():       true,
		dynamicconfig.EnableReplicationTaskBatching.Key(): false,
	}
	s.logger = log.NewTestLogger()
	s.setupSuite()
}

func (s *replicationSchedulerMetricsTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *replicationSchedulerMetricsTestSuite) SetupTest() {
	s.setupTest()
}

// simpleWorkflow is a minimal workflow that completes immediately.
// This generates replication events with minimal overhead.
func simpleWorkflow(ctx workflow.Context) error {
	return nil
}

func (s *replicationSchedulerMetricsTestSuite) TestSchedulerMetricsUnderLoad() {
	const (
		testDuration       = 2 * time.Minute
		metricsInterval    = 10 * time.Second
		numConcurrentUsers = 5
		taskQueueName      = "scheduler-metrics-test-tq"
	)

	ctx, cancel := context.WithTimeout(context.Background(), testDuration+30*time.Second)
	defer cancel()

	// Create global namespace (handles replication waiting automatically)
	s.namespaceName = s.createGlobalNamespace()

	// Start metrics capture on standby cluster (cluster 1)
	standbyCapture := s.clusters[1].Host().CaptureMetricsHandler()
	s.Require().NotNil(standbyCapture, "standby cluster should have CaptureMetricsHandler enabled")
	capture := standbyCapture.StartCapture()
	defer standbyCapture.StopCapture(capture)

	// Also capture metrics on active cluster for comparison
	activeCapture := s.clusters[0].Host().CaptureMetricsHandler()
	s.Require().NotNil(activeCapture, "active cluster should have CaptureMetricsHandler enabled")
	activeMetrics := activeCapture.StartCapture()
	defer activeCapture.StopCapture(activeMetrics)

	// Create SDK client for active cluster
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: s.namespaceName,
	})
	s.Require().NoError(err)
	defer sdkClient.Close()

	// Start worker on active cluster
	w := worker.New(sdkClient, taskQueueName, worker.Options{})
	w.RegisterWorkflowWithOptions(simpleWorkflow, workflow.RegisterOptions{Name: "simple-workflow"})
	err = w.Start()
	s.Require().NoError(err)
	defer w.Stop()

	// Track workflow stats
	var workflowsStarted atomic.Int64
	var workflowsCompleted atomic.Int64
	var workflowErrors atomic.Int64

	// Create a done channel to signal workflow generators to stop
	done := make(chan struct{})

	// Start concurrent workflow generators
	var wg sync.WaitGroup
	for i := 0; i < numConcurrentUsers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.runWorkflowGenerator(ctx, sdkClient, taskQueueName, workerID, done, &workflowsStarted, &workflowsCompleted, &workflowErrors)
		}(i)
	}

	// Metrics collection loop
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()

	testStart := time.Now()
	iteration := 0

	s.T().Logf("Starting scheduler metrics test - will run for %v with %d concurrent workflow generators", testDuration, numConcurrentUsers)

	for {
		select {
		case <-ctx.Done():
			s.T().Logf("Context done, stopping test")
			close(done)
			wg.Wait()
			return

		case <-ticker.C:
			iteration++
			elapsed := time.Since(testStart)

			// Log workflow progress
			started := workflowsStarted.Load()
			completed := workflowsCompleted.Load()
			errors := workflowErrors.Load()
			s.T().Logf("Iteration %d (elapsed: %v) - Workflows: started=%d, completed=%d, errors=%d",
				iteration, elapsed.Round(time.Second), started, completed, errors)

			// Log standby cluster metrics snapshot
			snapshot := capture.Snapshot()
			s.logSchedulerMetrics("standby", snapshot)

			// Check if we've run long enough
			if elapsed >= testDuration {
				s.T().Logf("Test duration reached, stopping workflow generators")
				close(done)
				wg.Wait()

				// Wait for replication to catch up
				s.T().Logf("Waiting for replication to catch up...")
				s.waitForClusterSynced()

				// Final metrics verification
				finalSnapshot := capture.Snapshot()
				s.T().Logf("\n=== FINAL METRICS VERIFICATION ===")
				s.verifySchedulerMetrics(finalSnapshot)
				return
			}
		}
	}
}

// runWorkflowGenerator continuously starts and waits for workflows until done channel is closed
func (s *replicationSchedulerMetricsTestSuite) runWorkflowGenerator(
	ctx context.Context,
	client sdkclient.Client,
	taskQueue string,
	workerID int,
	done <-chan struct{},
	started *atomic.Int64,
	completed *atomic.Int64,
	errors *atomic.Int64,
) {
	workflowNum := 0
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		default:
			workflowID := common.GenerateRandomString(10)
			workflowNum++

			started.Add(1)

			run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				ID:                 workflowID,
				TaskQueue:          taskQueue,
				WorkflowRunTimeout: 30 * time.Second,
			}, "simple-workflow")

			if err != nil {
				errors.Add(1)
				// Don't log every error to avoid spam, but track them
				if workflowNum%100 == 0 {
					s.T().Logf("Worker %d: workflow start error (count: %d): %v", workerID, errors.Load(), err)
				}
				continue
			}

			// Wait for workflow to complete
			err = run.Get(ctx, nil)
			if err != nil {
				errors.Add(1)
				continue
			}

			completed.Add(1)

			// Small delay to avoid overwhelming the system
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// logSchedulerMetrics logs all scheduler-related metrics from the snapshot
func (s *replicationSchedulerMetricsTestSuite) logSchedulerMetrics(clusterName string, snapshot map[string][]*metricstest.CapturedRecording) {
	schedulerMetrics := []string{
		"scheduler_tasks_submitted",
		"scheduler_tasks_completed",
		"scheduler_queue_latency",
		"scheduler_queue_depth",
		"scheduler_active_queues",
		"scheduler_active_workers",
		"scheduler_assigned_workers",
		"scheduler_busy_workers",
	}

	s.T().Logf("--- %s cluster scheduler metrics ---", clusterName)
	for _, metricName := range schedulerMetrics {
		recordings := snapshot[metricName]
		if len(recordings) == 0 {
			continue
		}

		// Filter for replication queue type
		for _, r := range recordings {
			if r.Tags["queue_type"] == "replication" {
				s.T().Logf("  %s: value=%v tags=%v", metricName, r.Value, r.Tags)
			}
		}
	}
}

// verifySchedulerMetrics asserts that scheduler metrics are being emitted for the replication queue
func (s *replicationSchedulerMetricsTestSuite) verifySchedulerMetrics(snapshot map[string][]*metricstest.CapturedRecording) {
	// Log all metrics for debugging
	s.logSchedulerMetrics("standby-final", snapshot)

	// Collect metrics by name filtered for replication
	metricsFound := make(map[string]bool)
	metricsValues := make(map[string][]interface{})

	schedulerMetrics := []string{
		"scheduler_tasks_submitted",
		"scheduler_tasks_completed",
		"scheduler_queue_latency",
		"scheduler_queue_depth",
		"scheduler_active_queues",
		"scheduler_active_workers",
		"scheduler_assigned_workers",
	}

	for _, metricName := range schedulerMetrics {
		recordings := snapshot[metricName]
		for _, r := range recordings {
			if r.Tags["queue_type"] == "replication" {
				metricsFound[metricName] = true
				metricsValues[metricName] = append(metricsValues[metricName], r.Value)
			}
		}
	}

	// Report what we found
	s.T().Logf("\nReplication scheduler metrics found:")
	for name, found := range metricsFound {
		if found {
			s.T().Logf("  [OK] %s: %v", name, metricsValues[name])
		}
	}

	// Assert key metrics exist
	// Note: We check that metrics are being recorded, but some gauges might be 0 if no tasks are queued
	s.True(metricsFound["scheduler_tasks_submitted"], "scheduler_tasks_submitted should be recorded for replication queue")
	s.True(metricsFound["scheduler_tasks_completed"], "scheduler_tasks_completed should be recorded for replication queue")

	// Verify that tasks were actually processed
	submittedValues := metricsValues["scheduler_tasks_submitted"]
	s.NotEmpty(submittedValues, "should have scheduler_tasks_submitted recordings")

	completedValues := metricsValues["scheduler_tasks_completed"]
	s.NotEmpty(completedValues, "should have scheduler_tasks_completed recordings")

	// Sum up counter values
	var totalSubmitted int64
	for _, v := range submittedValues {
		if intVal, ok := v.(int64); ok {
			totalSubmitted += intVal
		}
	}

	var totalCompleted int64
	for _, v := range completedValues {
		if intVal, ok := v.(int64); ok {
			totalCompleted += intVal
		}
	}

	s.T().Logf("\nTotal replication scheduler tasks: submitted=%d, completed=%d", totalSubmitted, totalCompleted)
	s.Greater(totalSubmitted, int64(0), "should have submitted some replication tasks")
	s.Greater(totalCompleted, int64(0), "should have completed some replication tasks")
}
