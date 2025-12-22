package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

const (
	benchmarkTestTimeout = 5 * time.Minute * debug.TimeoutMultiplier
)

type CHASMWorkerBenchmarkSuite struct {
	testcore.FunctionalTestBase
}

func TestCHASMWorkerBenchmarkSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CHASMWorkerBenchmarkSuite))
}

func (s *CHASMWorkerBenchmarkSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():               true,
			dynamicconfig.EnableWorkerStateTracking.Key(): true, // Store heartbeats in CHASM (persisted to DB)
		}),
	)
}

// startWorker creates a new worker and sends heartbeats until deadline.
// Returns true if context was cancelled (caller should exit).
func (s *CHASMWorkerBenchmarkSuite) startWorker(
	ctx context.Context,
	identity string,
	heartbeatInterval time.Duration,
	deadline time.Time,
	workersCreated *atomic.Int64,
	heartbeatsSent *atomic.Int64,
	errors *atomic.Int64,
) bool {
	workerID := fmt.Sprintf("%s-%s", identity, uuid.New().String())
	workersCreated.Add(1)

	// Register worker
	_, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: workerID,
				WorkerIdentity:    identity,
			},
		},
	})
	if err != nil {
		errors.Add(1)
		return ctx.Err() != nil
	}
	heartbeatsSent.Add(1)

	// Heartbeat until deadline
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return true
		case <-ticker.C:
			_, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
				Namespace: s.Namespace().String(),
				WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
					{
						WorkerInstanceKey: workerID,
						WorkerIdentity:    identity,
					},
				},
			})
			if err != nil {
				errors.Add(1)
			} else {
				heartbeatsSent.Add(1)
			}
		}
	}
	return false
}

// TestSteadyState tests steady-state worker heartbeating without churn.
func (s *CHASMWorkerBenchmarkSuite) TestSteadyState() {
	ctx, cancel := context.WithTimeout(context.Background(), benchmarkTestTimeout)
	defer cancel()

	const (
		numWorkers        = 100
		heartbeatInterval = 20 * time.Second
		testDuration      = 60 * time.Second
	)

	var (
		workersCreated atomic.Int64
		heartbeatsSent atomic.Int64
		errors         atomic.Int64
	)

	var wg sync.WaitGroup
	testStart := time.Now()
	testDeadline := testStart.Add(testDuration)

	// Start all workers (they run for the entire test duration)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			identity := fmt.Sprintf("steady-worker-%d", workerNum)
			s.startWorker(ctx, identity, heartbeatInterval, testDeadline, &workersCreated, &heartbeatsSent, &errors)
		}(i)
	}

	time.Sleep(testDuration)
	cancel()
	wg.Wait()

	// Results
	duration := time.Since(testStart)
	s.T().Logf("Steady State Results:")
	s.T().Logf("  Workers: %d", numWorkers)
	s.T().Logf("  Duration: %v", duration)
	s.T().Logf("  Heartbeats sent: %d (%.2f/sec)", heartbeatsSent.Load(), float64(heartbeatsSent.Load())/duration.Seconds())
	s.T().Logf("  Errors: %d", errors.Load())

	// Pause to allow manual inspection of database state
	s.T().Logf("Pausing for 2 minutes for database inspection...")
	time.Sleep(2 * time.Minute)

	s.Zero(errors.Load(), "Should have no errors in steady state")
}

// TestWorkerChurn tests worker registration under churn (periodic restarts).
func (s *CHASMWorkerBenchmarkSuite) TestWorkerChurn() {
	ctx, cancel := context.WithTimeout(context.Background(), benchmarkTestTimeout)
	defer cancel()

	const (
		numWorkerSlots    = 50
		restartInterval   = 10 * time.Second
		heartbeatInterval = 20 * time.Second
		testDuration      = 120 * time.Second
	)

	var (
		workersCreated atomic.Int64
		heartbeatsSent atomic.Int64
		errors         atomic.Int64
	)

	var wg sync.WaitGroup
	testStart := time.Now()
	testDeadline := testStart.Add(testDuration)

	// Each slot repeatedly starts workers that "crash" after restartInterval
	for slot := 0; slot < numWorkerSlots; slot++ {
		wg.Add(1)
		go func(slotNum int) {
			defer wg.Done()
			identity := fmt.Sprintf("churn-worker-slot-%d", slotNum)

			for time.Now().Before(testDeadline) {
				crashTime := time.Now().Add(restartInterval)
				if s.startWorker(ctx, identity, heartbeatInterval, crashTime, &workersCreated, &heartbeatsSent, &errors) {
					return // context cancelled
				}
				// Worker "crashed" - loop continues with new worker
			}
		}(slot)
	}

	time.Sleep(testDuration)
	cancel()
	wg.Wait()

	// Results
	duration := time.Since(testStart)
	s.T().Logf("Worker Churn Results:")
	s.T().Logf("  Worker slots: %d", numWorkerSlots)
	s.T().Logf("  Restart interval: %v", restartInterval)
	s.T().Logf("  Duration: %v", duration)
	s.T().Logf("  Workers created: %d (%.2f/sec)", workersCreated.Load(), float64(workersCreated.Load())/duration.Seconds())
	s.T().Logf("  Heartbeats sent: %d (%.2f/sec)", heartbeatsSent.Load(), float64(heartbeatsSent.Load())/duration.Seconds())
	s.T().Logf("  Errors: %d", errors.Load())
}

// TestHighActivityChurn tests worker churn with high activity count simulation.
func (s *CHASMWorkerBenchmarkSuite) TestHighActivityChurn() {
	ctx, cancel := context.WithTimeout(context.Background(), benchmarkTestTimeout)
	defer cancel()

	const (
		numWorkerSlots      = 100
		restartInterval     = 10 * time.Second
		heartbeatInterval   = 20 * time.Second
		testDuration        = 120 * time.Second
		activitiesPerWorker = 100 // For projection only
	)

	var (
		workersCreated atomic.Int64
		heartbeatsSent atomic.Int64
		errors         atomic.Int64
	)

	var wg sync.WaitGroup
	testStart := time.Now()
	testDeadline := testStart.Add(testDuration)

	for slot := 0; slot < numWorkerSlots; slot++ {
		wg.Add(1)
		go func(slotNum int) {
			defer wg.Done()
			identity := fmt.Sprintf("high-activity-worker-slot-%d", slotNum)

			for time.Now().Before(testDeadline) {
				crashTime := time.Now().Add(restartInterval)
				if s.startWorker(ctx, identity, heartbeatInterval, crashTime, &workersCreated, &heartbeatsSent, &errors) {
					return
				}
			}
		}(slot)
	}

	time.Sleep(testDuration)
	cancel()
	wg.Wait()

	// Results
	duration := time.Since(testStart)
	createsPerSec := float64(workersCreated.Load()) / duration.Seconds()

	s.T().Logf("High Activity Churn Results:")
	s.T().Logf("  Worker slots: %d", numWorkerSlots)
	s.T().Logf("  Activities per worker: %d", activitiesPerWorker)
	s.T().Logf("  Restart interval: %v", restartInterval)
	s.T().Logf("  Duration: %v", duration)
	s.T().Logf("  Workers created: %d (%.2f/sec)", workersCreated.Load(), createsPerSec)
	s.T().Logf("  Heartbeats sent: %d (%.2f/sec)", heartbeatsSent.Load(), float64(heartbeatsSent.Load())/duration.Seconds())
	s.T().Logf("  Errors: %d", errors.Load())
	s.T().Logf("  ---")
	s.T().Logf("  Projected activity reschedules: %d (%.2f/sec)", workersCreated.Load()*activitiesPerWorker, createsPerSec*activitiesPerWorker)
}
