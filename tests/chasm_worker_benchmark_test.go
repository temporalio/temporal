package tests

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
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

// =============================================================================
// Benchmark Configuration - Adjust these constants to change test parameters
// =============================================================================
const (
	// Test timeout
	benchmarkTestTimeout = 5 * time.Minute * debug.TimeoutMultiplier

	// Worker configuration
	numWorkers        = 1000              // Number of workers in each test
	heartbeatInterval = 20 * time.Second  // How often each worker sends a heartbeat
	testDuration      = 120 * time.Second // How long each test runs

	// Activity configuration
	numActivitiesPerWorker = 100 // Number of concurrent activities per worker

	// Churn configuration
	churnInterval = 10 * time.Second // How often to trigger worker restarts
)

// =============================================================================
// Metrics collection
// =============================================================================

// benchMetrics collects latency and throughput metrics for benchmarks.
type benchMetrics struct {
	mu         sync.Mutex
	latencies  []time.Duration
	errors     atomic.Int64
	heartbeats atomic.Int64
}

func (m *benchMetrics) recordLatency(d time.Duration) {
	m.mu.Lock()
	m.latencies = append(m.latencies, d)
	m.mu.Unlock()
	m.heartbeats.Add(1)
}

func (m *benchMetrics) recordError() {
	m.errors.Add(1)
}

func (m *benchMetrics) percentile(p float64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.latencies) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)
	slices.Sort(sorted)
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (m *benchMetrics) avg() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, l := range m.latencies {
		total += l
	}
	return total / time.Duration(len(m.latencies))
}

// =============================================================================
// Churn test configuration
// =============================================================================

// churnConfig defines parameters for churn tests.
type churnConfig struct {
	NumWorkers        int
	ChurnPercent      int           // Percentage of workers to restart each cycle
	ChurnInterval     time.Duration // How often to trigger restarts
	HeartbeatInterval time.Duration
	TestDuration      time.Duration
	NumActivities     int // Number of activities per worker
}

// =============================================================================
// Test Suite
// =============================================================================

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

// createWorkerHeartbeat creates a WorkerHeartbeat with the given parameters.
func (s *CHASMWorkerBenchmarkSuite) createWorkerHeartbeat(workerID, identity string, numActivities int) *workerpb.WorkerHeartbeat {
	return &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: workerID,
		WorkerIdentity:    identity,
		ActivityTaskSlotsInfo: &workerpb.WorkerSlotsInfo{
			CurrentUsedSlots:      int32(numActivities),
			CurrentAvailableSlots: 200, // Assume max capacity
		},
	}
}

// runChurnTest runs a churn simulation with the given configuration.
// It starts numWorkers workers, then every churnInterval restarts churnPercent% of them.
func (s *CHASMWorkerBenchmarkSuite) runChurnTest(cfg churnConfig) (metrics *benchMetrics, workersCreated int64, duration time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), benchmarkTestTimeout)
	defer cancel()

	metrics = &benchMetrics{}
	var created atomic.Int64

	testStart := time.Now()
	testDeadline := testStart.Add(cfg.TestDuration)

	// Each worker has a channel to receive restart signals
	type workerSlot struct {
		restartCh chan struct{}
	}
	slots := make([]*workerSlot, cfg.NumWorkers)
	for i := range slots {
		slots[i] = &workerSlot{restartCh: make(chan struct{}, 1)}
	}

	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Start all workers
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go func(slotNum int) {
			defer wg.Done()
			slot := slots[slotNum]
			identity := fmt.Sprintf("churn-worker-%d", slotNum)

			for {
				created.Add(1)
				workerID := fmt.Sprintf("%s-%s", identity, uuid.New().String())

				// Heartbeat loop until restart signal or test ends
				ticker := time.NewTicker(cfg.HeartbeatInterval)
			heartbeatLoop:
				for {
					// Send heartbeat
					start := time.Now()
					_, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
						Namespace:       s.Namespace().String(),
						WorkerHeartbeat: []*workerpb.WorkerHeartbeat{s.createWorkerHeartbeat(workerID, identity, cfg.NumActivities)},
					})
					if err != nil {
						// Ignore context canceled errors (expected during shutdown)
						if ctx.Err() == nil {
							metrics.recordError()
							if metrics.errors.Load() <= 5 {
								s.T().Logf("Heartbeat error: %v", err)
							}
						}
					} else {
						metrics.recordLatency(time.Since(start))
					}

					select {
					case <-ctx.Done():
						ticker.Stop()
						return
					case <-slot.restartCh:
						ticker.Stop()
						break heartbeatLoop // Restart with new worker
					case <-ticker.C:
						// Continue heartbeat loop
					}
				}

				if time.Now().After(testDeadline) {
					return
				}
			}
		}(i)
	}

	// Churn coordinator: every churnInterval, restart churnPercent% of workers
	go func() {
		ticker := time.NewTicker(cfg.ChurnInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Select random workers to restart (without replacement)
				numToRestart := cfg.NumWorkers * cfg.ChurnPercent / 100
				indices := rng.Perm(cfg.NumWorkers)[:numToRestart]
				for _, idx := range indices {
					select {
					case slots[idx].restartCh <- struct{}{}:
					default: // Already pending restart
					}
				}
			}
		}
	}()

	time.Sleep(cfg.TestDuration)
	cancel()
	wg.Wait()

	return metrics, created.Load(), time.Since(testStart)
}

// logChurnResults logs the results of a churn test.
func (s *CHASMWorkerBenchmarkSuite) logChurnResults(name string, cfg churnConfig, metrics *benchMetrics, workersCreated int64, duration time.Duration) {
	s.T().Logf("%s (workers=%d, activities=%d, churn=%d%% every %v):", name, cfg.NumWorkers, cfg.NumActivities, cfg.ChurnPercent, cfg.ChurnInterval)
	s.T().Logf("  Workers created: %.2f/sec", float64(workersCreated)/duration.Seconds())
	s.T().Logf("  Throughput: %.2f heartbeats/sec", float64(metrics.heartbeats.Load())/duration.Seconds())
	s.T().Logf("  Latency: avg=%v p50=%v p95=%v p99=%v", metrics.avg(), metrics.percentile(0.50), metrics.percentile(0.95), metrics.percentile(0.99))
	s.T().Logf("  Errors: %d", metrics.errors.Load())
}

// =============================================================================
// Tests
// =============================================================================

// TestSteadyState tests steady-state worker heartbeating without churn (0% churn).
func (s *CHASMWorkerBenchmarkSuite) TestSteadyState() {
	cfg := churnConfig{
		NumWorkers:        numWorkers,
		ChurnPercent:      0, // No churn - steady state
		ChurnInterval:     churnInterval,
		HeartbeatInterval: heartbeatInterval,
		TestDuration:      testDuration,
		NumActivities:     numActivitiesPerWorker,
	}
	metrics, workersCreated, duration := s.runChurnTest(cfg)
	s.logChurnResults("Steady State", cfg, metrics, workersCreated, duration)

	s.Equal(int64(numWorkers), workersCreated, "Steady state should create workers only once")
	s.Zero(metrics.errors.Load(), "Should have no errors in steady state")
}

// TestChurn20Percent tests 20% of workers restarting every churn interval (typical rollout).
func (s *CHASMWorkerBenchmarkSuite) TestChurn20Percent() {
	cfg := churnConfig{
		NumWorkers:        numWorkers,
		ChurnPercent:      20,
		ChurnInterval:     churnInterval,
		HeartbeatInterval: heartbeatInterval,
		TestDuration:      testDuration,
		NumActivities:     numActivitiesPerWorker,
	}
	metrics, workersCreated, duration := s.runChurnTest(cfg)
	s.logChurnResults("Churn 20%", cfg, metrics, workersCreated, duration)
}

// TestChurn50Percent tests 50% of workers restarting every churn interval (aggressive rollout).
func (s *CHASMWorkerBenchmarkSuite) TestChurn50Percent() {
	cfg := churnConfig{
		NumWorkers:        numWorkers,
		ChurnPercent:      50,
		ChurnInterval:     churnInterval,
		HeartbeatInterval: heartbeatInterval,
		TestDuration:      testDuration,
		NumActivities:     numActivitiesPerWorker,
	}
	metrics, workersCreated, duration := s.runChurnTest(cfg)
	s.logChurnResults("Churn 50%", cfg, metrics, workersCreated, duration)
}
