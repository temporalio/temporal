package tests

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
)

//parallelize:ignore
func TestWorkflowPersistencePipeline(t *testing.T) {
	type workload struct{ signals, concurrency int }
	var workloads []workload
	repeats := 5
	switch os.Getenv("TEMPORAL_PIPELINE_PERF") {
	case "1":
		workloads = []workload{{100, 1}, {100, 8}, {100, 32}, {1000, 1}, {1000, 8}, {1000, 32}}
	case "high":
		workloads = []workload{{1000, 64}, {1000, 128}, {1000, 256}, {1000, 512}, {5000, 64}, {5000, 128}, {5000, 256}, {5000, 512}}
	case "highest":
		workloads = []workload{{5000, 512}}
		repeats = 1
	default:
		workloads = []workload{{100, 32}}
		repeats = 1
	}
	for _, mode := range []struct {
		name                 string
		pipeline, coalescing bool
		maxInFlight          int
	}{
		{"off", false, false, 1}, {"pipeline", true, false, 1}, {"coalescing", true, true, 1},
		{"inflight", true, false, 4}, {"inflight-coalescing", true, true, 4},
	} {
		t.Run(mode.name, func(t *testing.T) {
			env := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.WorkflowPersistencePipeline, mode.pipeline),
				testcore.WithDynamicConfig(dynamicconfig.WorkflowPersistenceCoalescing, mode.coalescing),
				testcore.WithDynamicConfig(dynamicconfig.WorkflowPersistencePipelineMaxInFlight, mode.maxInFlight),
				testcore.WithDynamicConfig(dynamicconfig.FrontendRPS, 100000),
				testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 100000),
				testcore.WithDynamicConfig(dynamicconfig.HistoryRPS, 100000),
				testcore.WithDynamicConfig(dynamicconfig.HistoryPersistenceMaxQPS, 100000),
			)
			metricsHandler := env.GetTestCluster().Host().CaptureMetricsHandler() //nolint:staticcheck // Per-run filtered capture bounds retained metrics on this dedicated cluster.
			require.NotNil(t, metricsHandler)
			sawBatch := false
			ctx := testcontext.For(t)
			cluster, err := env.AdminClient().DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
			require.NoError(t, err)
			if testcore.UseCassandraPersistence() {
				require.Equal(t, "cassandra", cluster.GetPersistenceStore())
			}
			t.Logf("PIPELINE_BACKEND persistence=%s visibility=%s", cluster.GetPersistenceStore(), cluster.GetVisibilityStore())
			workflowFn := func(ctx workflow.Context, expected int) ([]int, error) {
				if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return true, nil }); err != nil {
					return nil, err
				}
				channel := workflow.GetSignalChannel(ctx, "pipeline")
				result := make([]int, 0, expected)
				for len(result) < expected {
					var id int
					channel.Receive(ctx, &id)
					result = append(result, id)
				}
				slices.Sort(result)
				return result, nil
			}
			env.SdkWorker().RegisterWorkflow(workflowFn)
			for _, work := range workloads {
				for repeat := -1; repeat < repeats; repeat++ {
					id := fmt.Sprintf("%s-%d-%d-%d", env.Tv().WorkflowID(), work.signals, work.concurrency, repeat)
					run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{ID: id, TaskQueue: env.WorkerTaskQueue()}, workflowFn, work.signals)
					require.NoError(t, err)
					await.Require(ctx, t, func(a *await.T) {
						value, err := env.SdkClient().QueryWorkflow(a.Context(), id, run.GetRunID(), "ready")
						require.NoError(a, err)
						var ready bool
						require.NoError(a, value.Get(&ready))
						require.True(a, ready)
					}, 10*time.Second, 10*time.Millisecond)
					capture := metricsHandler.StartCaptureWithFilter(pipelinePerformanceMetric)
					latencies := make([]time.Duration, work.signals)
					errors := make(chan error, work.signals)
					jobs := make(chan int, work.signals)
					for n := range work.signals {
						jobs <- n
					}
					close(jobs)
					var senders sync.WaitGroup
					var active, peak atomic.Int64
					startSending := make(chan struct{})
					for range work.concurrency {
						senders.Go(func() {
							<-startSending
							for n := range jobs {
								current := active.Add(1)
								for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
								}
								begin := time.Now()
								err := env.SdkClient().SignalWorkflow(ctx, id, run.GetRunID(), "pipeline", n)
								latencies[n] = time.Since(begin)
								active.Add(-1)
								errors <- err
							}
						})
					}
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					start := time.Now()
					close(startSending)
					senders.Wait()
					close(errors)
					var firstError error
					errorCount := 0
					for err := range errors {
						if err != nil {
							errorCount++
							if firstError == nil {
								firstError = err
							}
						}
					}
					if firstError != nil {
						metricsHandler.StopCapture(capture)
						t.Logf("PIPELINE_FAILURE mode=%s signals=%d concurrency=%d repeat=%d failure_elapsed_ms=%.3f final_signal_errors=%d first_error=%q physical_writes=%d frontend_attempts=%d history_attempts=%d frontend_errors=%d history_errors=%d frontend_throttles=%d history_throttles=%d", mode.name, work.signals, work.concurrency, repeat, float64(time.Since(start))/float64(time.Millisecond), errorCount, firstError.Error(), pipelinePhysicalWrites(capture, env.Namespace().String()), pipelineSignalMetric(capture, metrics.ServiceRequests.Name(), "frontend"), pipelineSignalMetric(capture, metrics.ServiceRequests.Name(), "history"), pipelineSignalMetric(capture, metrics.ServiceErrorWithType.Name(), "frontend"), pipelineSignalMetric(capture, metrics.ServiceErrorWithType.Name(), "history"), pipelineSignalMetric(capture, metrics.ServiceErrResourceExhaustedCounter.Name(), "frontend"), pipelineSignalMetric(capture, metrics.ServiceErrResourceExhaustedCounter.Name(), "history"))
					}
					require.NoError(t, firstError)
					var result []int
					require.NoError(t, run.Get(ctx, &result))
					elapsed := time.Since(start)
					runtime.ReadMemStats(&after)
					metricsHandler.StopCapture(capture)
					want := make([]int, work.signals)
					for n := range want {
						want[n] = n
					}
					require.Equal(t, want, result)
					iterator := env.SdkClient().GetWorkflowHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
					signals := 0
					for eventID := int64(1); iterator.HasNext(); eventID++ {
						event, err := iterator.Next()
						require.NoError(t, err)
						require.Equal(t, eventID, event.EventId)
						if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
							signals++
						}
					}
					require.Equal(t, work.signals, signals)
					state, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
						Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run.GetRunID()}, Archetype: chasm.WorkflowArchetype,
					})
					require.NoError(t, err)
					cached, persisted := state.GetCacheMutableState().GetExecutionInfo(), state.GetDatabaseMutableState().GetExecutionInfo()
					require.Positive(t, persisted.GetExecutionStats().GetHistorySize())
					require.Equal(t, persisted.GetExecutionStats().GetHistorySize(), cached.GetExecutionStats().GetHistorySize())
					require.Positive(t, persisted.GetCloseTransferTaskId())
					require.Equal(t, persisted.GetCloseTransferTaskId(), cached.GetCloseTransferTaskId())
					require.Positive(t, persisted.GetCloseVisibilityTaskId())
					require.Equal(t, persisted.GetCloseVisibilityTaskId(), cached.GetCloseVisibilityTaskId())
					batches := capture.SnapshotMetric("workflow_persistence_pipeline_batch_size")
					logical, maxBatch := int64(0), int64(0)
					for _, batch := range batches {
						value := batch.Value.(int64)
						logical += value
						maxBatch = max(maxBatch, value)
					}
					sawBatch = sawBatch || maxBatch > 1
					if mode.pipeline {
						require.Equal(t, int64(work.signals), logical)
					} else {
						require.Empty(t, batches)
					}
					slices.Sort(latencies)
					if repeat >= 0 {
						t.Logf("PIPELINE_RESULT mode=%s signals=%d concurrency=%d repeat=%d completion_ms=%.3f rpc_p50_ms=%.3f rpc_p95_ms=%.3f throughput=%.3f physical_writes=%d pipeline_writes=%d logical_submissions=%d max_batch=%d peak_senders=%d frontend_attempts=%d history_attempts=%d frontend_errors=%d history_errors=%d frontend_throttles=%d history_throttles=%d final_signal_errors=0 allocated_mb=%.3f heap_mb=%.3f gc_cycles=%d gc_pause_ms=%.3f", mode.name, work.signals, work.concurrency, repeat, float64(elapsed)/float64(time.Millisecond), float64(latencies[len(latencies)/2])/float64(time.Millisecond), float64(latencies[(len(latencies)*95)/100])/float64(time.Millisecond), float64(work.signals)/elapsed.Seconds(), pipelinePhysicalWrites(capture, env.Namespace().String()), len(batches), logical, maxBatch, peak.Load(), pipelineSignalMetric(capture, metrics.ServiceRequests.Name(), "frontend"), pipelineSignalMetric(capture, metrics.ServiceRequests.Name(), "history"), pipelineSignalMetric(capture, metrics.ServiceErrorWithType.Name(), "frontend"), pipelineSignalMetric(capture, metrics.ServiceErrorWithType.Name(), "history"), pipelineSignalMetric(capture, metrics.ServiceErrResourceExhaustedCounter.Name(), "frontend"), pipelineSignalMetric(capture, metrics.ServiceErrResourceExhaustedCounter.Name(), "history"), float64(after.TotalAlloc-before.TotalAlloc)/(1<<20), float64(after.HeapAlloc)/(1<<20), after.NumGC-before.NumGC, float64(after.PauseTotalNs-before.PauseTotalNs)/float64(time.Millisecond))
					}
				}
			}
			if mode.coalescing {
				require.True(t, sawBatch)
			}
		})
	}
}

func pipelinePerformanceMetric(name string, recording *metricstest.CapturedRecording) bool {
	switch name {
	case "workflow_persistence_pipeline_batch_size":
		return true
	case metrics.PersistenceRequests.Name():
		return recording.Tags["operation"] == metrics.PersistenceUpdateWorkflowExecutionScope
	case metrics.ServiceRequests.Name(), metrics.ServiceErrorWithType.Name(), metrics.ServiceErrResourceExhaustedCounter.Name():
		return recording.Tags["operation"] == "SignalWorkflowExecution"
	default:
		return false
	}
}

func pipelineSignalMetric(capture *metricstest.Capture, name, service string) int64 {
	var count int64
	for _, recording := range capture.SnapshotMetric(name) {
		if recording.Tags["service_name"] == service {
			count += recording.Value.(int64)
		}
	}
	return count
}

func pipelinePhysicalWrites(capture *metricstest.Capture, namespace string) int64 {
	var count int64
	for _, recording := range capture.SnapshotMetric(metrics.PersistenceRequests.Name()) {
		if recording.Tags["operation"] == metrics.PersistenceUpdateWorkflowExecutionScope && recording.Tags["namespace"] == namespace {
			count += recording.Value.(int64)
		}
	}
	return count
}
