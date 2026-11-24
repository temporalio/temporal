package queues

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

// BenchmarkMemoryImmediateDispatch simulates blocking dispatch, without RPC or worker execution.
// Run with -run '^$' -bench '^BenchmarkMemoryImmediateDispatch$' -benchtime=3000x -count=1.
// At 300 ms per dispatch, sustaining N dispatches/s requires at least 0.3*N concurrent goroutines.
// A zero-buffer pool needs headroom above that minimum to absorb bursts and scheduling delays.
func BenchmarkMemoryImmediateDispatch(b *testing.B) {
	const offeredRate = 1000
	const latency = 300 * time.Millisecond
	for _, concurrency := range []int{64, 128, 256, 512} {
		b.Run(fmt.Sprintf("workers=%d/offered=%d", concurrency, offeredRate), func(b *testing.B) {
			pool := ctasks.NewDynamicWorkerPoolScheduler(immediateTestLimiter{func() int { return concurrency }}, metrics.NoopMetricsHandler)
			var completed sync.WaitGroup
			task := &tasks.MemoryImmediateTask{Callback: func(ctx context.Context) {
				defer completed.Done()
				timer := time.NewTimer(latency)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
				}
			}}
			accepted := 0
			b.ReportAllocs()
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				if delay := time.Until(start.Add(time.Duration(i) * time.Second / offeredRate)); delay > 0 {
					time.Sleep(delay) //nolint:forbidigo // Pace offered traffic; there is no condition to poll.
				}
				completed.Add(1)
				if pool.TrySubmit(task) {
					accepted++
				} else {
					completed.Done()
				}
			}
			submissionDuration := time.Since(start)
			completed.Wait()
			totalDuration := time.Since(start)
			b.StopTimer()
			pool.InitiateShutdown()
			pool.WaitShutdown()
			b.ReportMetric(float64(accepted)/totalDuration.Seconds(), "completed/s")
			b.ReportMetric(float64(accepted)/submissionDuration.Seconds(), "admitted/s")
			b.ReportMetric(100*float64(b.N-accepted)/float64(b.N), "drop_pct")
		})
	}
}
