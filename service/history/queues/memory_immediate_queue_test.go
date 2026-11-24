package queues

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tasks"
)

type immediateTestLimiter struct{ concurrency dynamicconfig.IntPropertyFn }

func (l immediateTestLimiter) Concurrency() int { return l.concurrency() }
func (immediateTestLimiter) BufferSize() int    { return 0 }

type bufferedImmediateTestLimiter struct {
	immediateTestLimiter
	bufferSize int
}

func (l bufferedImmediateTestLimiter) BufferSize() int { return l.bufferSize }

func TestMemoryImmediateQueueExpiration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		wait    time.Duration
		timeout time.Duration
		wantRun bool
	}{
		{name: "before timeout", wait: 999 * time.Millisecond, timeout: time.Second, wantRun: true},
		{name: "at timeout", wait: time.Second, timeout: time.Second},
		{name: "after timeout", wait: 2 * time.Second, timeout: time.Second},
		{name: "longer task timeout", wait: time.Second, timeout: 2 * time.Second, wantRun: true},
		{name: "no task timeout", wait: 2 * time.Second, wantRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := ctasks.NewDynamicWorkerPoolScheduler(bufferedImmediateTestLimiter{
				immediateTestLimiter: immediateTestLimiter{func() int { return 1 }}, bufferSize: 2,
			}, metrics.NoopMetricsHandler)
			defer pool.WaitShutdown()
			defer pool.InitiateShutdown()
			timeSource := clock.NewEventTimeSource()
			q := NewMemoryImmediateQueue(pool, timeSource)
			defer q.Stop()
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			q.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(ctx context.Context) {
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
			}}})
			await.Rcv(t, started)
			ran := make(chan struct{}, 1)
			q.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{
				QueueTimeout: tc.timeout,
				Callback:     func(context.Context) { ran <- struct{}{} },
			}})
			drained := make(chan struct{}, 1)
			q.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(context.Context) { drained <- struct{}{} }}})
			timeSource.Advance(tc.wait)
			close(release)
			await.Rcv(t, drained)
			require.Equal(t, tc.wantRun, len(ran) == 1)
		})
	}
}

func TestMemoryImmediateQueueAdmissionAndShutdown(t *testing.T) {
	t.Parallel()
	pool := ctasks.NewDynamicWorkerPoolScheduler(immediateTestLimiter{func() int { return 1 }}, metrics.NoopMetricsHandler)
	defer pool.WaitShutdown()
	defer pool.InitiateShutdown()
	first := NewMemoryImmediateQueue(pool, clock.NewRealTimeSource())
	second := NewMemoryImmediateQueue(pool, clock.NewRealTimeSource())
	defer second.Stop()
	first.Start()
	unexpected := make(chan struct{}, 2)
	started := make(chan struct{}, 1)
	finished := make(chan error, 1)
	first.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(ctx context.Context) {
		started <- struct{}{}
		<-ctx.Done()
		finished <- ctx.Err()
	}}})
	await.Rcv(t, started)
	// The pool is shared across shards. Admission returns immediately and drops the second task.
	second.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(context.Context) { unexpected <- struct{}{} }}})
	first.Stop()
	require.ErrorIs(t, await.Rcv(t, finished), context.Canceled)
	// Stopping a shard prevents later execution even after host capacity becomes available.
	first.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(context.Context) { unexpected <- struct{}{} }}})
	pool.InitiateShutdown()
	pool.WaitShutdown()
	require.Empty(t, unexpected)
	require.Equal(t, tasks.CategoryMemoryImmediate, first.Category())
}

func TestMemoryImmediateQueueHostShutdown(t *testing.T) {
	t.Parallel()
	pool := ctasks.NewDynamicWorkerPoolScheduler(immediateTestLimiter{func() int { return 1 }}, metrics.NoopMetricsHandler)
	defer pool.WaitShutdown()
	defer pool.InitiateShutdown()
	q := NewMemoryImmediateQueue(pool, clock.NewRealTimeSource())
	defer q.Stop()
	unexpected := make(chan struct{}, 2)
	started := make(chan struct{}, 1)
	finished := make(chan error, 1)
	q.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(ctx context.Context) {
		started <- struct{}{}
		<-ctx.Done()
		finished <- ctx.Err()
	}}})
	await.Rcv(t, started)
	pool.InitiateShutdown()
	require.ErrorIs(t, await.Rcv(t, finished), context.Canceled)
	pool.WaitShutdown()
	q.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(context.Context) { unexpected <- struct{}{} }}})
	require.Empty(t, unexpected)
}

func TestMemoryImmediateQueueZeroCapacity(t *testing.T) {
	t.Parallel()
	pool := ctasks.NewDynamicWorkerPoolScheduler(immediateTestLimiter{func() int { return 0 }}, metrics.NoopMetricsHandler)
	defer pool.WaitShutdown()
	defer pool.InitiateShutdown()
	q := NewMemoryImmediateQueue(pool, clock.NewRealTimeSource())
	defer q.Stop()
	unexpected := make(chan struct{}, 1)
	q.NotifyNewTasks([]tasks.Task{&tasks.MemoryImmediateTask{Callback: func(context.Context) { unexpected <- struct{}{} }}})
	pool.InitiateShutdown()
	pool.WaitShutdown()
	require.Empty(t, unexpected)
}
