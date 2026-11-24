package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
)

type pipelineLatencyClock struct {
	*clock.EventTimeSource
	observations chan time.Time
}

func (c *pipelineLatencyClock) Now() time.Time {
	now := c.EventTimeSource.Now()
	c.observations <- now
	return now
}

func TestPersistencePipelineQueueWaitLatency(t *testing.T) {
	t.Parallel()
	for _, coalescing := range []bool{false, true} {
		name := "individual writes"
		if coalescing {
			name = "coalesced writes"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := &pipelineLatencyClock{clock.NewEventTimeSource(), make(chan time.Time, 16)}
			started, finish := make(chan struct{}, 1), make(chan struct{})
			var calls atomic.Int64
			c := pipelineTestContext(t, coalescing, func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
				if calls.Add(1) == 1 {
					started <- struct{}{}
					<-finish
				} else {
					ts.Advance(10 * time.Millisecond)
				}
				return pipelineTestResponse(0), nil
			})
			c.pipeline.timeSource = ts
			handler := metricstest.NewCaptureHandler()
			c.metricsHandler = handler
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			contexts := make([]context.Context, 3)
			done := make([]chan error, 3)
			for i := range contexts {
				contexts[i] = metrics.AddMetricsContext(t.Context())
				s, err := c.pipeline.submit(contexts[i], 1, pipelineTestRequest(t, int64(10+i)))
				require.NoError(t, err)
				done[i] = make(chan error, 1)
				go func() { done[i] <- c.completeSubmission(s) }()
				if i == 0 {
					await.Rcv(t, started)
				}
				await.Rcv(t, ts.observations)
				if i == 1 {
					ts.Advance(20 * time.Millisecond)
				}
			}
			ts.Advance(60 * time.Millisecond)
			close(finish)
			for _, result := range done {
				require.NoError(t, await.Rcv(t, result))
			}
			lastWait := 70 * time.Millisecond
			if coalescing {
				lastWait = 60 * time.Millisecond
			}
			for i, want := range []time.Duration{0, 80 * time.Millisecond, lastWait} {
				got, _ := metrics.ContextCounterGet(contexts[i], metrics.WorkflowPersistencePipelineWaitLatency.Name())
				require.Equal(t, want, time.Duration(got))
			}
			var total time.Duration
			for _, recording := range capture.SnapshotMetric(metrics.WorkflowPersistencePipelineWaitLatency.Name()) {
				total += recording.Value.(time.Duration)
			}
			require.Equal(t, 80*time.Millisecond+lastWait, total)
		})
	}
}

func TestPersistencePipelineFailedQueueWaitLatency(t *testing.T) {
	t.Parallel()
	for _, cancelPending := range []bool{false, true} {
		name := "predecessor fails"
		if cancelPending {
			name = "pending request canceled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := &pipelineLatencyClock{clock.NewEventTimeSource(), make(chan time.Time, 16)}
			started, finish := make(chan struct{}, 1), make(chan struct{})
			c := pipelineTestContext(t, true, func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
				started <- struct{}{}
				<-finish
				if cancelPending {
					return pipelineTestResponse(0), nil
				}
				return nil, serviceerror.NewUnavailable("write failed")
			})
			c.pipeline.timeSource = ts
			a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
			require.NoError(t, err)
			aDone := make(chan error, 1)
			go func() { aDone <- c.completeSubmission(a) }()
			await.Rcv(t, started)
			await.Rcv(t, ts.observations)
			ctx, cancel := context.WithCancel(metrics.AddMetricsContext(t.Context()))
			defer cancel()
			b, err := c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 11))
			require.NoError(t, err)
			bDone := make(chan error, 1)
			go func() { bDone <- c.completeSubmission(b) }()
			await.Rcv(t, ts.observations)
			ts.Advance(40 * time.Millisecond)
			if cancelPending {
				cancel()
				require.ErrorIs(t, await.Rcv(t, bDone), context.Canceled)
				close(finish)
				require.NoError(t, await.Rcv(t, aDone))
			} else {
				close(finish)
				require.Error(t, await.Rcv(t, aDone))
				require.Error(t, await.Rcv(t, bDone))
			}
			got, ok := metrics.ContextCounterGet(ctx, metrics.WorkflowPersistencePipelineWaitLatency.Name())
			require.True(t, ok)
			require.Equal(t, 40*time.Millisecond, time.Duration(got))
		})
	}
}

func TestPersistencePipelineDependencyWaitLatency(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"success", "failure", "cancellation", "already ready"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			c := pipelineTestContext(t, false, nil)
			ts := &pipelineLatencyClock{clock.NewEventTimeSource(), make(chan time.Time, 16)}
			c.pipeline.timeSource = ts
			ctx, cancel := context.WithCancel(metrics.AddMetricsContext(t.Context()))
			defer cancel()
			dependency := future.NewFuture[struct{}]()
			if outcome == "already ready" {
				dependency.Set(struct{}{}, nil)
			}
			done := make(chan error, 1)
			go func() { done <- c.completeOperation(&operation{ctx: ctx, dependency: dependency}) }()
			if outcome != "already ready" {
				await.Rcv(t, ts.observations)
				ts.Advance(35 * time.Millisecond)
				switch outcome {
				case "success":
					dependency.Set(struct{}{}, nil)
				case "failure":
					dependency.Set(struct{}{}, pipelineDependencyError())
				case "cancellation":
					cancel()
				default:
					t.Fatalf("unexpected outcome: %s", outcome)
				}
			}
			err := await.Rcv(t, done)
			if outcome == "failure" || outcome == "cancellation" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			want := 35 * time.Millisecond
			if outcome == "already ready" {
				want = 0
			}
			got, _ := metrics.ContextCounterGet(ctx, metrics.WorkflowPersistencePipelineWaitLatency.Name())
			require.Equal(t, want, time.Duration(got))
		})
	}
}

func TestPersistencePipelineOwnedWritesKeepPersistenceLatency(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()
	c := pipelineTestContext(t, false, func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		ts.Advance(10 * time.Millisecond)
		return pipelineTestResponse(0), nil
	})
	c.pipeline.timeSource = ts
	ctx := metrics.AddMetricsContext(t.Context())
	op := &operation{ctx: ctx}
	for i := range 3 {
		s, err := c.pipeline.submit(ctx, 1, pipelineTestRequest(t, int64(10+i)))
		require.NoError(t, err)
		op.submissions = append(op.submissions, s)
	}
	require.NoError(t, c.completeOperation(op))
	got, _ := metrics.ContextCounterGet(ctx, metrics.WorkflowPersistencePipelineWaitLatency.Name())
	require.Zero(t, got)
	require.Equal(t, 30*time.Millisecond, ts.Now().Sub(time.Unix(0, 0)))
}

func TestPersistencePipelineFallbackWaitLatency(t *testing.T) {
	t.Parallel()
	ts := &pipelineLatencyClock{clock.NewEventTimeSource(), make(chan time.Time, 16)}
	started, finish := make(chan struct{}, 1), make(chan struct{})
	c := pipelineTestContext(t, false, func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		started <- struct{}{}
		<-finish
		return pipelineTestResponse(0), nil
	})
	c.pipeline.timeSource = ts
	a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	aDone := make(chan error, 1)
	go func() { aDone <- c.completeSubmission(a) }()
	await.Rcv(t, started)
	await.Rcv(t, ts.observations)
	ctx := metrics.AddMetricsContext(t.Context())
	require.NoError(t, c.LockSpeculative(ctx, locks.PriorityHigh))
	done := make(chan error, 1)
	go func() {
		err := c.makeOperationCommitted(ctx, false)
		if unlockErr := c.Unlock(); err == nil {
			err = unlockErr
		}
		done <- err
	}()
	await.Rcv(t, ts.observations)
	ts.Advance(40 * time.Millisecond)
	close(finish)
	require.NoError(t, await.Rcv(t, aDone))
	require.NoError(t, await.Rcv(t, done))
	got, ok := metrics.ContextCounterGet(ctx, metrics.WorkflowPersistencePipelineWaitLatency.Name())
	require.True(t, ok)
	require.Equal(t, 40*time.Millisecond, time.Duration(got))
}
