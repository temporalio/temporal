package workflowresend

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
)

const testJobTimeout = time.Minute

type panicDeadlineContext struct {
	context.Context
}

func (panicDeadlineContext) Deadline() (time.Time, bool) {
	panic("test panic")
}

func testWorkflowKey(workflowID string) definition.WorkflowKey {
	return definition.NewWorkflowKey("namespace", workflowID, "run")
}

func TestSubmitResultZeroValueFailsSafe(t *testing.T) {
	var result SubmitResult
	require.Equal(t, SubmitResultFailed, result)
}

func TestHostSchedulerDeduplicatesAndReleasesWorkflow(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)
	scheduler := NewHostScheduler(func() int { return 2 }, log.NewNoopLogger(), metricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		testJobTimeout,
		func(ctx context.Context) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			close(finished)
		},
	))
	requireSignal(t, started)

	var duplicateRuns atomic.Int32
	require.Equal(t, SubmitResultDuplicate, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		testJobTimeout,
		func(context.Context) { duplicateRuns.Add(1) },
	))
	require.Equal(t, SubmitResultDuplicate, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		testJobTimeout,
		func(context.Context) { duplicateRuns.Add(1) },
	))
	require.Zero(t, duplicateRuns.Load())
	require.Empty(t, capture.Snapshot()[metrics.WorkflowResendSchedulerAtCapacity.Name()])

	differentWorkflowRan := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("b"),
		testJobTimeout,
		func(context.Context) { close(differentWorkflowRan) },
	))
	requireSignal(t, differentWorkflowRan)

	close(release)
	requireSignal(t, finished)
	resubmittedWorkflowRan := make(chan struct{})
	await.RequireTrue(t, func() bool {
		return scheduler.TrySubmit(
			t.Context(),
			testWorkflowKey("a"),
			testJobTimeout,
			func(context.Context) { close(resubmittedWorkflowRan) },
		) == SubmitResultAccepted
	}, 5*time.Second, 10*time.Millisecond)
	requireSignal(t, resubmittedWorkflowRan)
	require.Zero(t, duplicateRuns.Load())
}

func TestHostSchedulerSharedConcurrencyRejectsAndReleasesWorkflow(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		testJobTimeout,
		func(ctx context.Context) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			close(finished)
		},
	))
	requireSignal(t, started)

	var rejectedRuns atomic.Int32
	require.Equal(t, SubmitResultAtCapacity, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("b"),
		testJobTimeout,
		func(context.Context) { rejectedRuns.Add(1) },
	))
	require.Zero(t, rejectedRuns.Load())
	require.Len(t, capture.Snapshot()[metrics.WorkflowResendSchedulerAtCapacity.Name()], 1)
	require.Empty(t, capture.Snapshot()[metrics.DynamicWorkerPoolSchedulerRejectedTasks.Name()])

	close(release)
	requireSignal(t, finished)
	resubmittedWorkflowRan := make(chan struct{})
	await.RequireTrue(t, func() bool {
		return scheduler.TrySubmit(
			t.Context(),
			testWorkflowKey("b"),
			testJobTimeout,
			func(context.Context) { close(resubmittedWorkflowRan) },
		) == SubmitResultAccepted
	}, 5*time.Second, 10*time.Millisecond)
	requireSignal(t, resubmittedWorkflowRan)
}

func TestHostSchedulerShutdownCancelsAndCleansUp(t *testing.T) {
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metrics.NoopMetricsHandler)

	started := make(chan struct{})
	finished := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		testJobTimeout,
		func(ctx context.Context) {
			close(started)
			<-ctx.Done()
			close(finished)
		},
	))
	requireSignal(t, started)

	scheduler.InitiateShutdown()
	scheduler.WaitShutdown()
	requireSignal(t, finished)
	require.Empty(t, scheduler.inFlight)
}

func TestHostSchedulerCallerCancellationReachesRunningJob(t *testing.T) {
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	jobCtx, cancelJob := context.WithCancel(t.Context())
	started := make(chan struct{})
	finished := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		jobCtx,
		testWorkflowKey("a"),
		testJobTimeout,
		func(ctx context.Context) {
			close(started)
			<-ctx.Done()
			close(finished)
		},
	))
	requireSignal(t, started)

	cancelJob()
	requireSignal(t, finished)
}

func TestHostSchedulerAbortCleansUp(t *testing.T) {
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	scheduler.InitiateShutdown()

	var runs atomic.Int32
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		testJobTimeout,
		func(context.Context) { runs.Add(1) },
	))
	scheduler.WaitShutdown()

	require.Zero(t, runs.Load())
	require.Empty(t, scheduler.inFlight)
}

func TestHostSchedulerTimeoutReleasesWorkflow(t *testing.T) {
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	finished := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		testWorkflowKey("a"),
		10*time.Millisecond,
		func(ctx context.Context) {
			<-ctx.Done()
			close(finished)
		},
	))
	requireSignal(t, finished)

	resubmittedWorkflowRan := make(chan struct{})
	await.RequireTrue(t, func() bool {
		return scheduler.TrySubmit(
			t.Context(),
			testWorkflowKey("a"),
			testJobTimeout,
			func(context.Context) { close(resubmittedWorkflowRan) },
		) == SubmitResultAccepted
	}, 5*time.Second, 10*time.Millisecond)
	requireSignal(t, resubmittedWorkflowRan)
}

func TestHostSchedulerTaskPanicFailsOpen(t *testing.T) {
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	key := testWorkflowKey("a")
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		key,
		testJobTimeout,
		func(context.Context) { panic("test panic") },
	))

	resubmittedWorkflowRan := make(chan struct{})
	await.RequireTrue(t, func() bool {
		return scheduler.TrySubmit(
			t.Context(),
			key,
			testJobTimeout,
			func(context.Context) { close(resubmittedWorkflowRan) },
		) == SubmitResultAccepted
	}, 5*time.Second, 10*time.Millisecond)
	requireSignal(t, resubmittedWorkflowRan)
}

func TestHostSchedulerLimitProviderPanicFailsOpen(t *testing.T) {
	var panicLimit atomic.Bool
	panicLimit.Store(true)
	scheduler := NewHostScheduler(func() int {
		if panicLimit.Swap(false) {
			panic("test panic")
		}
		return 1
	}, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	key := testWorkflowKey("a")
	require.Equal(t, SubmitResultAtCapacity, scheduler.TrySubmit(
		t.Context(),
		key,
		testJobTimeout,
		func(context.Context) { require.FailNow(t, "rejected job ran") },
	))

	resubmittedWorkflowRan := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		key,
		testJobTimeout,
		func(context.Context) { close(resubmittedWorkflowRan) },
	))
	requireSignal(t, resubmittedWorkflowRan)
}

func TestHostSchedulerSubmitPanicFailsOpen(t *testing.T) {
	scheduler := NewHostScheduler(func() int { return 1 }, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	t.Cleanup(func() {
		scheduler.InitiateShutdown()
		scheduler.WaitShutdown()
	})

	key := testWorkflowKey("a")
	require.Equal(t, SubmitResultFailed, scheduler.TrySubmit(
		panicDeadlineContext{Context: t.Context()},
		key,
		testJobTimeout,
		func(context.Context) { require.FailNow(t, "failed job ran") },
	))

	resubmittedWorkflowRan := make(chan struct{})
	require.Equal(t, SubmitResultAccepted, scheduler.TrySubmit(
		t.Context(),
		key,
		testJobTimeout,
		func(context.Context) { close(resubmittedWorkflowRan) },
	))
	requireSignal(t, resubmittedWorkflowRan)
}

func TestResendRunnableDoesNotRunAfterShutdown(t *testing.T) {
	shutdownCtx, cancel := context.WithCancel(t.Context())
	cancel()

	var runs atomic.Int32
	var cleanups atomic.Int32
	runnable := &resendRunnable{
		ctx:     t.Context(),
		run:     func(context.Context) { runs.Add(1) },
		logger:  log.NewNoopLogger(),
		cleanup: func() { cleanups.Add(1) },
	}
	runnable.Run(shutdownCtx)

	require.Zero(t, runs.Load())
	require.Equal(t, int32(1), cleanups.Load())
}

func requireSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for signal")
	}
}
