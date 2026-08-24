package workflowresend

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

const OperationName = "WorkflowResend"

// SubmitResult describes the outcome of scheduler admission.
type SubmitResult int

const (
	SubmitResultFailed     SubmitResult = 0
	SubmitResultAccepted   SubmitResult = 1
	SubmitResultDuplicate  SubmitResult = 2
	SubmitResultAtCapacity SubmitResult = 3
)

// Scheduler runs workflow resend jobs asynchronously.
type Scheduler interface {
	// TrySubmit reports whether the job was accepted, deduplicated, rejected at capacity, or failed
	// during submission.
	TrySubmit(
		ctx context.Context,
		key definition.WorkflowKey,
		timeout time.Duration,
		run func(context.Context),
	) SubmitResult
}

// HostScheduler deduplicates workflow resends and bounds their concurrency across a history host.
type HostScheduler struct {
	pool *ctasks.DynamicWorkerPoolScheduler

	logger         log.Logger
	metricsHandler metrics.Handler

	mu       sync.Mutex
	inFlight map[definition.WorkflowKey]struct{}
}

var _ Scheduler = (*HostScheduler)(nil)

// NewHostScheduler creates a host-level scheduler with no task buffer.
func NewHostScheduler(
	maxConcurrency dynamicconfig.IntPropertyFn,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *HostScheduler {
	limiter := hostSchedulerLimiter{
		maxConcurrency: maxConcurrency,
		logger:         logger,
	}
	return &HostScheduler{
		pool: ctasks.NewDynamicWorkerPoolScheduler(
			limiter,
			metrics.NoopMetricsHandler,
		),
		logger:         logger,
		metricsHandler: metricsHandler,
		inFlight:       make(map[definition.WorkflowKey]struct{}),
	}
}

func (s *HostScheduler) TrySubmit(
	ctx context.Context,
	key definition.WorkflowKey,
	timeout time.Duration,
	run func(context.Context),
) (result SubmitResult) {
	var panicErr error
	defer log.CapturePanic(log.With(s.logger, workflowKeyTags(key)...), &panicErr)
	return s.trySubmit(ctx, key, timeout, run)
}

func (s *HostScheduler) trySubmit(
	ctx context.Context,
	key definition.WorkflowKey,
	timeout time.Duration,
	run func(context.Context),
) SubmitResult {
	if !s.tryClaim(key) {
		return SubmitResultDuplicate
	}
	claimed := true
	var runnable *resendRunnable
	defer func() {
		if runnable != nil {
			runnable.Abort()
		} else if claimed {
			s.release(key)
		}
	}()

	// The timeout starts at admission; this scheduler intentionally has no task buffer.
	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	runnable = &resendRunnable{
		ctx:    jobCtx,
		run:    run,
		logger: log.With(s.logger, workflowKeyTags(key)...),
		cleanup: func() {
			cancel()
			s.release(key)
		},
	}
	if !s.pool.TrySubmit(runnable) {
		metrics.WorkflowResendSchedulerAtCapacity.With(s.metricsHandler).Record(1)
		return SubmitResultAtCapacity
	}
	runnable = nil
	claimed = false
	return SubmitResultAccepted
}

func (s *HostScheduler) tryClaim(key definition.WorkflowKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inFlight[key]; ok {
		return false
	}
	s.inFlight[key] = struct{}{}
	return true
}

func (s *HostScheduler) release(key definition.WorkflowKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, key)
}

// InitiateShutdown cancels running jobs and aborts jobs waiting to run.
func (s *HostScheduler) InitiateShutdown() {
	s.pool.InitiateShutdown()
}

// WaitShutdown waits for all scheduler goroutines to exit.
func (s *HostScheduler) WaitShutdown() {
	s.pool.WaitShutdown()
}

type hostSchedulerLimiter struct {
	maxConcurrency dynamicconfig.IntPropertyFn
	logger         log.Logger
}

func (l hostSchedulerLimiter) Concurrency() (concurrency int) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			concurrency = 0
		}
	}()
	defer log.CapturePanic(l.logger, &panicErr)
	return l.maxConcurrency()
}

func (hostSchedulerLimiter) BufferSize() int {
	return 0
}

type resendRunnable struct {
	ctx     context.Context
	run     func(context.Context)
	logger  log.Logger
	cleanup func()
	once    sync.Once
}

func (r *resendRunnable) Run(shutdownCtx context.Context) {
	var panicErr error
	defer log.CapturePanic(r.logger, &panicErr)
	defer r.clean()

	runCtx, cancel := context.WithCancel(r.ctx)
	stopShutdownCancellation := context.AfterFunc(shutdownCtx, cancel)
	defer func() {
		stopShutdownCancellation()
		cancel()
	}()
	if shutdownCtx.Err() != nil || runCtx.Err() != nil {
		return
	}

	r.run(runCtx)
}

func (r *resendRunnable) Abort() {
	r.clean()
}

func (r *resendRunnable) clean() {
	r.once.Do(func() {
		if r.cleanup != nil {
			r.cleanup()
		}
	})
}

func workflowKeyTags(key definition.WorkflowKey) []tag.Tag {
	return []tag.Tag{
		tag.WorkflowNamespaceID(key.NamespaceID),
		tag.WorkflowID(key.WorkflowID),
		tag.WorkflowRunID(key.RunID),
	}
}
