package scheduler

import (
	"context"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

// Export unexported methods for testing.

// ExecutionStatus search-attribute values, exported for tests.
var (
	ExecutionStatusRunning   = executionStatusRunning
	ExecutionStatusCompleted = executionStatusCompleted
)

func NewTestHandler(logger log.Logger) *handler {
	return newHandler(logger, legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 }))
}

func (h *handler) TestCreateFromMigrationState(ctx context.Context, req *schedulerpb.CreateFromMigrationStateRequest) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	return h.CreateFromMigrationState(ctx, req)
}

func (h *handler) TestMigrateToWorkflow(ctx context.Context, req *schedulerpb.MigrateToWorkflowRequest) (*schedulerpb.MigrateToWorkflowResponse, error) {
	return h.MigrateToWorkflow(ctx, req)
}

func (s *Scheduler) RecordCompletedAction(
	ctx chasm.MutableContext,
	completed *schedulespb.CompletedResult,
	requestID string,
) time.Time {
	invoker := s.Invoker.Get(ctx)
	return invoker.recordCompletedAction(ctx, completed, requestID)
}

func (i *Invoker) RunningWorkflowID(requestID string) string {
	return i.runningWorkflowID(requestID)
}

// RecentActionCount exposes the completed-retention limit for tests.
const RecentActionCount = recentActionCount

// ApplyCompletedRetention exposes applyCompletedRetention for tests.
func (i *Invoker) ApplyCompletedRetention() {
	i.applyCompletedRetention()
}

// RecordExecuteResult exposes recordExecuteResult so tests can pin the
// per-RequestId idempotency guard against concurrent ExecuteTasks.
func (i *Invoker) RecordExecuteResult(
	ctx chasm.MutableContext,
	completed []*schedulespb.BufferedStart,
	retryable []*schedulespb.BufferedStart,
) (newlyStarted, droppedDuplicates int) {
	return i.recordExecuteResult(ctx, &executeResult{
		CompletedStarts: completed,
		RetryableStarts: retryable,
	})
}

func (b *BackfillerTaskHandler) ProcessBackfill(
	scheduler *Scheduler,
	backfiller *Backfiller,
	limit int,
) (backfillProgressResult, error) {
	return b.processBackfill(nil, scheduler, backfiller, limit)
}

func (b *BackfillerTaskHandler) AllowedBufferedStarts(
	ctx chasm.Context,
	scheduler *Scheduler,
	invoker *Invoker,
	tweakables Tweakables,
) (int, error) {
	return b.allowedBufferedStarts(ctx, scheduler, invoker, tweakables)
}
