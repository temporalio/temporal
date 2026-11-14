package shard

import (
	"context"

	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// historyTaskRangeCompleter is implemented by *ContextImpl. It is deliberately not part of
// historyi.ShardContext, so admission can be added without changing that interface.
type historyTaskRangeCompleter interface {
	RangeCompleteHistoryTasks(ctx context.Context, request *persistence.RangeCompleteHistoryTasksRequest) error
}

// RangeCompleteHistoryTasks deletes a range of history tasks owned by shardContext.
//
// request.RangeID must be the shard range ID captured when the deletion boundary was computed.
// It is passed through unchanged and enforced by persistence. When shardContext provides shard
// I/O admission (*ContextImpl), the deletion is admitted through it. Otherwise the request goes
// directly to the execution manager with the same ownership fence.
func RangeCompleteHistoryTasks(
	ctx context.Context,
	shardContext historyi.ShardContext,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	if completer, ok := shardContext.(historyTaskRangeCompleter); ok {
		return completer.RangeCompleteHistoryTasks(ctx, request)
	}
	return shardContext.GetExecutionManager().RangeCompleteHistoryTasks(ctx, request)
}

// RangeCompleteHistoryTasks deletes a range of history tasks. On stores where range completion
// is a conditional write on the shard partition (Cassandra), it is admitted through ioSemaphore so
// it does not run concurrently with this shard's other conditional writes. request.RangeID is
// not refreshed: ownership is still fenced by persistence with the caller's captured range ID.
func (s *ContextImpl) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	if !s.serializeRangeCompletion {
		return s.executionManager.RangeCompleteHistoryTasks(ctx, request)
	}

	// Bind the request to the shard lifetime so that waiting for admission ends when the shard stops.
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	// Always use PriorityHigh, regardless of caller type. Replication cleanup uses a preemptable
	// caller, and a PriorityLow waiter can be starved for as long as foreground writes keep the
	// high-priority queue non-empty. Cleanup that never runs leaves completed tasks in place.
	if err := s.ioSemaphoreAcquireWithPriority(ctx, locks.PriorityHigh); err != nil {
		return err
	}
	defer s.ioSemaphoreRelease()

	// Do not issue the conditional delete from a shard that is acquiring or stopping.
	if err := s.errorByState(); err != nil {
		return err
	}

	// Failures are returned to the caller, which retries with backoff. They are deliberately not
	// passed to handleWriteError: a failed cleanup must not trigger shard re-acquisition.
	return s.executionManager.RangeCompleteHistoryTasks(ctx, request)
}
