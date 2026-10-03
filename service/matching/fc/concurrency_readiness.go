package fc

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
)

var ErrConcurrencyBlocked = serviceerror.NewFailedPrecondition("blocked by concurrency limit")

type concurrencyState struct {
	rs   *readinessShard
	nsID namespace.ID
	key  string

	generation int64
	tokens     int32
	// invariant: {len(waiters) > 0} == {Wait goroutine is running} == {goroCancel != nil}
	// (for now, until we add eviction)
	goroCancel context.CancelFunc
}

func (r *Readiness) getConcurrencyLimiter(nsID namespace.ID, key string) *concurrencyState {
	return r.getShard(nsID).getConcurrencyLimiter(nsID, key)
}

func (rs *readinessShard) getConcurrencyLimiter(nsID namespace.ID, key string) *concurrencyState {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	mapkey := nsID.String() + key
	if cs, ok := rs.concurrencyLimiters[mapkey]; ok {
		return cs
	}
	cs := &concurrencyState{
		rs:   rs,
		nsID: nsID,
		key:  key,
	}
	rs.concurrencyLimiters[mapkey] = cs
	return cs
}

func (cs *concurrencyState) check(cb ReadinessCallback, pri wakePriority) error {
	return cs.rs.update(cs, func() error {
		if cs.tokens == 0 {
			cs.rs.addEdgeLocked(cs, cb, pri)
			return ErrConcurrencyBlocked
		}

		// remove in case it was present before
		cs.rs.removeEdgeLocked(cs, cb)
		// take one check token
		cs.tokens--
		return nil
	})
}

func (cs *concurrencyState) cancelCheck() {
	cs.rs.update(cs, func() error {
		// this could theoretically go over the limit but it doesn't matter here
		cs.tokens++
		return nil
	})
}

func (cs *concurrencyState) reserve(
	ctx context.Context,
	slotID string,
	configUpdate *taskqueuepb.ConcurrencyLimit,
	configUpdateVersion int64,
) error {
	// TODO(fc): consider doing this from history to make invalid tasks cheaper to invalidate
	res, err := cs.rs.r.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyBatchRequest{
		NamespaceId:         cs.nsID.String(),
		Key:                 cs.key,
		ReserveSlots:        []string{slotID},
		ConfigUpdate:        configUpdate,
		ConfigUpdateVersion: configUpdateVersion,
	})
	if err != nil {
		return err // don't update cache on rpc error
	}
	cs.reportSlotsHint(res.Generation, res.AvailableSlotsHint)
	if !res.ReserveSuccess[0] {
		return serviceerrors.NewFlowControlBlocked()
	}
	return nil
}

func (cs *concurrencyState) commit(ctx context.Context, slotID string) error {
	res, err := cs.rs.r.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyBatchRequest{
		NamespaceId: cs.nsID.String(),
		Key:         cs.key,
		CommitSlots: []string{slotID},
	})
	if err != nil {
		return err
	}
	cs.reportSlotsHint(res.Generation, res.AvailableSlotsHint)
	if !res.CommitSuccess[0] {
		return errCommitFailure
	}
	return nil
}

func (cs *concurrencyState) cancelReserve(ctx context.Context, slotID string) {
	// call in new goroutine, don't block here
	go func() {
		res, err := cs.rs.r.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyBatchRequest{
			NamespaceId:            cs.nsID.String(),
			Key:                    cs.key,
			CancelReservationSlots: []string{slotID},
		})
		if err == nil {
			cs.reportSlotsHint(res.Generation, res.AvailableSlotsHint)
		}
	}()
}

func (cs *concurrencyState) reportSlotsHint(gen int64, slots int32) {
	cs.rs.update(cs, func() error {
		if gen < cs.generation {
			return nil
		}
		cs.generation = gen

		// the first batched response will get the actual slots hint, the rest will be given -1 so
		// that we don't resupply tokens after a waiter that we wake takes them
		if slots >= 0 {
			cs.tokens = slots
		}
		return nil
	})
}

func (cs *concurrencyState) syncLocked(rsu *readinessSyncUpdate) {
	// wake as many as we can. note that we do not take the tokens here, the waiter will do
	// that after it wakes up and calls check.
	rsu.wake(int(cs.tokens))

	haveWaiters := len(rsu.waiters) > 0
	if (cs.goroCancel != nil) == haveWaiters {
		return
	}

	if !haveWaiters {
		cs.goroCancel()
		cs.goroCancel = nil
		return
	}

	ctx := headers.SetCallerInfo(context.Background(), headers.NewCallerInfo(
		cs.nsID.String(), // TODO(fc): use namespace name instead of id
		headers.CallerTypeBackgroundHigh,
		"",
	))
	ctx, cs.goroCancel = context.WithCancel(ctx)
	// Wait result will be reported back through reportSlotsHint
	go cs.callWait(ctx)

	return
}

func (cs *concurrencyState) makeWaitRequest() *fcpb.ConcurrencyWaitRequest {
	cs.rs.lock.Lock()
	defer cs.rs.lock.Unlock()

	waiters := cs.rs.fwd[cs]
	if len(waiters) == 0 {
		return nil
	}

	return &fcpb.ConcurrencyWaitRequest{
		NamespaceId:         cs.nsID.String(),
		Key:                 cs.key,
		Generation:          cs.generation,
		WakePriority:        int64(waiters[0].pri),
		RequestedWakeTokens: int32(len(waiters)),
	}
}

func (cs *concurrencyState) callWait(ctx context.Context) {
	policy := backoff.NewExponentialRetryPolicy(time.Second).
		WithExpirationInterval(backoff.NoInterval).
		WithMaximumInterval(time.Minute)
	retrier := backoff.NewRetrier(policy, clock.NewRealTimeSource())

	for ctx.Err() == nil {
		// TODO(fc): if minPriority decreases during this call, interrupt and restart it.
		// increasing minPriority should not interrupt
		req := cs.makeWaitRequest()
		if req == nil {
			// maybe race with canceling context
			util.InterruptibleSleep(ctx, retrier.NextBackOff(nil))
			continue
		}
		res, err := cs.rs.r.concurrencyServiceClient.Wait(ctx, req)
		if _, ok := err.(*serviceerror.NotFound); ok {
			// The limiter is only created by Reserve, so there's nothing to wait on yet. Let one
			// waiter through to Reserve, which will create it with the initial config.
			cs.reportSlotsHint(req.Generation, 1)
			continue
		} else if err != nil {
			util.InterruptibleSleep(ctx, retrier.NextBackOff(err))
			continue
		}
		retrier.Reset()

		cs.reportSlotsHint(res.Generation, res.WakeTokens)
		// note: If we have satisfied all our waiters, then ctx
		// will be canceled before we continue this loop.
	}
}
