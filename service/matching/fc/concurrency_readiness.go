package fc

import (
	"context"
	"errors"
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
	rs     *readinessShard
	mapKey string
	// Namespace IDs are UUIDs; keep their boundary so RPCs can share the map key storage.
	namespaceLen uint16

	generation int64
	tokens     int32
	// tokensSeq is incremented whenever tokens is overwritten from a server hint. When we take
	// a token in check, we record the sequence number, so that in cancelCheck, we can avoid
	// returning a token if tokens has been superseded since check.
	tokensSeq int64
	// invariant: {len(waiters) > 0} == {Wait goroutine is running} == {goroCancel != nil}
	// (for now, until we add eviction)
	goroCancel         context.CancelFunc
	waiterWakePriority wakePriority // wake priority in use by current call

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
		rs:           rs,
		mapKey:       mapkey,
		namespaceLen: uint16(len(nsID)),
		// optimistically assume unknown limiter has one slot, so we call Reserve first.
		// we'll get the real number of slots as a hint.
		tokens: 1,
	}
	rs.concurrencyLimiters[mapkey] = cs
	return cs
}

// check takes a check token if available. It returns the sequence of the token taken, to be
// passed to cancelCheck.
func (cs *concurrencyState) check(cb ReadinessCallback, pri wakePriority) (tokensSeq int64, err error) {
	err = cs.rs.update(cs, notifyAsync, func() error {
		if cs.tokens == 0 {
			cs.rs.addEdgeLocked(cs, cb, pri)
			return ErrConcurrencyBlocked
		}

		// remove in case it was present before
		cs.rs.removeEdgeLocked(cs, cb)
		// take one check token
		cs.tokens--
		tokensSeq = cs.tokensSeq
		return nil
	})
	return
}

func (cs *concurrencyState) cancelCheck(tokensSeq int64) {
	_ = cs.rs.update(cs, notifyAsync, func() error {
		if tokensSeq == cs.tokensSeq {
			// this could theoretically go over the limit but it doesn't matter here
			cs.tokens++
		}
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
	res, err := cs.rs.r.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyServiceBatchRequest{
		NamespaceId:         cs.namespaceID().String(),
		Key:                 cs.limiterKey(),
		ReserveSlots:        []string{slotID},
		ConfigUpdate:        configUpdate,
		ConfigUpdateVersion: configUpdateVersion,
	})
	if err != nil {
		return err
	}
	if len(res.GetReserveSuccess()) != 1 {
		return serviceerror.NewInternal("invalid concurrency batch response")
	}
	cs.reportSlotsHint(res.GetGeneration(), res.GetAvailableSlotsHint(), notifyAsync)
	if !res.ReserveSuccess[0] {
		return serviceerrors.NewFlowControlBlocked()
	}
	return nil
}

func (cs *concurrencyState) commit(ctx context.Context, slotID string) error {
	res, err := cs.rs.r.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyServiceBatchRequest{
		NamespaceId: cs.namespaceID().String(),
		Key:         cs.limiterKey(),
		CommitSlots: []string{slotID},
	})
	if err != nil {
		return err
	}
	if len(res.GetCommitSuccess()) != 1 {
		return serviceerror.NewInternal("invalid concurrency batch response")
	}
	cs.reportSlotsHint(res.GetGeneration(), res.GetAvailableSlotsHint(), notifyAsync)
	if !res.CommitSuccess[0] {
		return errCommitFailure
	}
	return nil
}

func (cs *concurrencyState) cancelReserve(ctx context.Context, slotID string) {
	// The poll context may already be canceled when rollback runs.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cs.rs.r.config.cancelReservationTimeout())
	go func() {
		defer cancel()
		// best-effort
		res, err := cs.rs.r.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyServiceBatchRequest{
			NamespaceId:            cs.namespaceID().String(),
			Key:                    cs.limiterKey(),
			CancelReservationSlots: []string{slotID},
		})
		if err == nil && res != nil {
			cs.reportSlotsHint(res.GetGeneration(), res.GetAvailableSlotsHint(), notifyAsync)
		}
	}()
}

func (cs *concurrencyState) reportSlotsHint(gen int64, slots int32, mode notifyMode) {
	_ = cs.rs.update(cs, mode, func() error {
		cs.updateTokensLocked(gen, slots)
		return nil
	})
}

// reportWaitResponse updates cached state from a Wait response. Wake tokens are a signal to
// this node only, so they don't update the slots hint that we pass on to other partitions.
func (cs *concurrencyState) reportWaitResponse(gen int64, wakeTokens int32, mode notifyMode) {
	_ = cs.rs.update(cs, mode, func() error {
		cs.updateTokensLocked(gen, wakeTokens)
		return nil
	})
}

// updateTokensLocked returns false if gen is older than our current state.
//
// TODO(fc): generation is not enough to really exclude stale hints, it only updates when
// slots goes to zero. we could use a server-side "hintSeq" that has more granularity than
// generation.
func (cs *concurrencyState) updateTokensLocked(gen int64, tokens int32) bool {
	if gen < cs.generation || tokens < 0 {
		return false
	}
	cs.generation = gen
	cs.tokens = tokens
	cs.tokensSeq++
	return true
}

func (cs *concurrencyState) syncLocked(rsu *readinessSyncUpdate) {
	// wake as many as we can. note that we do not take the tokens here, the waiter will do
	// that after it wakes up and calls check.
	rsu.wake(int(cs.tokens))

	haveWaiters := len(rsu.waiters) > 0

	// if we have an outstanding call but wake priority decreased, cancel it and start a new one
	if cs.goroCancel != nil && haveWaiters && betterWakePriority(rsu.waiters[0].pri, cs.waiterWakePriority) {
		cs.goroCancel()
		cs.goroCancel = nil
	}

	if (cs.goroCancel != nil) == haveWaiters {
		return
	}

	if !haveWaiters {
		cs.goroCancel()
		cs.goroCancel = nil
		return
	}

	ctx := headers.SetCallerInfo(context.Background(), headers.NewCallerInfo(
		cs.namespaceID().String(), // TODO(fc): use namespace name instead of id
		headers.CallerTypeBackgroundHigh,
		"",
	))
	ctx, cs.goroCancel = context.WithCancel(ctx)
	cs.waiterWakePriority = rsu.waiters[0].pri // set here so we have a not-stale value before callWait runs
	// Wait result will be reported back through reportWaitResponse
	go cs.callWait(ctx)
}

func (cs *concurrencyState) makeWaitRequest() *fcpb.ConcurrencyServiceWaitRequest {
	cs.rs.lock.Lock()
	defer cs.rs.lock.Unlock()

	waiters := cs.rs.fwd[cs]
	if len(waiters) == 0 {
		return nil
	}

	cs.waiterWakePriority = waiters[0].pri
	return &fcpb.ConcurrencyServiceWaitRequest{
		NamespaceId:         cs.namespaceID().String(),
		Key:                 cs.limiterKey(),
		Generation:          cs.generation,
		WakePriority:        int64(cs.waiterWakePriority),
		RequestedWakeTokens: int32(len(waiters)),
	}
}

func (cs *concurrencyState) callWait(ctx context.Context) {
	policy := backoff.NewExponentialRetryPolicy(time.Second).
		WithExpirationInterval(backoff.NoInterval).
		WithMaximumInterval(time.Minute)
	retrier := backoff.NewRetrier(policy, clock.NewRealTimeSource())

	for ctx.Err() == nil {
		req := cs.makeWaitRequest()
		if req == nil {
			// maybe race with canceling context
			_ = util.InterruptibleSleep(ctx, retrier.NextBackOff(nil))
			continue
		}
		res, err := cs.rs.r.concurrencyServiceClient.Wait(ctx, req)
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			// If the limiter doesn't exist yet, we'll get NotFound. Treat that as if we got
			// one token, so we'll allow a task through Check to Reserve, which will create it.
			res, err = &fcpb.ConcurrencyServiceWaitResponse{
				Generation: req.Generation,
				WakeTokens: 1,
			}, nil
		}
		if err != nil {
			_ = util.InterruptibleSleep(ctx, retrier.NextBackOff(err))
			continue
		}
		retrier.Reset()

		cs.reportWaitResponse(res.Generation, res.WakeTokens, notifySync)
		// note: If we have satisfied all our waiters, then ctx
		// will be canceled before we continue this loop.
	}
}

func (cs *concurrencyState) namespaceID() namespace.ID {
	return namespace.ID(cs.mapKey[:cs.namespaceLen])
}

func (cs *concurrencyState) limiterKey() string {
	return cs.mapKey[cs.namespaceLen:]
}
