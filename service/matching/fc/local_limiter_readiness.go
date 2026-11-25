package fc

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/matching/simplelimiter"
)

// maxLocalLimiterTokens is the maximum number of tokens we might consume at a time for
// simplelimiter.Limiter. This is used to update ready times after a rate is changed from very
// low (or zero) to higher: we may have set a ready time far in the future and need to clip it
// to something reasonable so we can dispatch again.
//
// Currently we only use 1 token at a time.
const maxLocalLimiterTokens = 1

var ErrLocalLimiterBlocked = serviceerror.NewFailedPrecondition("blocked by local limiter")

type localLimiterState struct {
	rs *readinessShard

	params simplelimiter.Params
	lim    simplelimiter.Ready
	// invariant: lim.Delay() < 0 -> len(waiters) == 0
	// invariant: {len(waiters) > 0} == {tmr != nil}
	tmr clock.Timer
}

func (r *Readiness) getLocalLimiter(nsID namespace.ID, key string) *localLimiterState {
	return r.getShard(nsID).getLocalLimiter(nsID, key)
}

func (rs *readinessShard) getLocalLimiter(nsID namespace.ID, key string) *localLimiterState {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	mapkey := nsID.String() + key
	if lls, ok := rs.localLimiters[mapkey]; ok {
		return lls
	}
	lls := &localLimiterState{
		rs:     rs,
		params: simplelimiter.NoLimit(),
	}
	rs.localLimiters[mapkey] = lls
	return lls
}

func (lls *localLimiterState) now() int64 {
	return lls.rs.r.timeSource.Now().UnixNano()
}

func (lls *localLimiterState) check(config simplelimiter.Params, cb ReadinessCallback, pri wakePriority) error {
	return lls.rs.update(lls, notifyAsync, func() error {
		now := lls.now()

		// install new params
		lls.params = config
		// Clip to handle the case where we have increased from a zero or very low limit and had
		// ready times in the far future.
		lls.lim = lls.lim.Clip(lls.params, now, maxLocalLimiterTokens)

		if delay := lls.lim.Delay(now); delay > 0 {
			lls.rs.addEdgeLocked(lls, cb, pri)
			return ErrLocalLimiterBlocked
		}

		// remove in case it was present before
		lls.rs.removeEdgeLocked(lls, cb)
		// now we can take the token
		lls.lim = lls.lim.Consume(lls.params, now, 1)
		return nil
	})
}

func (lls *localLimiterState) cancelCheck() {
	_ = lls.rs.update(lls, notifyAsync, func() error {
		now := lls.now()
		// return the token
		lls.lim = lls.lim.Consume(lls.params, now, -1)
		return nil
	})
}

func (lls *localLimiterState) syncLocked(rsu *readinessSyncUpdate) {
	now := lls.now()

	// use a local copy to figure out how many we can wake. note that we do not take from the
	// actual limiter here, since the waiter will do that after it wakes up and calls check.
	lim := lls.lim
	for lim.Delay(now) <= 0 && len(rsu.waiters) > 0 {
		rsu.wake(1)
		lim = lim.Consume(lls.params, now, 1)
	}

	if len(rsu.waiters) == 0 {
		if lls.tmr != nil {
			lls.tmr.Stop()
			lls.tmr = nil
		}
		return
	}

	delay := lim.Delay(now)
	if lls.tmr != nil {
		lls.tmr.Reset(delay)
	} else {
		lls.tmr = lls.rs.r.timeSource.AfterFunc(delay, lls.onTimer)
	}
}

func (lls *localLimiterState) onTimer() {
	_ = lls.rs.update(lls, notifySync, func() error { return nil })
}
