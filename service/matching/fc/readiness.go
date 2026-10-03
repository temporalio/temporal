package fc

import (
	"cmp"
	"hash/maphash"
	"reflect"
	"slices"
	"sync"

	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
)

const (
	// Shards should be a power of 2
	readinessStateShards = 32
)

type (
	Readiness struct {
		timeSource               clock.TimeSource
		concurrencyServiceClient fcpb.ConcurrencyServiceClient

		// Readiness state sharding and locking:
		//
		// The sharding and locking scheme tries to achieve high concurrency, given that we have to
		// maintain maps in two directions: limiters to callbacks, and callbacks to limiters, in a
		// many-to-many relationship. We also want the ability to do some processing over all
		// limiters.
		//
		// Overall, we shard the maps based on a hash of the namespace id: limiter<->callback
		// relations can't cross namespaces, so this is safe. Each shard uses a mutex to
		// synchronize access to its maps.
		shards [readinessStateShards]readinessShard

		seed maphash.Seed
	}

	readinessShard struct {
		r *Readiness

		// lock protects the maps below and everything in concurrencyState and
		// localLimiterState.
		lock sync.Mutex

		// for LIMITER_TYPE_CONCURRENCY
		concurrencyLimiters map[string]*concurrencyState // key is nsID+key
		// for LIMITER_TYPE_LOCAL_RATE_LIMIT
		localLimiters map[string]*localLimiterState // key is nsID+key

		// forward map, sorted by <pri, pointer(cb)>
		fwd map[limiterState][]fwdMapEntry
		// reverse map, sorted by <pointer(limiter)>
		rev map[ReadinessCallback][]revMapEntry

		// TODO(fc): clean up caches of idle/stale state
		// TODO(fc): gauges for size of cache
		// TODO(fc): limit active waiters, add suspended state, etc.

		_ [64 - 8 - 8 - 8 - 8 - 8 - 8]byte // force to different cache lines to avoid false sharing
	}

	fwdMapEntry struct {
		pri wakePriority
		cb  ReadinessCallback
	}
	revMapEntry struct {
		limiter limiterState
		pri     wakePriority
	}
	limiterState interface {
		syncLocked(*readinessSyncUpdate)
	}

	readinessSyncUpdate struct {
		waiters []fwdMapEntry
	}
	deferedNotify []ReadinessCallback
)

func NewReadiness(
	timeSource clock.TimeSource,
	concurrencyServiceClient fcpb.ConcurrencyServiceClient,
) *Readiness {
	r := &Readiness{
		timeSource:               timeSource,
		concurrencyServiceClient: concurrencyServiceClient,
		seed:                     maphash.MakeSeed(),
	}
	for i := range r.shards {
		rs := &r.shards[i]
		rs.r = r
		rs.concurrencyLimiters = make(map[string]*concurrencyState)
		rs.localLimiters = make(map[string]*localLimiterState)
		rs.fwd = make(map[limiterState][]fwdMapEntry)
		rs.rev = make(map[ReadinessCallback][]revMapEntry)
	}
	return r
}

func (r *Readiness) getShardIdx(nsID namespace.ID) int {
	return int(maphash.String(r.seed, nsID.String()) % readinessStateShards)
}

func (r *Readiness) getShard(nsID namespace.ID) *readinessShard {
	return &r.shards[r.getShardIdx(nsID)]
}

func (r *Readiness) Stop() {
	for i := range r.shards {
		r.shards[i].stop()
	}
}

func (rs *readinessShard) stop() {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	clear(rs.fwd)
	clear(rs.rev)
	var rsu readinessSyncUpdate
	for _, v := range rs.concurrencyLimiters {
		v.syncLocked(&rsu)
	}
	clear(rs.concurrencyLimiters)
	for _, v := range rs.localLimiters {
		v.syncLocked(&rsu)
	}
	clear(rs.localLimiters)
}

func (r *Readiness) CancelAllCallbacks(nsID namespace.ID, cb ReadinessCallback) {
	r.getShard(nsID).cancelAllCallbacks(cb)
}

func (rs *readinessShard) cancelAllCallbacks(cb ReadinessCallback) {
	// It would be quite unlikely, but it's possible for a limiter to decide to notify _other_
	// callbacks when we sync it. e.g. a rate limit due to the wall clock time advancing. The
	// timer would usually get there first but we may need to call some here.
	var toNotify deferedNotify
	defer toNotify.notify() // outside lock

	rs.lock.Lock()
	defer rs.lock.Unlock()

	rEnts := rs.rev[cb]
	// note: clone slice before iterating over it, removeEdgeLocked may modify slices
	for _, rEnt := range slices.Clone(rEnts) {
		rs.removeEdgeLocked(rEnt.limiter, cb)
		rs.syncLimiter(rEnt.limiter, &toNotify)
	}
}

func (rs *readinessShard) syncLimiter(limiter limiterState, toNotify *deferedNotify) {
	// call syncLocked with current waiter set
	fEnts := rs.fwd[limiter]
	rsu := readinessSyncUpdate{waiters: fEnts}
	// syncLocked may call rsu.wake(), which will remove elements from the front of rsu.waiters
	limiter.syncLocked(&rsu)

	// record and remove the ones that we're going to notify
	woke := len(fEnts) - len(rsu.waiters)
	// note: clone slice before iterating over it, removeEdgeLocked may modify slices
	for _, fEnt := range slices.Clone(fEnts[:woke]) {
		rs.removeEdgeLocked(limiter, fEnt.cb)
		toNotify.add(fEnt.cb)
	}
}

func (rs *readinessShard) addEdgeLocked(limiter limiterState, cb ReadinessCallback, pri wakePriority) {
	// fwd is sorted by pri, so if cb is already present with a different pri, we have to
	// remove the old entry first or we'll leave a stale one behind.
	rs.removeEdgeLocked(limiter, cb)

	newFEnt := fwdMapEntry{pri: pri, cb: cb}
	fEnts := rs.fwd[limiter]
	if i, found := slices.BinarySearchFunc(fEnts, newFEnt, fwdMapEntryCmp); found {
		fEnts[i] = newFEnt
	} else {
		rs.fwd[limiter] = slices.Insert(fEnts, i, newFEnt)
	}

	newREnt := revMapEntry{limiter: limiter, pri: pri}
	rEnts := rs.rev[cb]
	if i, found := slices.BinarySearchFunc(rEnts, newREnt, revMapEntryCmp); found {
		rEnts[i] = newREnt
	} else {
		rs.rev[cb] = slices.Insert(rEnts, i, newREnt)
	}
}

func (rs *readinessShard) removeEdgeLocked(limiter limiterState, cb ReadinessCallback) {
	// first look up in reverse map
	rEnts := rs.rev[cb]
	i, found := slices.BinarySearchFunc(rEnts, revMapEntry{limiter: limiter}, revMapEntryCmp)
	if !found {
		return
	}
	// record pri
	pri := rEnts[i].pri
	// now delete from reverse map
	if rEnts = slices.Delete(rEnts, i, i+1); len(rEnts) == 0 {
		delete(rs.rev, cb)
	} else {
		rs.rev[cb] = rEnts
	}

	// find in forward map
	fEnts := rs.fwd[limiter]
	i, found = slices.BinarySearchFunc(fEnts, fwdMapEntry{pri: pri, cb: cb}, fwdMapEntryCmp)
	if !found {
		return
	}
	// delete from forward map
	if fEnts = slices.Delete(fEnts, i, i+1); len(fEnts) == 0 {
		delete(rs.fwd, limiter)
	} else {
		rs.fwd[limiter] = fEnts
	}
}

func (rs *readinessShard) update(limiter limiterState, f func() error) error {
	var toNotify deferedNotify
	defer toNotify.notify() // outside lock

	rs.lock.Lock()
	defer rs.lock.Unlock()

	defer rs.syncLimiter(limiter, &toNotify)

	return f()
}

// syncLocked may call this. wake should remove up to n items from the front of rsu.waiters and
// update rsu.waiters.
func (rsu *readinessSyncUpdate) wake(n int) {
	take := min(n, len(rsu.waiters))
	rsu.waiters = rsu.waiters[take:]
}

func (dn *deferedNotify) notify() {
	cbs := *dn
	if len(cbs) == 0 {
		return
	}
	// Callers of Tx.Check etc. may be holding their own lock (e.g. matcherData.lock), and
	// OnReady may need that same lock, so we can't call OnReady synchronously here.
	go func() {
		for _, cb := range cbs {
			cb.OnReady()
		}
	}()
}

func (dn *deferedNotify) add(cb ...ReadinessCallback) {
	*dn = append(*dn, cb...)
}

func fwdMapEntryCmp(a, b fwdMapEntry) int {
	if c := cmp.Compare(a.pri, b.pri); c != 0 {
		return c
	}
	return cmp.Compare(
		reflect.ValueOf(a.cb).Pointer(),
		reflect.ValueOf(b.cb).Pointer(),
	)
}

func revMapEntryCmp(a, b revMapEntry) int {
	return cmp.Compare(
		reflect.ValueOf(a.limiter).Pointer(),
		reflect.ValueOf(b.limiter).Pointer(),
	)
}
