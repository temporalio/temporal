package concurrency

import (
	"context"
	"hash/maphash"
	"sync"
	"time"

	"github.com/tidwall/btree"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/stream_batcher"
)

type batchKey struct {
	namespaceID string
	key         string
}

type batchReq struct {
	ctx context.Context
	req *fcpb.ConcurrencyBatchRequest
}

type chasmReq struct {
	items        []batchReq
	getWakeLevel func(wantTokens int32) (wakeUpTo int64, wakeAll bool)
}

type batchRes struct {
	res *fcpb.ConcurrencyBatchResponse
	err error
}

type waiterEntry struct {
	// Waiter priority
	wakePriority int64
	// Number of tokens represented by this entry, including multiplicity of multiple waiters
	// on one host. Note that the multiplicity only updates when the long-poll retries, once a
	// minute, so this may be somewhat out of date. But each waiter counts at least once here.
	tokens int32
}

type waiterEntries = btree.BTreeG[waiterEntry]

// number of shards in sharded map
const numWaiterShards = 16

// waiterShard holds the waiters for a subset of batchKeys, protected by its own lock. Each
// shard's lock also protects the waiterEntries btrees reachable from its map, since those
// btrees are created with NoLocks and are not otherwise safe for concurrent access.
type waiterShard struct {
	lock    sync.Mutex
	waiters map[batchKey]*waiterEntries
}

type Handler struct {
	fcpb.UnimplementedConcurrencyServiceServer

	logger   log.Logger
	batchers *stream_batcher.KeyedBatcher[batchKey, batchReq, batchRes]

	waiterSeed   maphash.Seed
	waiterShards [numWaiterShards]waiterShard
}

func NewHandler(
	logger log.Logger,
	dc *dynamicconfig.Collection,
) *Handler {
	h := &Handler{
		logger:     logger,
		waiterSeed: maphash.MakeSeed(),
	}
	for i := range h.waiterShards {
		h.waiterShards[i].waiters = make(map[batchKey]*waiterEntries)
	}
	h.batchers = stream_batcher.NewKeyedBatcherWithPerItemResults(
		h.applyBatch,
		ServerBatcherOptions.Get(dc)(),
		clock.NewRealTimeSource(),
	)
	return h
}

func initFn(_ chasm.MutableContext, creq chasmReq) (*Component, error) {
	// For now (whole-queue limits), we should always have an initial config here.
	// For per-task limits, this may be missing and set via api later.
	c := &Component{
		ConcurrencyState: &fcpb.ConcurrencyState{
			Config: &taskqueuepb.ConcurrencyLimit{
				ConcurrentTasks: initialLimit,
			},
		},
	}
	for _, it := range creq.items {
		c.updateConfig(it.req.GetConfigUpdate(), it.req.GetConfigUpdateVersion())
	}
	// if any tasks are allowed at all, then Wait on generation 0 should not block
	c.WakeAll = c.Config.ConcurrentTasks > 0
	return c, nil
}

func updateFn(c *Component, cctx chasm.MutableContext, creq chasmReq) ([]*fcpb.ConcurrencyBatchResponse, error) {
	now := cctx.Now(c)

	// allocate new protos for responses
	ress := make([]*fcpb.ConcurrencyBatchResponse, len(creq.items))
	for i := range ress {
		ress[i] = &fcpb.ConcurrencyBatchResponse{}
	}

	prevStoredAvailable := c.availableSlots()
	prevEffectiveAvailable := c.effectiveAvailableSlots(now)

	// expire old reservations
	c.expire(now)

	// update config
	for _, it := range creq.items {
		c.updateConfig(it.req.GetConfigUpdate(), it.req.GetConfigUpdateVersion())
	}

	// apply releases
	for _, it := range creq.items {
		for _, slotId := range it.req.GetReleaseSlots() {
			c.release(slotId)
		}
	}

	// apply commits
	for i, it := range creq.items {
		for _, slotId := range it.req.GetCommitSlots() {
			success := c.commit(slotId)
			ress[i].CommitSuccess = append(ress[i].CommitSuccess, success)
		}
	}

	// apply cancels
	for _, it := range creq.items {
		for _, slotId := range it.req.GetCancelReservationSlots() {
			c.cancelReservation(slotId)
		}
	}

	// apply reserves
	for i, it := range creq.items {
		for _, slotId := range it.req.GetReserveSlots() {
			success := c.reserve(slotId, now)
			ress[i].ReserveSuccess = append(ress[i].ReserveSuccess, success)
		}
	}

	// Increment generation if we took the last slot. Note that we check prevEffectiveAvailable
	// so that an expiration plus taking that expired slot increases generation.
	if c.availableSlots() == 0 && prevEffectiveAvailable > 0 {
		c.incrementGeneration()
	}

	// If we have additional available slots, then we can wake some waiters. Note that we only
	// wake on an _increase_ in available slots. If the number is the same or fewer, even if
	// positive, then the previous transaction's wakes (or the staged wake task started by
	// that) should handle those. If we just called doWake again here, we might push our task
	// out later before it got to run.
	//
	// Note that we can't do this in the same transaction that we increment generation in
	// (availableSlots can't be both 0 and > prevStoredAvailable).
	if c.availableSlots() > prevStoredAvailable {
		doWake(cctx, c, creq.getWakeLevel)
	}

	// capture Generation after maybe incrementing
	for i := range creq.items {
		ress[i].Generation = c.Generation
	}

	return ress, nil
}

func (h *Handler) applyBatch(
	key batchKey,
	items []batchReq,
) []batchRes {
	needStart := false
	canSpeculate := true

	for _, it := range items {
		// only Reserve needs UpdateWithStart, other calls can assume it exists
		needStart = needStart || len(it.req.GetReserveSlots()) > 0

		// Commit and Release must be durable, Reserve and cancel may be speculative
		canSpeculate = canSpeculate && len(it.req.GetCommitSlots()) == 0 && len(it.req.GetReleaseSlots()) == 0
	}

	opts := make([]chasm.TransitionOption, 0, 2)
	if canSpeculate {
		opts = append(opts, chasm.WithSpeculative()) // note: not implemented yet
	}

	creq := chasmReq{
		items:        items,
		getWakeLevel: func(wantTokens int32) (int64, bool) { return h.getWakeLevel(key, wantTokens) },
	}

	if needStart {
		opts = append(opts, chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyUseExisting,
		))
		updateRes, err := chasm.UpdateWithStartExecution(
			items[0].ctx,
			chasm.ExecutionKey{
				NamespaceID: key.namespaceID,
				BusinessID:  key.key,
			},
			initFn,
			updateFn,
			creq,
			opts...,
		)
		return makeBatchResults(len(items), updateRes.UpdateOutput, err)
	}

	ress, _, err := chasm.UpdateComponent(
		items[0].ctx,
		chasm.NewComponentRef[*Component](chasm.ExecutionKey{
			NamespaceID: key.namespaceID,
			BusinessID:  key.key,
		}),
		updateFn,
		creq,
		opts...,
	)
	return makeBatchResults(len(items), ress, err)
}

func makeBatchResults(
	size int,
	ress []*fcpb.ConcurrencyBatchResponse,
	err error,
) []batchRes {
	results := make([]batchRes, size)
	if err != nil {
		for i := range results {
			results[i].err = err
		}
	} else {
		for i := range results {
			if i < len(ress) {
				results[i].res = ress[i]
			}
		}
	}
	return results
}

func (h *Handler) Batch(ctx context.Context, req *fcpb.ConcurrencyBatchRequest) (retRes *fcpb.ConcurrencyBatchResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	res, err := h.batchers.Add(ctx, batchKey{
		namespaceID: req.GetNamespaceId(),
		key:         req.GetKey(),
	}, batchReq{ctx: ctx, req: req})
	if err != nil {
		return nil, err
	}
	return res.res, res.err
}

func (h *Handler) Wait(ctx context.Context, req *fcpb.ConcurrencyWaitRequest) (retRes *fcpb.ConcurrencyWaitResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	req = common.CloneProto(req)
	req.RequestedWakeTokens = max(req.RequestedWakeTokens, 1) // assume one token if unset
	req.WakePriority = max(req.WakePriority, 1)               // must be positive since 0 means no wakes yet

	// Note that the maximum timeout here has an effect besides just capping rpc duration:
	// If a caller has Reserved the last slot, and crashes before Commit, then the reservation
	// will expire. But expiration is handled lazily, it doesn't cause a state transition of
	// the component. So a Wait call that's waiting for that expiry will only notice when it
	// re-enters PollComponent, which is only once per this timeout.
	// TODO(fc): move constants to dynamic config
	ctx, cancel := contextutil.WithDeadlineBuffer(ctx, time.Minute, time.Second)
	defer cancel()

	k := batchKey{namespaceID: req.NamespaceId, key: req.Key}
	h.registerWaiter(k, req.WakePriority, req.RequestedWakeTokens)
	defer h.unregisterWaiter(k, req.WakePriority, req.RequestedWakeTokens)

	// loop to do internal retries with increased generation
	for {
		// CHASM requires that PollComponent be monotonic: if it returns true at some state,
		// then it should return true at future states. Our predicate is "is state.wake_up_to
		// >= req.start_time" (or state.wake_all means wake_upto = forever in the future). But
		// when we go from having slots to not having slots, we need to reset wake_up_to to
		// some time in the past, so that everyone is blocked again. So we need to add a
		// "generation" on top of the timestamps. Essentially, the state moves the pair
		// <generation, wake_up_to> monotonically.
		res, _, err := chasm.PollComponent(
			ctx,
			chasm.NewComponentRef[*Component](chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.Key,
			}),
			func(c *Component, cctx chasm.Context, req *fcpb.ConcurrencyWaitRequest) (*fcpb.ConcurrencyWaitResponse, bool, error) {
				generation, tokens, ready := c.poll(cctx.Now(c), req.Generation, req.WakePriority, req.RequestedWakeTokens)
				if !ready {
					return nil, false, nil
				}
				return &fcpb.ConcurrencyWaitResponse{Generation: generation, WakeTokens: tokens}, true, nil
			},
			req,
		)
		if err != nil {
			if common.IsContextDeadlineExceededErr(err) {
				// timed out without becoming ready
				return &fcpb.ConcurrencyWaitResponse{Generation: req.Generation}, nil
			}
			return nil, err
		} else if res == nil {
			// chasm-imposed timeout
			return &fcpb.ConcurrencyWaitResponse{Generation: req.Generation}, nil
		} else if res.Generation != req.Generation {
			// try again on server with new generation
			req.Generation = res.Generation
			continue
		}
		return res, nil
	}
}

// waiterShardFor returns the shard responsible for key.
func (h *Handler) waiterShardFor(key batchKey) *waiterShard {
	var hash maphash.Hash
	hash.SetSeed(h.waiterSeed)
	hash.WriteString(key.namespaceID)
	hash.WriteString(key.key)
	return &h.waiterShards[hash.Sum64()%numWaiterShards]
}

func (s *waiterShard) getWaiterEntriesLocked(key batchKey) *waiterEntries {
	if wt, ok := s.waiters[key]; ok {
		return wt
	}
	wt := btree.NewBTreeGOptions(
		func(a, b waiterEntry) bool { return a.wakePriority < b.wakePriority },
		// degree 3 allows up to 5 values per node without splitting
		btree.Options{Degree: 3, NoLocks: true},
	)
	s.waiters[key] = wt
	return wt
}

func (h *Handler) registerWaiter(key batchKey, wakePriority int64, tokens int32) {
	s := h.waiterShardFor(key)
	s.lock.Lock()
	defer s.lock.Unlock()

	wt := s.getWaiterEntriesLocked(key)
	newTokens := tokens
	var hint btree.PathHint
	if entry, ok := wt.GetHint(waiterEntry{wakePriority: wakePriority}, &hint); ok {
		newTokens += entry.tokens
	}
	wt.SetHint(waiterEntry{wakePriority: wakePriority, tokens: newTokens}, &hint)
}

func (h *Handler) unregisterWaiter(key batchKey, wakePriority int64, tokens int32) {
	s := h.waiterShardFor(key)
	s.lock.Lock()
	defer s.lock.Unlock()

	wt, ok := s.waiters[key]
	if !ok {
		return
	}
	var hint btree.PathHint
	if entry, ok := wt.GetHint(waiterEntry{wakePriority: wakePriority}, &hint); ok {
		newTokens := max(0, entry.tokens-tokens)
		if newTokens == 0 {
			wt.DeleteHint(waiterEntry{wakePriority: wakePriority}, &hint)
		} else {
			wt.SetHint(waiterEntry{wakePriority: wakePriority, tokens: newTokens}, &hint)
		}
	}
	if wt.Len() == 0 {
		delete(s.waiters, key)
	}
}

// getWakeLevel returns t such that waiters representing wantTokens tokens have wakePriority <= t.
// If there aren't enough waiters to satisfy all the tokens, then wakeAll will be true.
// wantTokens must be > 0; if it's zero then wake state should not be modified.
func (h *Handler) getWakeLevel(key batchKey, wantTokens int32) (wakeUpTo int64, wakeAll bool) {
	s := h.waiterShardFor(key)
	s.lock.Lock()
	defer s.lock.Unlock()

	wt, ok := s.waiters[key]
	if !ok {
		return 0, true // no waiters at all
	}

	remaining := false
	wt.Scan(func(entry waiterEntry) bool {
		if wakeUpTo != 0 {
			// we satisfied all our tokens, but there are more waiters
			remaining = true
			return false
		}
		if wantTokens -= entry.tokens; wantTokens <= 0 {
			// satisfied all tokens. set wakeUpTo but keep scanning to see
			// if there are more waiters.
			wakeUpTo = entry.wakePriority
		}
		return true
	})
	// if we have any waiters we _don't_ want to wake, do a staged wake
	if remaining {
		return wakeUpTo, false
	}
	// otherwise wake all
	return 0, true
}
