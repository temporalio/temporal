package concurrency

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

type batchKey struct {
	namespaceID string
	key         string
}

type batchReq struct {
	ctx context.Context
	req *fcpb.ConcurrencyServiceBatchRequest
}

type chasmReq struct {
	items          []batchReq
	reserveTimeout time.Duration
}

type batchRes struct {
	res *fcpb.ConcurrencyServiceBatchResponse
	err error
}

type Handler struct {
	fcpb.UnimplementedConcurrencyServiceServer

	logger         log.Logger
	longPollOpts   dynamicconfig.TypedPropertyFn[WaitLongPollOptions]
	reserveTimeout dynamicconfig.DurationPropertyFn
}

func NewHandler(
	logger log.Logger,
	dc *dynamicconfig.Collection,
) *Handler {
	h := &Handler{
		logger:         logger,
		longPollOpts:   WaitLongPoll.Get(dc),
		reserveTimeout: ReserveTimeout.Get(dc),
	}
	return h
}

func initFn(_ chasm.MutableContext, creq chasmReq) (*Component, error) {
	// For now (whole-queue limits), we should always have an initial config here.
	// For keyed limiters, this may be missing and set via api later.
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

func updateFn(c *Component, cctx chasm.MutableContext, creq chasmReq) ([]*fcpb.ConcurrencyServiceBatchResponse, error) {
	now := cctx.Now(c)

	// allocate new protos for responses
	ress := make([]*fcpb.ConcurrencyServiceBatchResponse, len(creq.items))
	for i := range ress {
		ress[i] = &fcpb.ConcurrencyServiceBatchResponse{}
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
		for _, slotID := range it.req.GetReleaseSlots() {
			c.release(slotID)
		}
	}

	// apply commits
	for i, it := range creq.items {
		for _, slotID := range it.req.GetCommitSlots() {
			success := c.commit(slotID)
			ress[i].CommitSuccess = append(ress[i].CommitSuccess, success)
		}
	}

	// apply cancels
	for _, it := range creq.items {
		for _, slotID := range it.req.GetCancelReservationSlots() {
			c.cancelReservation(slotID)
		}
	}

	// apply reserves
	for i, it := range creq.items {
		for _, slotID := range it.req.GetReserveSlots() {
			success := c.reserve(slotID, now, creq.reserveTimeout)
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
		c.WakeAll = true
	}

	// capture Generation after maybe having incremented
	availableSlots := c.availableSlots()
	for i := range creq.items {
		ress[i].Generation = c.Generation
		// TODO(fc): per-client hints instead of giving everyone the same one
		ress[i].AvailableSlotsHint = availableSlots
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
		items:          items,
		reserveTimeout: h.reserveTimeout(),
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
	ress []*fcpb.ConcurrencyServiceBatchResponse,
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

func (h *Handler) Batch(ctx context.Context, req *fcpb.ConcurrencyServiceBatchRequest) (retRes *fcpb.ConcurrencyServiceBatchResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	results := h.applyBatch(batchKey{
		namespaceID: req.GetNamespaceId(),
		key:         req.GetKey(),
	}, []batchReq{{ctx: ctx, req: req}})
	res := results[0]
	if res.res == nil && res.err == nil {
		res.err = serviceerror.NewInternal("nil response")
	}
	return res.res, res.err
}

func (h *Handler) Wait(ctx context.Context, req *fcpb.ConcurrencyServiceWaitRequest) (retRes *fcpb.ConcurrencyServiceWaitResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	req = common.CloneProto(req)
	req.RequestedWakeTokens = max(req.RequestedWakeTokens, 1) // assume one token if unset

	// Note that the maximum timeout here has an effect besides just capping rpc duration:
	// If a caller has Reserved the last slot, and crashes before Commit, then the reservation
	// will expire. But expiration is handled lazily, it doesn't cause a state transition of
	// the component. So a Wait call that's waiting for that expiry will only notice when it
	// re-enters PollComponent, which is only once per this timeout.
	longPollOpts := h.longPollOpts()
	ctx, cancel := contextutil.WithDeadlineBuffer(ctx, longPollOpts.Timeout, longPollOpts.Buffer)
	defer cancel()

	// loop to do internal retries with increased generation
	for {
		// CHASM requires that PollComponent be monotonic: if it returns true at some state,
		// then it should return true at future states. Our predicate is "is state.wake_up_to
		// >= req.start_time" (or state.wake_all means wake_up_to = forever in the future). But
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
			func(c *Component, cctx chasm.Context, req *fcpb.ConcurrencyServiceWaitRequest) (*fcpb.ConcurrencyServiceWaitResponse, bool, error) {
				generation, tokens, ready := c.poll(cctx.Now(c), req.Generation, req.RequestedWakeTokens)
				if !ready {
					return nil, false, nil
				}
				return &fcpb.ConcurrencyServiceWaitResponse{Generation: generation, WakeTokens: tokens}, true, nil
			},
			req,
		)
		if err != nil {
			if common.IsContextDeadlineExceededErr(err) {
				// timed out without becoming ready
				return &fcpb.ConcurrencyServiceWaitResponse{Generation: req.Generation}, nil
			}
			return nil, err
		} else if res == nil {
			// chasm-imposed timeout
			return &fcpb.ConcurrencyServiceWaitResponse{Generation: req.Generation}, nil
		} else if res.Generation != req.Generation {
			// try again on server with new generation
			req.Generation = res.Generation
			continue
		}
		return res, nil
	}
}
