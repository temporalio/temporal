package stream

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// SweepExpiredTaskHandler runs periodically and aborts expired inflights.
// Pure task: holds the chasm execution write lock, no I/O.
//
// Spec correspondence: SweepExpired(t).
type SweepExpiredTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func NewSweepExpiredTaskHandler() *SweepExpiredTaskHandler { return &SweepExpiredTaskHandler{} }

func (h *SweepExpiredTaskHandler) Validate(
	_ chasm.Context,
	_ *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.SweepExpiredTask,
) (bool, error) {
	// Always proceed; the Execute step iterates and may no-op if nothing
	// is expired.
	return true, nil
}

func (h *SweepExpiredTaskHandler) Execute(
	ctx chasm.MutableContext,
	s *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.SweepExpiredTask,
) error {
	now := ctx.Now(nil)
	for txnID, field := range s.Inflights {
		ifl, ok := field.TryGet(ctx)
		if !ok || ifl == nil || ifl.InflightPublishState == nil {
			continue
		}
		if ifl.ExpiresAt.AsTime().Before(now) {
			ctx.AddTask(s, chasm.TaskAttributes{}, &streampb.AbortCleanupTask{
				TxnId:          txnID,
				FirstSegmentId: ifl.FirstSegmentId,
				LastSegmentId:  ifl.LastSegmentId,
			})
			delete(s.Inflights, txnID)
		}
	}
	scheduleSweepExpired(ctx, s)
	return nil
}

// AbortCleanupTaskHandler deletes tentative segment rows under a
// (stream, txn_id) prefix in the stream_segments facet.  Side-effect task.
type AbortCleanupTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.AbortCleanupTask]
}

func NewAbortCleanupTaskHandler() *AbortCleanupTaskHandler { return &AbortCleanupTaskHandler{} }

func (h *AbortCleanupTaskHandler) Validate(
	_ chasm.Context,
	_ *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.AbortCleanupTask,
) (bool, error) {
	return true, nil
}

func (h *AbortCleanupTaskHandler) Execute(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.AbortCleanupTask,
) error {
	// TODO(M2): call into the stream_segments persistence facet to delete
	// rows under (execution_key, txn_id).  Persistence-driver method
	// not implemented yet.
	return nil
}

// CloseCleanupTaskHandler runs phase-2 of Close: drain inflight and
// subscription maps in chunks.  Pure task.
//
// Spec correspondence: CloseCleanup(t) + CloseSubscriptionDrain(s).
type CloseCleanupTaskHandler struct {
	chasm.PureTaskHandlerBase
	chunkSize int
}

func NewCloseCleanupTaskHandler() *CloseCleanupTaskHandler {
	return &CloseCleanupTaskHandler{chunkSize: 100}
}

func (h *CloseCleanupTaskHandler) Validate(
	_ chasm.Context,
	s *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.CloseCleanupTask,
) (bool, error) {
	if !s.StreamState.Closed {
		return false, nil
	}
	if len(s.Inflights) == 0 && len(s.Subscriptions) == 0 {
		return false, nil
	}
	return true, nil
}

func (h *CloseCleanupTaskHandler) Execute(
	ctx chasm.MutableContext,
	s *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.CloseCleanupTask,
) error {
	drained := 0
	for txnID, field := range s.Inflights {
		if drained >= h.chunkSize {
			break
		}
		if ifl, ok := field.TryGet(ctx); ok && ifl != nil && ifl.InflightPublishState != nil {
			ctx.AddTask(s, chasm.TaskAttributes{}, &streampb.AbortCleanupTask{
				TxnId:          txnID,
				FirstSegmentId: ifl.FirstSegmentId,
				LastSegmentId:  ifl.LastSegmentId,
			})
		}
		delete(s.Inflights, txnID)
		drained++
	}
	for subID := range s.Subscriptions {
		if drained >= h.chunkSize {
			break
		}
		delete(s.Subscriptions, subID)
		drained++
	}
	if len(s.Inflights) > 0 || len(s.Subscriptions) > 0 {
		ctx.AddTask(s, chasm.TaskAttributes{}, &streampb.CloseCleanupTask{})
	}
	return nil
}

// OwnerWorkflowCloseTaskHandler runs when the linked owner workflow
// completes.  Side-effect task.
type OwnerWorkflowCloseTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.OwnerWorkflowCloseTask]
}

func NewOwnerWorkflowCloseTaskHandler() *OwnerWorkflowCloseTaskHandler {
	return &OwnerWorkflowCloseTaskHandler{}
}

func (h *OwnerWorkflowCloseTaskHandler) Validate(
	_ chasm.Context,
	s *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.OwnerWorkflowCloseTask,
) (bool, error) {
	return !s.StreamState.Closed, nil
}

func (h *OwnerWorkflowCloseTaskHandler) Execute(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.OwnerWorkflowCloseTask,
) error {
	// TODO(M3): call back via chasm.UpdateComponent to run the Close
	// transition with close_reason = OWNER_WORKFLOW_DONE.
	return nil
}

// PublisherDedupSweepTaskHandler prunes per-publisher dedup state whose
// last_seen is older than publisher_ttl.  Pure task.
type PublisherDedupSweepTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func NewPublisherDedupSweepTaskHandler() *PublisherDedupSweepTaskHandler {
	return &PublisherDedupSweepTaskHandler{}
}

func (h *PublisherDedupSweepTaskHandler) Validate(
	_ chasm.Context,
	_ *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.PublisherDedupSweepTask,
) (bool, error) {
	return true, nil
}

func (h *PublisherDedupSweepTaskHandler) Execute(
	ctx chasm.MutableContext,
	s *Stream,
	_ chasm.TaskAttributes,
	_ *streampb.PublisherDedupSweepTask,
) error {
	ttl := defaultPublisherTTL
	if s.StreamState.PublisherTtl != nil {
		ttl = s.StreamState.PublisherTtl.AsDuration()
	}
	cutoff := ctx.Now(nil).Add(-ttl)
	for pubID, field := range s.Publishers {
		ps, ok := field.TryGet(ctx)
		if !ok || ps == nil || ps.PublisherState == nil {
			continue
		}
		if ps.LastSeen.AsTime().Before(cutoff) {
			delete(s.Publishers, pubID)
		}
	}
	schedulePublisherDedupSweep(ctx, s)
	return nil
}

// DeliveryTaskHandler drives delivery for one subscription.  Side-effect.
type DeliveryTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.DeliveryTask]
}

func NewDeliveryTaskHandler() *DeliveryTaskHandler { return &DeliveryTaskHandler{} }

func (h *DeliveryTaskHandler) Validate(
	ctx chasm.Context,
	s *Stream,
	_ chasm.TaskAttributes,
	payload *streampb.DeliveryTask,
) (bool, error) {
	field, ok := s.Subscriptions[payload.SubscriptionId]
	if !ok {
		return false, nil
	}
	sub, ok := field.TryGet(ctx)
	if !ok || sub == nil || sub.SubscriptionState == nil {
		return false, nil
	}
	if sub.SubscriptionState.AtCapacity {
		return false, nil
	}
	return true, nil
}

func (h *DeliveryTaskHandler) Execute(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.DeliveryTask,
) error {
	// TODO(M6): actual delivery.
	return nil
}

// scheduleSweepExpired (re-)arms the SweepExpiredTask.
func scheduleSweepExpired(ctx chasm.MutableContext, s *Stream) {
	ctx.AddTask(s, chasm.TaskAttributes{
		ScheduledTime: ctx.Now(nil).Add(inflightGraceDuration / 2),
	}, &streampb.SweepExpiredTask{})
}

// schedulePublisherDedupSweep (re-)arms the PublisherDedupSweepTask.
func schedulePublisherDedupSweep(ctx chasm.MutableContext, s *Stream) {
	ttl := defaultPublisherTTL
	if s.StreamState.PublisherTtl != nil {
		ttl = s.StreamState.PublisherTtl.AsDuration()
	}
	ctx.AddTask(s, chasm.TaskAttributes{
		ScheduledTime: ctx.Now(nil).Add(ttl / 4),
	}, &streampb.PublisherDedupSweepTask{})
}

func timestampNow(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }
func durationProto(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}
