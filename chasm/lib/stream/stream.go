// Package stream implements the native-streams chasm library — a
// server-side primitive for incremental result delivery on top of which
// `temporalio.streams` / equivalent SDK surfaces are built.
//
// See ../../../../design-overview.html (in the native-streams prototype
// repo) for the design and ../../../../spec/StreamCommit.tla for the
// TLA+ model the implementation is being aligned against.
package stream

import (
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// Stream is the root chasm component for a native stream.  Each Stream is
// a chasm Execution; its identity is (namespace_id, stream_id).
//
// Fields map one-to-one to spec state variables in
// ../../../../spec/StreamCommit.tla.  Naming discipline (see
// ../../../../spec/SPEC_TO_CODE.md): when this struct changes, the spec
// row updates in the same commit.
type Stream struct {
	chasm.UnimplementedComponent

	// Persisted top-level state: visibility frontier, lifecycle, ownership
	// epoch, retention caps, segment policy, audit identity.
	// Spec state: head_offset / base_offset / committed_txn_id / closed /
	// owner_epoch.
	*streampb.StreamState

	// Per-publisher dedup state.  Rolling per-publisher: (last_seq,
	// last_seen, first_offset, item_count).  TTL'd by PublisherDedupSweepTask.
	// Spec state: pub_state[p].
	Publishers chasm.Map[string, *PublisherState]

	// In-flight prepared publishes, keyed by txn_id.  Drained by
	// SweepExpiredTask, CommitPublish, AbortInflight, or CloseCleanup.
	// Spec state: inflight[t].
	Inflights chasm.Map[int64, *InflightPublish]

	// Per-subscription state.  Sub-component; lifecycle is independent of
	// the publish path.  Spec state: subscribers[s].
	Subscriptions chasm.Map[string, *Subscription]

	// Search-attribute facing visibility state.  Populated from the same
	// fields as DescribeStream.
	Visibility chasm.Field[*chasm.Visibility]
}

// PublisherState is one entry in the Publishers map.  Wraps the proto so
// the chasm.Field machinery can serialize it independently.
//
// Spec correspondence: pub_state[p] entry.
type PublisherState struct {
	chasm.UnimplementedComponent
	*streampb.PublisherState
}

// LifecycleState: per-publisher dedup state is "running" while it
// exists; removal happens via the publisher_ttl sweeper (entry deleted
// from the map outright).
func (*PublisherState) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// InflightPublish is one entry in the Inflights map.  See
// exactly-once.html §6 protocol.
//
// Spec correspondence: inflight[t] entry.
type InflightPublish struct {
	chasm.UnimplementedComponent
	*streampb.InflightPublishState
}

// LifecycleState: inflights are running until committed, aborted, or
// swept.  Removal happens via map delete in any of those paths.
func (*InflightPublish) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Subscription is one entry in the Subscriptions map.  Its own
// sub-component so it can carry per-subscription side-effect tasks
// (delivery, drain, close).
//
// Spec correspondence: subscribers[s] entry.  Defined in subscription.go.

// Default values.  Per design-overview.html §3 segment-size policy and
// the dedup-TTL contract in exactly-once.html.
const (
	defaultSegmentMaxItems = int64(1024)
	defaultSegmentMaxBytes = int64(1 << 20) // 1 MB
	defaultPublisherTTL    = 15 * time.Minute

	// inflightGraceDuration is the wall-clock budget from PreparePublish
	// to CommitPublish.  Inflights past this are aborted by the sweeper.
	// Sized to absorb the RPC + WriteSegmentsTentative round-trip with
	// healthy margin.  See exactly-once.html §6 protocol.
	inflightGraceDuration = 10 * time.Minute
)

// Typed errors visible to publish-path callers.  See design-overview.html
// §4 API table and §6 typed-error taxonomy.
var (
	ErrStreamClosed         = serviceerror.NewFailedPrecondition("stream is closed")
	ErrStreamDeleted        = serviceerror.NewNotFound("stream has been deleted")
	ErrPublishAbortedClose  = serviceerror.NewFailedPrecondition("publish aborted because stream was closed")
	ErrPublishAbortedOwner  = serviceerror.NewUnavailable("publish aborted because shard ownership changed")
	ErrTruncateConflict     = serviceerror.NewFailedPrecondition("truncate conflict: base advanced past prepared inflight")
	ErrTruncateBeyondHead   = serviceerror.NewInvalidArgument("truncate: up_to_offset exceeds head_offset")
	ErrTruncateBeyondBase   = serviceerror.NewInvalidArgument("truncate: up_to_offset is below base_offset")
	ErrTruncateBlockedBySub = serviceerror.NewFailedPrecondition("truncate blocked by active subscriber; use force=true to override")
	ErrSegmentsMissing      = serviceerror.NewFailedPrecondition("proof-of-write check failed: tentative segments missing or hash mismatch")
	ErrOrderingGap          = serviceerror.NewFailedPrecondition("ordering gap: concurrent publisher won the head race")
	ErrPublishExpired       = serviceerror.NewFailedPrecondition("prepared publish expired before commit")
	ErrPublishGroupAborted  = serviceerror.NewFailedPrecondition("publish aborted: group-commit peer failed a guard")
	ErrOffsetTruncated      = serviceerror.NewFailedPrecondition("requested offset is below base_offset (truncated)")
)

// NewStream constructs the root Stream component at CreateStream time.
// The caller (handler.CreateStream) wraps this in chasm.StartExecution.
//
// Spec correspondence: Init.
func NewStream(
	ctx chasm.MutableContext,
	req *streampb.CreateStreamRequest,
) (*Stream, error) {
	now := ctx.Now(nil)

	segMaxItems := req.SegmentMaxItems
	if segMaxItems == 0 {
		segMaxItems = defaultSegmentMaxItems
	}
	segMaxBytes := req.SegmentMaxBytes
	if segMaxBytes == 0 {
		segMaxBytes = defaultSegmentMaxBytes
	}
	pubTTL := defaultPublisherTTL
	if req.PublisherTtl != nil {
		pubTTL = req.PublisherTtl.AsDuration()
	}

	state := &streampb.StreamState{
		HeadOffset:        0,
		BaseOffset:        0,
		CommittedTxnId:    0,
		OwnerEpoch:        0,
		Closed:            false,
		CreatedBy:         req.CreatedBy,
		CreatedTime:       timestampNow(now),
		OwnerWorkflowId:   req.OwnerWorkflowId,
		OwnerRunId:        req.OwnerRunId,
		RetentionMaxBytes: req.RetentionMaxBytes,
		RetentionMaxItems: req.RetentionMaxItems,
		SegmentMaxItems:   segMaxItems,
		SegmentMaxBytes:   segMaxBytes,
		PublisherTtl:      durationProto(pubTTL),
		NextTxnId:         1,
	}

	s := &Stream{
		StreamState:   state,
		Publishers:    make(chasm.Map[string, *PublisherState]),
		Inflights:     make(chasm.Map[int64, *InflightPublish]),
		Subscriptions: make(chasm.Map[string, *Subscription]),
	}

	// Schedule the sweepers.  Both pure tasks; run under the chasm
	// execution write lock with no I/O.
	scheduleSweepExpired(ctx, s)
	schedulePublisherDedupSweep(ctx, s)

	// If an owner-workflow link is set, schedule the side-effect task that
	// listens for the owner's terminal state.  Implementation TBD —
	// requires owner-workflow callback hookup that lives outside this
	// package.

	return s, nil
}

// LifecycleState reports closed / running to the chasm framework.
func (s *Stream) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if s.StreamState.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

// ContextMetadata is the RootComponent hook for surfacing chasm metadata
// to gRPC context.  Streams don't currently surface anything (the
// stream_id is already the BusinessID); future fields like attached
// owner_workflow_id could go here.
func (s *Stream) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate is the RootComponent hook for forceful termination.  Maps
// to Close with close_reason = DELETED.
func (s *Stream) Terminate(
	ctx chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if !s.StreamState.Closed {
		_ = s.Close(ctx, CloseInput{
			ClosedBy:    "system-terminate",
			CloseReason: streampb.STREAM_CLOSE_REASON_DELETED,
		})
	}
	return chasm.TerminateComponentResponse{}, nil
}

// closeGuard is the close-guard predicate every mutator hits.  Returns
// the appropriate typed error if the stream is closed.  Spec
// correspondence: ~closed predicate on PreparePublish / CommitPublish /
// GroupPrepare / GroupCommit / Truncate / ForceTruncate.
func (s *Stream) closeGuard() error {
	if s.StreamState.Closed {
		return ErrStreamClosed
	}
	return nil
}

// allocateTxnID returns the next monotone txn_id and advances the
// counter.  Stream-global monotone across owner_epoch bumps per the
// exactly-once.html load-bearing invariants.
func (s *Stream) allocateTxnID() int64 {
	id := s.StreamState.NextTxnId
	s.StreamState.NextTxnId = id + 1
	return id
}
