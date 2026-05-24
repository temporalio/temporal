package stream

import (
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// PreparePublishInput is the input to Stream.PreparePublish.
//
// Spec correspondence: PreparePublish(p, s, n, h) action arguments.
type PreparePublishInput struct {
	PublisherID string
	Sequence    uint64
	ItemCount   int32
	// PayloadHash is the SDK-computed hash over the items' codec-applied
	// bytes.  Recorded on the inflight; verified by the proof-of-write
	// check at commit time (M4).
	PayloadHash []byte
}

// PreparePublishOutput carries the txn_id and assigned offset range so
// the caller can subsequently call CommitPublish.  On dedup-replay,
// IsDedupReplay is true and the prior (first_offset, item_count) is
// returned.
type PreparePublishOutput struct {
	TxnID          int64
	FirstOffset    int64
	ItemCount      int32
	FirstSegmentID int64
	LastSegmentID  int64
	IsDedupReplay  bool
}

// PreparePublish runs the chasm transition for step 1 of the cross-facet
// commit protocol.  Guards, in order:
//   - close-guard (~closed)
//   - per-publisher monotonic sequence (drop on seq < last_seq → error,
//     seq == last_seq → dedup-replay)
//
// On success: allocates a fresh txn_id, reserves
// [head_offset, head_offset+n), records an InflightPublish with
// expires_at = now + grace and prep_epoch = owner_epoch.  Does NOT
// advance head_offset.
//
// Spec correspondence: PreparePublish action in StreamCommit.tla L164.
func (s *Stream) PreparePublish(
	ctx chasm.MutableContext,
	input PreparePublishInput,
) (PreparePublishOutput, error) {
	if err := s.closeGuard(); err != nil {
		return PreparePublishOutput{}, err
	}
	if input.ItemCount <= 0 {
		return PreparePublishOutput{}, ErrInvalidPublishBatchEmpty
	}

	// Dedup short-circuit / regression check.
	if pubField, ok := s.Publishers[input.PublisherID]; ok {
		ps, ok := pubField.TryGet(ctx)
		if ok && ps != nil && ps.PublisherState != nil {
			if input.Sequence < ps.LastSeq {
				return PreparePublishOutput{}, ErrSeqRegression
			}
			if input.Sequence == ps.LastSeq {
				return PreparePublishOutput{
					FirstOffset:   ps.FirstOffset,
					ItemCount:     int32(ps.ItemCount),
					IsDedupReplay: true,
				}, nil
			}
		}
	}

	now := ctx.Now(nil)
	txnID := s.allocateTxnID()
	firstOffset := s.StreamState.HeadOffset
	endOffset := firstOffset + int64(input.ItemCount)
	firstSegmentID, lastSegmentID := s.segmentRangeFor(firstOffset, endOffset)

	ifl := &InflightPublish{
		InflightPublishState: &streampb.InflightPublishState{
			PublisherId:    input.PublisherID,
			Sequence:       input.Sequence,
			FirstOffset:    firstOffset,
			EndOffset:      endOffset,
			FirstSegmentId: firstSegmentID,
			LastSegmentId:  lastSegmentID,
			PayloadHash:    input.PayloadHash,
			PreparedAt:     timestampNow(now),
			ExpiresAt:      timestampNow(now.Add(inflightGraceDuration)),
			PrepEpoch:      s.StreamState.OwnerEpoch,
		},
	}
	s.Inflights[txnID] = chasm.NewComponentField(ctx, ifl)

	return PreparePublishOutput{
		TxnID:          txnID,
		FirstOffset:    firstOffset,
		ItemCount:      input.ItemCount,
		FirstSegmentID: firstSegmentID,
		LastSegmentID:  lastSegmentID,
	}, nil
}

// segmentRangeFor returns (first_segment_id, last_segment_id) for an
// offset range under the configured segment-size policy.
func (s *Stream) segmentRangeFor(firstOffset, endOffset int64) (int64, int64) {
	segMax := s.StreamState.SegmentMaxItems
	if segMax == 0 {
		segMax = defaultSegmentMaxItems
	}
	firstSeg := firstOffset / segMax
	lastSeg := (endOffset - 1) / segMax
	return firstSeg, lastSeg
}

// CommitPublishInput is the input to Stream.CommitPublish.
//
// Spec correspondence: CommitPublish(t) action arguments.
type CommitPublishInput struct {
	TxnID int64
	// ProofOfWriteVerified is set by the caller after it has read back
	// the tentative segment rows for (stream, txn_id) and verified
	// coverage + payload-hash equality.  The chasm transition trusts
	// this signal but re-runs the chasm-side guards independently.
	//
	// M3 leaves this caller-driven; M4 will collapse the read+commit
	// into a single SQL transaction on PG/MySQL/SQLite and run the
	// per-txn CQL scan on Cassandra immediately pre-CAS.
	ProofOfWriteVerified bool
}

// CommitPublishOutput is the committed offset range.
type CommitPublishOutput struct {
	FirstOffset int64
	ItemCount   int32
}

// CommitPublish runs step 3 of the cross-facet commit protocol.  Guards:
//   - inflight entry exists
//   - close-guard (~closed → PublishAbortedByClose)
//   - expiry guard
//   - caller-provided proof-of-write boolean
//   - CAS: head_offset == ifl.first_offset; base_offset <= ifl.first_offset
//   - ownership-epoch fence: ifl.prep_epoch == owner_epoch
//   - dedup-replay short-circuit
//
// On success: writes publisher_map, advances head_offset, sets
// committed_txn_id, removes the inflight, returns the committed range.
// On any guard failure: in-band abortInflight(t) — schedules the
// segment-row cleanup task and removes the inflight.
//
// Spec correspondence: CommitPublish action in StreamCommit.tla L319.
func (s *Stream) CommitPublish(
	ctx chasm.MutableContext,
	input CommitPublishInput,
) (CommitPublishOutput, error) {
	iflField, ok := s.Inflights[input.TxnID]
	if !ok {
		return CommitPublishOutput{}, ErrInflightNotFound
	}
	ifl, ok := iflField.TryGet(ctx)
	if !ok || ifl == nil || ifl.InflightPublishState == nil {
		return CommitPublishOutput{}, ErrInflightNotFound
	}
	state := ifl.InflightPublishState

	if s.StreamState.Closed {
		s.abortInflight(ctx, input.TxnID, ifl)
		return CommitPublishOutput{}, ErrPublishAbortedClose
	}

	now := ctx.Now(nil)
	if state.ExpiresAt.AsTime().Before(now) {
		s.abortInflight(ctx, input.TxnID, ifl)
		return CommitPublishOutput{}, ErrPublishExpired
	}
	if state.PrepEpoch != s.StreamState.OwnerEpoch {
		s.abortInflight(ctx, input.TxnID, ifl)
		return CommitPublishOutput{}, ErrPublishAbortedOwner
	}
	if !input.ProofOfWriteVerified {
		s.abortInflight(ctx, input.TxnID, ifl)
		return CommitPublishOutput{}, ErrSegmentsMissing
	}
	if s.StreamState.HeadOffset != state.FirstOffset {
		s.abortInflight(ctx, input.TxnID, ifl)
		return CommitPublishOutput{}, ErrOrderingGap
	}
	if s.StreamState.BaseOffset > state.FirstOffset {
		s.abortInflight(ctx, input.TxnID, ifl)
		return CommitPublishOutput{}, ErrTruncateConflict
	}

	// Dedup-replay short-circuit: pub_state may have advanced from a
	// concurrent commit for the same publisher.
	if pubField, ok := s.Publishers[state.PublisherId]; ok {
		ps, ok := pubField.TryGet(ctx)
		if ok && ps != nil && ps.PublisherState != nil && state.Sequence <= ps.LastSeq {
			s.abortInflight(ctx, input.TxnID, ifl)
			return CommitPublishOutput{
				FirstOffset: ps.FirstOffset,
				ItemCount:   int32(ps.ItemCount),
			}, nil
		}
	}

	itemCount := int32(state.EndOffset - state.FirstOffset)
	s.Publishers[state.PublisherId] = chasm.NewComponentField(ctx, &PublisherState{
		PublisherState: &streampb.PublisherState{
			LastSeq:     state.Sequence,
			LastSeen:    timestampNow(now),
			FirstOffset: state.FirstOffset,
			ItemCount:   int64(itemCount),
		},
	})
	s.StreamState.HeadOffset = state.EndOffset
	s.StreamState.CommittedTxnId = input.TxnID
	delete(s.Inflights, input.TxnID)

	return CommitPublishOutput{
		FirstOffset: state.FirstOffset,
		ItemCount:   itemCount,
	}, nil
}

// abortInflight is the in-band abort cleanup: remove the inflight from
// the map and schedule the side-effect task to delete tentative rows.
//
// Spec correspondence: AbortInflight(t) action.
func (s *Stream) abortInflight(
	ctx chasm.MutableContext,
	txnID int64,
	ifl *InflightPublish,
) {
	if ifl != nil && ifl.InflightPublishState != nil {
		ctx.AddTask(s, chasm.TaskAttributes{}, &streampb.AbortCleanupTask{
			TxnId:          txnID,
			FirstSegmentId: ifl.FirstSegmentId,
			LastSegmentId:  ifl.LastSegmentId,
		})
	}
	delete(s.Inflights, txnID)
}

// Additional typed errors used only by the publish path.
var (
	ErrInvalidPublishBatchEmpty = serviceerrorInvalidArgument("publish batch must contain at least one item")
	ErrInflightNotFound         = serviceerrorNotFound("inflight not found (already committed, aborted, or swept)")
	ErrSeqRegression            = serviceerrorInvalidArgument("sequence regression: seq < last_seq for this publisher")
)
