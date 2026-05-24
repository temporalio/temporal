package stream

import (
	"sort"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// GroupPrepareItem describes one item in a group-prepare batch.
type GroupPrepareItem struct {
	PublisherID string
	Sequence    uint64
	ItemCount   int32
	PayloadHash []byte
}

// GroupPrepareInput is the input to Stream.GroupPrepare.  Items are
// assigned consecutive offset ranges in iteration order in a single
// chasm transition.
//
// Spec correspondence: GroupPrepare(itemSeq) action.
type GroupPrepareInput struct {
	Items []GroupPrepareItem
}

// GroupPrepareItemOutput is the per-item outcome from GroupPrepare.
type GroupPrepareItemOutput struct {
	TxnID         int64
	FirstOffset   int64
	ItemCount     int32
	IsDedupReplay bool
}

// GroupPrepareOutput pairs each output by the input index so callers can
// trivially route results back to the originating publish call.
type GroupPrepareOutput struct {
	Items []GroupPrepareItemOutput
}

// GroupPrepare runs the chasm transition for the single-transition
// group-prepare path.  Per the design (exactly-once.html §6 N-entry
// group-commit), the dispatcher pre-filters duplicates so the group
// only contains non-duplicate (publisher_id, sequence) pairs.
//
// Guards:
//   - close-guard (~closed)
//   - per-item monotonic-sequence (any seq < last_seq → error for that item,
//     but the group as a whole still proceeds — duplicates short-circuit)
//   - no two items in the group share the same (publisher, sequence)
//
// On success: assigns consecutive offset ranges in iteration order
// starting at head_offset; allocates monotone txn_ids in the same order
// (so the GroupCommit's "max txn_id" rule is equivalent to "last
// member's txn_id").  Does NOT advance head_offset.
//
// Spec correspondence: GroupPrepare action in StreamCommit.tla L201.
func (s *Stream) GroupPrepare(
	ctx chasm.MutableContext,
	input GroupPrepareInput,
) (GroupPrepareOutput, error) {
	if err := s.closeGuard(); err != nil {
		return GroupPrepareOutput{}, err
	}
	n := len(input.Items)
	if n < 2 {
		return GroupPrepareOutput{}, ErrGroupPrepareUnderMin
	}
	// Reject same-(publisher,seq) duplicates within the group.
	type pubKey struct {
		p string
		s uint64
	}
	seen := make(map[pubKey]struct{}, n)
	for _, item := range input.Items {
		if item.ItemCount <= 0 {
			return GroupPrepareOutput{}, ErrInvalidPublishBatchEmpty
		}
		k := pubKey{item.PublisherID, item.Sequence}
		if _, dup := seen[k]; dup {
			return GroupPrepareOutput{}, ErrGroupPrepareDuplicate
		}
		seen[k] = struct{}{}
	}

	now := ctx.Now(nil)
	out := GroupPrepareOutput{Items: make([]GroupPrepareItemOutput, n)}
	offset := s.StreamState.HeadOffset

	for i, item := range input.Items {
		// Dedup short-circuit per item.
		if pubField, ok := s.Publishers[item.PublisherID]; ok {
			ps, ok := pubField.TryGet(ctx)
			if ok && ps != nil && ps.PublisherState != nil {
				if item.Sequence < ps.LastSeq {
					return GroupPrepareOutput{}, ErrSeqRegression
				}
				if item.Sequence == ps.LastSeq {
					out.Items[i] = GroupPrepareItemOutput{
						FirstOffset:   ps.FirstOffset,
						ItemCount:     int32(ps.ItemCount),
						IsDedupReplay: true,
					}
					continue
				}
			}
		}

		txnID := s.allocateTxnID()
		firstSeg, lastSeg := s.segmentRangeFor(offset, offset+int64(item.ItemCount))
		ifl := &InflightPublish{
			InflightPublishState: &streampb.InflightPublishState{
				PublisherId:    item.PublisherID,
				Sequence:       item.Sequence,
				FirstOffset:    offset,
				EndOffset:      offset + int64(item.ItemCount),
				FirstSegmentId: firstSeg,
				LastSegmentId:  lastSeg,
				PayloadHash:    item.PayloadHash,
				PreparedAt:     timestampNow(now),
				ExpiresAt:      timestampNow(now.Add(inflightGraceDuration)),
				PrepEpoch:      s.StreamState.OwnerEpoch,
			},
		}
		s.Inflights[txnID] = chasm.NewComponentField(ctx, ifl)
		out.Items[i] = GroupPrepareItemOutput{
			TxnID:       txnID,
			FirstOffset: offset,
			ItemCount:   item.ItemCount,
		}
		offset += int64(item.ItemCount)
	}
	return out, nil
}

// GroupCommitInput is the input to Stream.GroupCommit.  All TxnIDs must
// reference active inflights and all must have ProofOfWriteVerified=true
// (the caller verified them; see M3 publish.go for the rationale on the
// caller-driven proof-of-write boolean).
//
// Spec correspondence: GroupCommit(S) action arguments.
type GroupCommitInput struct {
	TxnIDs []int64
}

// GroupCommitOutput is the per-txn outcome.  Ordered to match TxnIDs.
type GroupCommitOutput struct {
	Items []CommitPublishOutput
}

// GroupCommit runs the chasm transition for the N-entry atomic commit.
// All-or-none semantics: every guard must pass for every member, or
// nothing commits and every member is aborted via the in-band cleanup
// (the GroupAbort path).
//
// Guards (same set as CommitPublish, applied to every member):
//   - inflight entry exists
//   - close-guard
//   - expiry guard per entry
//   - ownership-epoch fence per entry
//   - contiguity: ordered by first_offset, head_offset == first_offset
//     of leader, every successor's first_offset == predecessor's end_offset
//   - CAS via the leader's first_offset
//   - dedup short-circuit per entry
//
// On success: writes pub_state for every member, advances head_offset
// to the trailing member's end_offset, sets committed_txn_id to the
// largest txn_id in the group.
//
// Spec correspondence: GroupCommit(S) action in StreamCommit.tla L347.
func (s *Stream) GroupCommit(
	ctx chasm.MutableContext,
	input GroupCommitInput,
) (GroupCommitOutput, error) {
	if err := s.closeGuard(); err != nil {
		return GroupCommitOutput{}, ErrPublishAbortedClose
	}
	n := len(input.TxnIDs)
	if n < 2 {
		return GroupCommitOutput{}, ErrGroupCommitUnderMin
	}

	// Resolve inflights; reject if any is missing.
	type entry struct {
		txnID int64
		ifl   *InflightPublish
	}
	entries := make([]entry, 0, n)
	for _, txnID := range input.TxnIDs {
		f, ok := s.Inflights[txnID]
		if !ok {
			return GroupCommitOutput{}, ErrInflightNotFound
		}
		ifl, ok := f.TryGet(ctx)
		if !ok || ifl == nil || ifl.InflightPublishState == nil {
			return GroupCommitOutput{}, ErrInflightNotFound
		}
		entries = append(entries, entry{txnID, ifl})
	}

	// Sort by first_offset.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ifl.FirstOffset < entries[j].ifl.FirstOffset
	})

	// Per-entry expiry / ownership / hash / dedup guards.
	now := ctx.Now(nil)
	for _, e := range entries {
		st := e.ifl.InflightPublishState
		if st.ExpiresAt.AsTime().Before(now) {
			return GroupCommitOutput{}, ErrPublishExpired
		}
		if st.PrepEpoch != s.StreamState.OwnerEpoch {
			return GroupCommitOutput{}, ErrPublishAbortedOwner
		}
	}

	// Contiguity + CAS.
	if s.StreamState.HeadOffset != entries[0].ifl.FirstOffset {
		return GroupCommitOutput{}, ErrOrderingGap
	}
	if s.StreamState.BaseOffset > entries[0].ifl.FirstOffset {
		return GroupCommitOutput{}, ErrTruncateConflict
	}
	for i := 0; i < n-1; i++ {
		if entries[i].ifl.EndOffset != entries[i+1].ifl.FirstOffset {
			return GroupCommitOutput{}, ErrOrderingGap
		}
	}

	// Same-(publisher,seq) within group already rejected at GroupPrepare,
	// but recheck here defensively: under group reuse this could fire if
	// a caller hand-rolls.
	type pubKey struct {
		p string
		s uint64
	}
	seenPub := make(map[pubKey]struct{}, n)
	for _, e := range entries {
		st := e.ifl.InflightPublishState
		k := pubKey{st.PublisherId, st.Sequence}
		if _, dup := seenPub[k]; dup {
			return GroupCommitOutput{}, ErrGroupCommitDuplicate
		}
		seenPub[k] = struct{}{}
		// Dedup-replay short-circuit at commit time.
		if pubField, ok := s.Publishers[st.PublisherId]; ok {
			ps, ok := pubField.TryGet(ctx)
			if ok && ps != nil && ps.PublisherState != nil && st.Sequence <= ps.LastSeq {
				return GroupCommitOutput{}, ErrPublishGroupAborted
			}
		}
	}

	// All guards pass.  Apply the commit.
	out := GroupCommitOutput{Items: make([]CommitPublishOutput, n)}
	maxTxnID := entries[0].txnID
	for i, e := range entries {
		st := e.ifl.InflightPublishState
		itemCount := int32(st.EndOffset - st.FirstOffset)
		s.Publishers[st.PublisherId] = chasm.NewComponentField(ctx, &PublisherState{
			PublisherState: &streampb.PublisherState{
				LastSeq:     st.Sequence,
				LastSeen:    timestampNow(now),
				FirstOffset: st.FirstOffset,
				ItemCount:   int64(itemCount),
			},
		})
		out.Items[i] = CommitPublishOutput{
			FirstOffset: st.FirstOffset,
			ItemCount:   itemCount,
		}
		delete(s.Inflights, e.txnID)
		if e.txnID > maxTxnID {
			maxTxnID = e.txnID
		}
	}
	s.StreamState.HeadOffset = entries[n-1].ifl.EndOffset
	s.StreamState.CommittedTxnId = maxTxnID
	return out, nil
}

// GroupAbortInput is the input to Stream.GroupAbort.
//
// Spec correspondence: GroupAbort(S) action arguments.
type GroupAbortInput struct {
	TxnIDs []int64
}

// GroupAbort atomically aborts a group of inflights, scheduling the
// per-entry segment-row cleanup task for each.  Called by the
// orchestrator (handler.Publish or the group-commit dispatcher) when
// any commit guard would fail; the all-or-none atomicity matches the
// design's "any member's guard failure aborts whole group" rule.
//
// Spec correspondence: GroupAbort(S) action in StreamCommit.tla L407.
func (s *Stream) GroupAbort(
	ctx chasm.MutableContext,
	input GroupAbortInput,
) error {
	for _, txnID := range input.TxnIDs {
		f, ok := s.Inflights[txnID]
		if !ok {
			continue
		}
		ifl, ok := f.TryGet(ctx)
		if !ok || ifl == nil {
			delete(s.Inflights, txnID)
			continue
		}
		s.abortInflight(ctx, txnID, ifl)
	}
	return nil
}

var (
	ErrGroupPrepareUnderMin  = serviceerrorInvalidArgument("group-prepare requires >= 2 items")
	ErrGroupPrepareDuplicate = serviceerrorInvalidArgument("group-prepare contains duplicate (publisher_id, sequence)")
	ErrGroupCommitUnderMin   = serviceerrorInvalidArgument("group-commit requires >= 2 txn_ids")
	ErrGroupCommitDuplicate  = serviceerrorInvalidArgument("group-commit members contain duplicate (publisher_id, sequence)")
)
