package stream_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// Spec correspondence: GroupPrepare action assigns CONSECUTIVE offset
// ranges in iteration order in a single chasm transition.
func TestGroupPrepareAssignsConsecutiveOffsets(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	out, err := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 3, PayloadHash: []byte("h1")},
			{PublisherID: "pub-2", Sequence: 1, ItemCount: 2, PayloadHash: []byte("h2")},
			{PublisherID: "pub-3", Sequence: 1, ItemCount: 4, PayloadHash: []byte("h3")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 3)

	// Consecutive ranges starting at head=0.
	require.Equal(t, int64(0), out.Items[0].FirstOffset)
	require.Equal(t, int32(3), out.Items[0].ItemCount)
	require.Equal(t, int64(3), out.Items[1].FirstOffset)
	require.Equal(t, int32(2), out.Items[1].ItemCount)
	require.Equal(t, int64(5), out.Items[2].FirstOffset)
	require.Equal(t, int32(4), out.Items[2].ItemCount)

	// Monotone txn_ids.
	require.Less(t, out.Items[0].TxnID, out.Items[1].TxnID)
	require.Less(t, out.Items[1].TxnID, out.Items[2].TxnID)

	// Head must NOT have advanced.
	require.Equal(t, int64(0), s.StreamState.HeadOffset)
	require.Len(t, s.Inflights, 3)
}

// Spec correspondence: GroupPrepare rejects same-(publisher,seq)
// duplicates within a single group.
func TestGroupPrepareRejectsIntraGroupDuplicates(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	_, err := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 1},
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 1},
		},
	})
	require.ErrorIs(t, err, stream.ErrGroupPrepareDuplicate)
}

// Spec correspondence: GroupCommit advances head to the trailing
// member's end_offset and sets committed_txn_id to the largest member
// txn_id.
func TestGroupCommitAdvancesHeadAndSetsCommittedTxnID(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 2, PayloadHash: []byte("h1")},
			{PublisherID: "pub-2", Sequence: 1, ItemCount: 3, PayloadHash: []byte("h2")},
		},
	})

	out, err := s.GroupCommit(ctx, stream.GroupCommitInput{
		TxnIDs: []int64{prep.Items[0].TxnID, prep.Items[1].TxnID},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 2)

	require.Equal(t, int64(5), s.StreamState.HeadOffset)
	// CommittedTxnId = max member txn_id.
	maxTxn := prep.Items[1].TxnID
	if prep.Items[0].TxnID > maxTxn {
		maxTxn = prep.Items[0].TxnID
	}
	require.Equal(t, maxTxn, s.StreamState.CommittedTxnId)
	require.Empty(t, s.Inflights)
	require.Len(t, s.Publishers, 2)
}

// Spec correspondence: GroupCommit close-guard.  Close between prepare
// and commit aborts the whole group with PublishAbortedByClose.
func TestGroupCommitAfterCloseAborts(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 1},
			{PublisherID: "pub-2", Sequence: 1, ItemCount: 1},
		},
	})
	_, closeErr := s.Close(ctx, stream.CloseInput{
		ClosedBy: "tester", CloseReason: streampb.STREAM_CLOSE_REASON_EXPLICIT,
	})
	require.NoError(t, closeErr)

	_, err := s.GroupCommit(ctx, stream.GroupCommitInput{
		TxnIDs: []int64{prep.Items[0].TxnID, prep.Items[1].TxnID},
	})
	require.ErrorIs(t, err, stream.ErrPublishAbortedClose)
}

// Spec correspondence: GroupCommit ownership-epoch fence.  Any member
// with stale prep_epoch causes the whole group to fail.
func TestGroupCommitAbortsOnOwnerEpochMismatch(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 1},
			{PublisherID: "pub-2", Sequence: 1, ItemCount: 1},
		},
	})

	// Simulate ownership transfer.
	s.StreamState.OwnerEpoch = 1

	_, err := s.GroupCommit(ctx, stream.GroupCommitInput{
		TxnIDs: []int64{prep.Items[0].TxnID, prep.Items[1].TxnID},
	})
	require.ErrorIs(t, err, stream.ErrPublishAbortedOwner)
	// All-or-none: head not advanced.
	require.Equal(t, int64(0), s.StreamState.HeadOffset)
}

// Spec correspondence: GroupAbort atomically aborts every member and
// schedules per-entry segment-row cleanup tasks.
func TestGroupAbortRemovesAllMembers(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 1},
			{PublisherID: "pub-2", Sequence: 1, ItemCount: 1},
			{PublisherID: "pub-3", Sequence: 1, ItemCount: 1},
		},
	})
	require.Len(t, s.Inflights, 3)

	require.NoError(t, s.GroupAbort(ctx, stream.GroupAbortInput{
		TxnIDs: []int64{prep.Items[0].TxnID, prep.Items[1].TxnID, prep.Items[2].TxnID},
	}))
	require.Empty(t, s.Inflights)
	require.Equal(t, int64(0), s.StreamState.HeadOffset)
}

// Spec correspondence: GroupPrepare dedup short-circuit.  A duplicate
// in a group returns the prior outcome for that item and the rest of
// the group proceeds.
func TestGroupPrepareDedupShortCircuit(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	// Pre-commit pub-1 seq 1 so it's in pub_state.
	first, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 2, PayloadHash: []byte("h"),
	})
	_, _ = s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: first.TxnID, ProofOfWriteVerified: true,
	})

	// Now run a group where one item dedup-replays.
	out, err := s.GroupPrepare(ctx, stream.GroupPrepareInput{
		Items: []stream.GroupPrepareItem{
			{PublisherID: "pub-1", Sequence: 1, ItemCount: 2},        // dedup-replay
			{PublisherID: "pub-2", Sequence: 1, ItemCount: 3},        // fresh
		},
	})
	require.NoError(t, err)
	require.True(t, out.Items[0].IsDedupReplay)
	require.Equal(t, int64(0), out.Items[0].FirstOffset)
	require.False(t, out.Items[1].IsDedupReplay)
	// pub-2 starts where pub-1's earlier commit landed it.
	require.Equal(t, int64(2), out.Items[1].FirstOffset)
}
