package streamsegmentstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/streamsegmentstore"
)

func key() persistence.StreamSegmentKey {
	return persistence.StreamSegmentKey{
		ShardID:     1,
		NamespaceID: "ns-1",
		StreamID:    "stream-1",
	}
}

func row(segmentID, txnID, first, last int64) persistence.StreamSegmentRow {
	return persistence.StreamSegmentRow{
		SegmentID:   segmentID,
		TxnID:       txnID,
		FirstOffset: first,
		LastOffset:  last,
		ItemCount:   int32(last - first + 1),
		PayloadHash: []byte("h"),
		Data:        &commonpb.DataBlob{},
	}
}

// TentativeWriteIsIdempotent verifies the spec's "step-2 retry with same
// txn_id is idempotent" claim — re-writing the same (segment_id, txn_id)
// is allowed and the row count does not grow.
//
// Spec correspondence: WriteSegmentsTentative(t) action.
func TestTentativeWriteIsIdempotent(t *testing.T) {
	m := streamsegmentstore.NewMemory()
	ctx := context.Background()

	_, err := m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key:  key(),
		Rows: []persistence.StreamSegmentRow{row(1, 100, 0, 9)},
	})
	require.NoError(t, err)
	require.Equal(t, 1, m.CountRows())

	// Replay same txn — should be a no-op for the row count.
	_, err = m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key:  key(),
		Rows: []persistence.StreamSegmentRow{row(1, 100, 0, 9)},
	})
	require.NoError(t, err)
	require.Equal(t, 1, m.CountRows())
}

// MultipleTxnVersionsCoexist verifies the spec's "multiple tentative
// versions of the same offset range coexist under different txn_ids"
// claim — what makes the proof-of-write check meaningful.
//
// Spec correspondence: segments set is a set of (txn_id, offset, ...)
// records; the same offset can appear under multiple txn_ids while
// inflights race.
func TestMultipleTxnVersionsCoexist(t *testing.T) {
	m := streamsegmentstore.NewMemory()
	ctx := context.Background()

	// Two prepares for the same offset range under different txn_ids.
	for _, txnID := range []int64{100, 101} {
		_, err := m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
			Key:  key(),
			Rows: []persistence.StreamSegmentRow{row(1, txnID, 0, 9)},
		})
		require.NoError(t, err)
	}
	require.Equal(t, 2, m.CountRows())
}

// ReadRangeFiltersByCommittedTxnID verifies the visibility rule from
// the design: subscribers see rows with txn_id <= committed_txn_id,
// and only the max txn_id per segment_id is returned.
//
// Spec correspondence: VisibilityMatchesCommitted invariant.
func TestReadRangeFiltersByCommittedTxnID(t *testing.T) {
	m := streamsegmentstore.NewMemory()
	ctx := context.Background()

	// Two competing publishes wrote the same segment range under txn_ids
	// 100 and 101.  After commit, committed_txn_id is set to 101 (the
	// winning publish) — but for this test we set it lower to verify
	// the filter.
	_, _ = m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key:  key(),
		Rows: []persistence.StreamSegmentRow{row(1, 100, 0, 9)},
	})
	_, _ = m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key:  key(),
		Rows: []persistence.StreamSegmentRow{row(1, 101, 0, 9)},
	})

	// committed_txn_id = 100 → only txn 100 visible.
	resp, err := m.ReadRange(ctx, &persistence.ReadStreamRangeRequest{
		Key: key(), StartOffset: 0, EndOffset: 100, CommittedTxnID: 100,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	require.Equal(t, int64(100), resp.Rows[0].TxnID)

	// committed_txn_id = 101 → returns the winning (max) txn for the
	// segment_id.
	resp, err = m.ReadRange(ctx, &persistence.ReadStreamRangeRequest{
		Key: key(), StartOffset: 0, EndOffset: 100, CommittedTxnID: 101,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	require.Equal(t, int64(101), resp.Rows[0].TxnID)
}

// DeleteByTxnCleansAllSegmentsForOneTxn verifies the abort-cleanup task's
// contract: deleting by (stream, txn_id) removes every segment that txn
// touched.
//
// Spec correspondence: cleanup invoked from AbortInflight / SweepExpired /
// GroupAbort / CloseCleanup.
func TestDeleteByTxnCleansAllSegmentsForOneTxn(t *testing.T) {
	m := streamsegmentstore.NewMemory()
	ctx := context.Background()

	// One inflight that spans two segments under one txn_id.
	_, _ = m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key: key(),
		Rows: []persistence.StreamSegmentRow{
			row(1, 100, 0, 9),
			row(2, 100, 10, 19),
		},
	})
	// A peer with a different txn_id.
	_, _ = m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key:  key(),
		Rows: []persistence.StreamSegmentRow{row(1, 101, 0, 9)},
	})

	require.Equal(t, 3, m.CountRows())

	require.NoError(t, m.DeleteByTxn(ctx, &persistence.DeleteSegmentsByTxnRequest{
		Key: key(), TxnID: 100,
	}))

	// Only the txn-101 row should remain.
	require.Equal(t, 1, m.CountRows())
}

// DeletePrefixHonoursLastOffset verifies that truncate uses last_offset,
// not first_offset — a segment is dropped iff its last item is below the
// watermark.
//
// Spec correspondence: Truncate(upTo) action.
func TestDeletePrefixHonoursLastOffset(t *testing.T) {
	m := streamsegmentstore.NewMemory()
	ctx := context.Background()

	_, _ = m.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key: key(),
		Rows: []persistence.StreamSegmentRow{
			row(1, 100, 0, 9),
			row(2, 100, 10, 19),
			row(3, 100, 20, 29),
		},
	})
	require.Equal(t, 3, m.CountRows())

	// Truncate up_to_offset = 15 → drops segments whose last_offset < 15.
	// Segment 1 ends at 9 → dropped.  Segments 2/3 survive.
	require.NoError(t, m.DeletePrefix(ctx, &persistence.DeleteSegmentsPrefixRequest{
		Key: key(), BelowOffset: 15,
	}))
	require.Equal(t, 2, m.CountRows())
}
