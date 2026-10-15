package persistence_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

// TestValidateBatchEmpty covers the empty-batch case. readHistoryBranch and
// readHistoryBranchReverse already guard it with errEmptyEvents before their own first/last
// event assignments; ValidateBatch must do the same rather than panicking on batch[0].
func TestValidateBatchEmpty(t *testing.T) {
	logger := log.NewNoopLogger()
	branchToken := []byte("branch-token")

	t.Run("nil batch returns data loss, does not panic", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() {
			err = p.ValidateBatch(nil, branchToken, 0, logger)
		})
		require.Error(t, err)
		require.IsType(t, &serviceerror.DataLoss{}, err)
		require.Contains(t, err.Error(), "empty events")
	})

	t.Run("a valid contiguous batch is still accepted", func(t *testing.T) {
		// Guards against the empty-batch check over-triggering.
		batch := []*historyspb.StrippedHistoryEvent{
			{EventId: 1, Version: 100},
			{EventId: 2, Version: 100},
		}
		require.NoError(t, p.ValidateBatch(batch, branchToken, 0, logger))
	})

	t.Run("zero-length blob deserializes silently, then must not panic", func(t *testing.T) {
		// A zero-length blob is a *valid* proto encoding of "no events", and
		// DeserializeStrippedEvents returns (nil, nil) for it — no error. So an empty batch
		// reaches ValidateBatch with nothing upstream having flagged a problem.
		events, err := serialization.NewSerializer().DeserializeStrippedEvents(&commonpb.DataBlob{
			Data:         []byte{},
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		})
		require.NoError(t, err, "zero-length blob must deserialize without error")
		require.Empty(t, events, "and must yield zero events")

		var verr error
		require.NotPanics(t, func() {
			verr = p.ValidateBatch(events, branchToken, 0, logger)
		})
		require.Error(t, verr)
		require.IsType(t, &serviceerror.DataLoss{}, verr)
	})
}
