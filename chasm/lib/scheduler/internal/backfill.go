package internal

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// InclusiveBackfillCursor returns the exclusive cursor that includes startTime.
func InclusiveBackfillCursor(startTime time.Time) time.Time {
	return startTime.Add(-time.Millisecond)
}

// HasRecordedBackfillProgress reports whether a backfill has a persisted range
// cursor. New backfillers use nil for no progress; a present zero timestamp is
// also treated as unset for compatibility with older backfiller state.
func HasRecordedBackfillProgress(lastProcessed *timestamppb.Timestamp) bool {
	return lastProcessed != nil && (lastProcessed.GetSeconds() != 0 || lastProcessed.GetNanos() != 0)
}
