package internal

import "google.golang.org/protobuf/types/known/timestamppb"

// HasRecordedBackfillProgress reports whether a backfill has a persisted range cursor.
func HasRecordedBackfillProgress(lastProcessed *timestamppb.Timestamp) bool {
	return lastProcessed != nil
}
