package schedules

import "time"

// InclusiveBackfillStartOffset is subtracted from a backfill request's start time
// before it is used as the cursor of a next-time search.
//
// Both scheduler implementations search for matching times strictly *after* a
// cursor, but a backfill's start time is inclusive: an action landing exactly on
// the requested start must be generated. Shifting the cursor back by this offset
// encodes that inclusivity.
//
// V1 applies the offset once, at patch intake, and stores the shifted value in
// InternalState.OngoingBackfills. V2 applies it when a Backfiller runs for the
// first time (Attempt == 0), leaving the stored request untouched. Migration
// between the two must therefore translate between those representations.
const InclusiveBackfillStartOffset = time.Millisecond
