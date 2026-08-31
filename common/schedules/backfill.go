package schedules

import "time"

// InclusiveBackfillCursor returns the exclusive cursor that includes startTime.
func InclusiveBackfillCursor(startTime time.Time) time.Time {
	return startTime.Add(-time.Millisecond)
}
