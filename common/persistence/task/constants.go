package task

import "time"

// ScheduledTaskMinPrecision is the minimum precision for scheduled task times.
// This matches the precision used by the persistence layer when storing and retrieving tasks.
const ScheduledTaskMinPrecision = time.Millisecond
