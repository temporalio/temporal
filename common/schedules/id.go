package schedules

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GenerateWorkflowID generates a deterministic workflow ID for a buffered
// action by combining the base workflow ID with the truncated nominal time.
func GenerateWorkflowID(baseWorkflowID string, nominalTime time.Time) string {
	nominalTimeSec := nominalTime.Truncate(time.Second)
	return fmt.Sprintf("%s-%s", baseWorkflowID, nominalTimeSec.UTC().Format(time.RFC3339))
}

// GenerateBackfillerID generates a unique ID for a Backfiller component.
// This ID is used to identify and deduplicate backfill requests.
func GenerateBackfillerID() string {
	return uuid.NewString()
}
