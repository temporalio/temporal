package schedules

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKnownIssue_DerivedSchedulerIDsRespectFrontendLimit(t *testing.T) {
	const maxIDLength = 1000
	when := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)

	t.Run("workflow ID", func(t *testing.T) {
		workflowID := GenerateWorkflowID(strings.Repeat("w", maxIDLength), when)
		require.LessOrEqual(t, len(workflowID), maxIDLength,
			"a frontend-valid base workflow ID must not produce an invalid derived workflow ID")
	})

	t.Run("request ID", func(t *testing.T) {
		requestID := GenerateRequestID(
			"namespace-id",
			strings.Repeat("s", maxIDLength),
			1,
			"backfill-id",
			when,
			when,
		)
		require.LessOrEqual(t, len(requestID), maxIDLength,
			"a frontend-valid schedule ID must not produce an invalid derived request ID")
	})
}
