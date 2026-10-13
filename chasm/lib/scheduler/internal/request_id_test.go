package internal

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGenerateWorkflowID(t *testing.T) {
	baseWorkflowID := "my-workflow"
	nominalTime := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)

	actual := GenerateWorkflowID(baseWorkflowID, nominalTime)
	require.Equal(t, "my-workflow-2024-06-15T10:30:45Z", actual)
}

func TestGenerateRequestID(t *testing.T) {
	nominalTime := time.UnixMilli(1_700_000_000_000)
	actualTime := time.UnixMilli(1_700_000_000_001)
	scheduleID := strings.Repeat("a", 1000)
	requestIDUUID := uuid.NewSHA1(uuid.Nil, fmt.Appendf(nil,
		"%q-%q-%d-%d-%d",
		"nsid",
		scheduleID,
		10,
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	))

	for _, tc := range []struct {
		name       string
		backfillID string
		prefix     string
	}{
		{name: "automatic", prefix: "auto"},
		{name: "backfill", backfillID: "backfill-id", prefix: "backfill-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestID := GenerateRequestID("nsid", scheduleID, 10, tc.backfillID, nominalTime, actualTime)
			require.Equal(t, fmt.Sprintf("sched-%s-%s", tc.prefix, requestIDUUID), requestID)
			require.NotContains(t, requestID, scheduleID)
		})
	}
}
