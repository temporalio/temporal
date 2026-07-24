package internal

import (
	"fmt"
	"math"
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
	nominalTime := time.Now()
	actualTime := time.Now()
	scheduleID := "mysched"
	scheduleIDUUID := uuid.NewSHA1(uuid.Nil, []byte(scheduleID))

	actual := GenerateRequestID("nsid", scheduleID, 10, "", nominalTime, actualTime)
	expected := fmt.Sprintf(
		"sched-auto-nsid-%s-10-%d-%d",
		scheduleIDUUID,
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)

	actual = GenerateRequestID("nsid", scheduleID, 10, "backfillid", nominalTime, actualTime)
	expected = fmt.Sprintf(
		"sched-backfillid-nsid-%s-10-%d-%d",
		scheduleIDUUID,
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)
}

func TestGenerateRequestIDUsesUUIDv5ForScheduleID(t *testing.T) {
	nominalTime := time.UnixMilli(1_700_000_000_000)
	actualTime := time.UnixMilli(1_700_000_000_001)
	scheduleID := strings.Repeat("a", 1000)
	scheduleIDUUID := uuid.NewSHA1(uuid.Nil, []byte(scheduleID))
	require.Equal(t, uuid.Version(5), scheduleIDUUID.Version())

	for _, tc := range []struct {
		name       string
		backfillID string
		prefix     string
	}{
		{name: "automatic", prefix: "auto"},
		{name: "backfill", backfillID: "backfill-id", prefix: "backfill-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestID := GenerateRequestID("nsid", scheduleID, 1, tc.backfillID, nominalTime, actualTime)
			require.Contains(t, requestID, fmt.Sprintf("sched-%s-nsid-%s-1-", tc.prefix, scheduleIDUUID))
			require.NotContains(t, requestID, scheduleID)
			require.LessOrEqual(t, len(requestID), lowestKnownSchemaRequestIDColumnLimit)

			otherRequestID := GenerateRequestID("nsid", scheduleID+"b", 1, tc.backfillID, nominalTime, actualTime)
			require.NotEqual(t, requestID, otherRequestID)
		})
	}
}

func TestGenerateRequestIDFitsSQLColumn(t *testing.T) {
	requestID := GenerateRequestID(
		strings.Repeat("a", 36),
		"schedule-id",
		math.MinInt64,
		strings.Repeat("b", 36),
		time.UnixMilli(math.MinInt64),
		time.UnixMilli(math.MaxInt64),
	)

	require.LessOrEqual(t, len(requestID), lowestKnownSchemaRequestIDColumnLimit)
}
