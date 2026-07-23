package internal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerateRequestID(t *testing.T) {
	nominalTime := time.Now()
	actualTime := time.Now()
	scheduleIDHashSum := sha256.Sum256([]byte("mysched"))
	scheduleIDHash := hex.EncodeToString(scheduleIDHashSum[:16])

	actual := GenerateRequestID("nsid", "mysched", 10, "", nominalTime, actualTime)
	expected := fmt.Sprintf(
		"sched-auto-nsid-%s-10-%d-%d",
		scheduleIDHash,
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)

	actual = GenerateRequestID("nsid", "mysched", 10, "backfillid", nominalTime, actualTime)
	expected = fmt.Sprintf(
		"sched-backfillid-nsid-%s-10-%d-%d",
		scheduleIDHash,
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)
}

func TestGenerateRequestIDHashesLongScheduleID(t *testing.T) {
	nominalTime := time.UnixMilli(1_700_000_000_000)
	actualTime := time.UnixMilli(1_700_000_000_001)
	scheduleID := strings.Repeat("a", 1000)
	scheduleIDHashSum := sha256.Sum256([]byte(scheduleID))
	scheduleIDHash := hex.EncodeToString(scheduleIDHashSum[:16])

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
			require.Contains(t, requestID, fmt.Sprintf("sched-%s-nsid-%s-1-", tc.prefix, scheduleIDHash))
			require.NotContains(t, requestID, scheduleID)

			otherRequestID := GenerateRequestID("nsid", scheduleID+"b", 1, tc.backfillID, nominalTime, actualTime)
			require.NotEqual(t, requestID, otherRequestID)
		})
	}
}
