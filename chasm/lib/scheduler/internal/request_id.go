package internal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateRequestID generates a deterministic request ID for a buffered action's
// time. The request ID is deterministic because the jittered actual time (as
// well as the spec's nominal time) is, in turn, also deterministic.
//
// backfillID should be left blank for actions that are being started
// automatically, based on the schedule spec. It must be set for backfills,
// as backfills may generate buffered actions that overlap with both
// automatically-buffered actions, as well as other requested backfills.
func GenerateRequestID(
	namespaceID string,
	scheduleID string,
	conflictToken int64,
	backfillID string,
	nominal time.Time,
	actual time.Time,
) string {
	if backfillID == "" {
		backfillID = "auto"
	}
	scheduleIDHash := sha256.Sum256([]byte(scheduleID))

	return fmt.Sprintf(
		"sched-%s-%s-%s-%d-%d-%d",
		backfillID,
		namespaceID,
		hex.EncodeToString(scheduleIDHash[:16]),
		conflictToken,
		nominal.UnixMilli(),
		actual.UnixMilli(),
	)
}
