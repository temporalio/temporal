package scheduler2_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/components/scheduler2"
)

func TestGenerateRequestID(t *testing.T) {
	scheduler := scheduler2.Scheduler{
		SchedulerInternal: &schedule.SchedulerInternal{
			Namespace:     "ns",
			NamespaceId:   "nsid",
			ScheduleId:    "mysched",
			ConflictToken: 10,
		},
	}
	nominalTime := time.Now()
	actualTime := time.Now()

	// No backfill ID given.
	actual := scheduler2.GenerateRequestID(
		scheduler,
		"",
		nominalTime,
		actualTime,
	)
	expected := fmt.Sprintf(
		"sched-auto-nsid-mysched-10-%d-%d",
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)

	// Backfill ID given.
	actual = scheduler2.GenerateRequestID(
		scheduler,
		"backfillid",
		nominalTime,
		actualTime,
	)
	expected = fmt.Sprintf(
		"sched-backfillid-nsid-mysched-10-%d-%d",
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)
}
