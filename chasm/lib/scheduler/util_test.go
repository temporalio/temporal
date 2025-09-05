package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulespb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestGenerateRequestID(t *testing.T) {
	scheduler := &Scheduler{
		SchedulerState: &schedulespb.SchedulerState{
			Namespace:     "ns",
			NamespaceId:   "nsid",
			ScheduleId:    "mysched",
			ConflictToken: 10,
		},
	}
	nominalTime := time.Now()
	actualTime := time.Now()

	// No backfill ID given.
	actual := generateRequestID(
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
	actual = generateRequestID(
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

func TestMergeCustomSearchAttributes(t *testing.T) {
	currentAttrs := map[string]*commonpb.Payload{
		"keep":   {Data: []byte("keep-data")},
		"update": {Data: []byte("old-data")},
		"delete": {Data: []byte("delete-data")},
	}
	customAttrs := map[string]*commonpb.Payload{
		"keep":   {Data: []byte("keep-data")},
		"update": {Data: []byte("new-data")},
		"new":    {Data: []byte("new-data")},
	}
	expected := map[string]any{
		"update": &commonpb.Payload{Data: []byte("new-data")},
		"new":    &commonpb.Payload{Data: []byte("new-data")},
		"delete": nil,
	}

	upsertAttrs := make(map[string]any)
	mergeCustomSearchAttributes(currentAttrs, customAttrs, upsertAttrs)
	require.Equal(t, expected, upsertAttrs)
}
