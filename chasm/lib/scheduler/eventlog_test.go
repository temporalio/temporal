package scheduler_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testEventLogMaxEntries    = 30
	testEventLogMaxMessageLen = 1000
)

func TestEventLog_Accumulates(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	eventLog := sched.EventLog.Get(ctx)
	eventLog.Events = nil

	messages := []string{"first", "second", "third"}
	for _, m := range messages {
		eventLog.LogEvent(ctx, m, testEventLogMaxEntries, testEventLogMaxMessageLen)
	}

	require.Len(t, eventLog.Events, len(messages))
	for i, m := range messages {
		require.Equal(t, m, eventLog.Events[i].Message)
		require.NotNil(t, eventLog.Events[i].Time)
	}
}

func TestEventLog_TruncatesLongMessages(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	eventLog := sched.EventLog.Get(ctx)
	eventLog.Events = nil

	long := strings.Repeat("x", testEventLogMaxMessageLen+50)
	eventLog.LogEvent(ctx, long, testEventLogMaxEntries, testEventLogMaxMessageLen)

	require.Len(t, eventLog.Events, 1)
	require.Len(t, eventLog.Events[0].Message, testEventLogMaxMessageLen)
	require.Equal(t, long[:testEventLogMaxMessageLen], eventLog.Events[0].Message)
}

func TestEventLog_DropsEarliestWhenFull(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	eventLog := sched.EventLog.Get(ctx)
	eventLog.Events = nil

	const overflow = 5
	total := testEventLogMaxEntries + overflow
	for i := range total {
		eventLog.LogEvent(ctx, fmt.Sprintf("event-%d", i), testEventLogMaxEntries, testEventLogMaxMessageLen)
	}

	require.Len(t, eventLog.Events, testEventLogMaxEntries)
	// The earliest `overflow` entries should have been dropped; the retained
	// window starts at event-`overflow` and ends at the most recent event.
	require.Equal(t, fmt.Sprintf("event-%d", overflow), eventLog.Events[0].Message)
	require.Equal(t, fmt.Sprintf("event-%d", total-1), eventLog.Events[len(eventLog.Events)-1].Message)
}

func TestEventLog_EachComponentHasOwn(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	// Create a backfiller so we can exercise its EventLog too.
	backfiller := sched.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(time.Now().Add(-time.Hour)),
		EndTime:       timestamppb.New(time.Now()),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	})

	// Each host component gets its own EventLog instance.
	require.NotNil(t, sched.EventLog.Get(ctx))
	require.NotNil(t, sched.Generator.Get(ctx).EventLog.Get(ctx))
	require.NotNil(t, sched.Invoker.Get(ctx).EventLog.Get(ctx))
	require.NotNil(t, backfiller.EventLog.Get(ctx))

	// EventLogs on different hosts are independent.
	sched.EventLog.Get(ctx).Events = nil
	sched.Generator.Get(ctx).EventLog.Get(ctx).Events = nil
	backfiller.EventLog.Get(ctx).Events = nil

	sched.EventLog.Get(ctx).LogEvent(ctx, "scheduler-event", testEventLogMaxEntries, testEventLogMaxMessageLen)
	require.Len(t, sched.EventLog.Get(ctx).Events, 1)
	require.Empty(t, sched.Generator.Get(ctx).EventLog.Get(ctx).Events)
	require.Empty(t, backfiller.EventLog.Get(ctx).Events)
}
