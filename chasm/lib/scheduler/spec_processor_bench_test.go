package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Production spec that emulates fixed-delay: fires every second (with SKIP), plus a
// one-shot year-2100 calendar to keep the schedule from going idle.
const everySecondKeepaliveSpecJSON = `{
  "structuredCalendar": [
    {
      "second": [{"start": 59, "end": 59}], "minute": [{"start": 59, "end": 59}],
      "hour": [{"start": 23, "end": 23}], "dayOfMonth": [{"start": 31, "end": 31}],
      "month": [{"start": 12, "end": 12}], "year": [{"start": 2100, "end": 2100}],
      "dayOfWeek": [{"end": 6, "step": 1}]
    },
    {
      "second": [{"end": 59}], "minute": [{"end": 59}], "hour": [{"end": 23}],
      "dayOfMonth": [{"start": 1, "end": 31}], "month": [{"start": 1, "end": 12}],
      "dayOfWeek": [{"end": 6}]
    }
  ],
  "startTime": "1970-01-01T00:00:00Z",
  "endTime": "2124-11-03T06:25:23.869Z",
  "timezoneName": "Etc/GMT"
}`

// Cost of catching up after a stall: one op processes a 1h gap (3600 fires).
func BenchmarkProcessTimeRange_EverySecond(b *testing.B) {
	var spec schedulepb.ScheduleSpec
	require.NoError(b, protojson.Unmarshal([]byte(everySecondKeepaliveSpecJSON), &spec))
	sched := defaultSchedule()
	sched.Spec = &spec
	sched.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	sched.Policies.CatchupWindow = durationpb.New(365 * 24 * time.Hour)

	env := newTestEnv(b, withSchedule(sched))
	ctx := chasm.NewMutableContext(context.Background(), env.Node)
	s, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, sched, nil)
	require.NoError(b, err)
	processor := newTestSpecProcessor(env.Ctrl)

	const gap = time.Hour
	end := time.Date(2026, 10, 16, 4, 47, 11, 0, time.UTC)
	b.ReportAllocs()
	for b.Loop() {
		limit := 1000
		res, err := processor.ProcessTimeRange(s, end.Add(-gap), end,
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, s.WorkflowID(), "", false, &limit)
		if err != nil || res.DroppedCount != int64(gap/time.Second)-1000 {
			b.Fatalf("unexpected result: %v, dropped=%d", err, res.DroppedCount)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(gap/time.Second)), "ns/fire")
}
