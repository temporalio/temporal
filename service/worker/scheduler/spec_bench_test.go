package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A production V1 spec that emulates fixed delay: fires every second, plus a one-shot
// year-2100 calendar that keeps the schedule from being deleted as idle.
const benchEverySecondKeepaliveSpecJSON = `{
  "structuredCalendar": [
    {
      "second": [{"start": 59, "end": 59}],
      "minute": [{"start": 59, "end": 59}],
      "hour": [{"start": 23, "end": 23}],
      "dayOfMonth": [{"start": 31, "end": 31}],
      "month": [{"start": 12, "end": 12}],
      "year": [{"start": 2100, "end": 2100}],
      "dayOfWeek": [{"end": 6, "step": 1}],
      "comment": "For ensuring that the schedule will never be automatically deleted"
    },
    {
      "second": [{"end": 59}],
      "minute": [{"end": 59}],
      "hour": [{"end": 23}],
      "dayOfMonth": [{"start": 1, "end": 31}],
      "month": [{"start": 1, "end": 12}],
      "dayOfWeek": [{"end": 6}]
    }
  ],
  "startTime": "1970-01-01T00:00:00Z",
  "endTime": "2124-11-03T06:25:23.869Z",
  "timezoneName": "Etc/GMT"
}`

func benchSpecs(b *testing.B) map[string]*schedulepb.ScheduleSpec {
	var prod schedulepb.ScheduleSpec
	require.NoError(b, protojson.Unmarshal([]byte(benchEverySecondKeepaliveSpecJSON), &prod))
	everySecond := &schedulepb.ScheduleSpec{
		StructuredCalendar: prod.StructuredCalendar[1:],
		TimezoneName:       "Etc/GMT",
	}
	jittered := &schedulepb.ScheduleSpec{
		StructuredCalendar: prod.StructuredCalendar[1:],
		Jitter:             durationpb.New(500 * time.Millisecond),
	}
	return map[string]*schedulepb.ScheduleSpec{
		"prod_every_second_with_2100_keepalive": &prod,
		"every_second_calendar":                 everySecond,
		"every_second_calendar_jitter":          jittered,
		"interval_1s":                           {Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}},
		"hourly_calendar_new_york": {
			Calendar:     []*schedulepb.CalendarSpec{{Second: "0", Minute: "0", Hour: "*"}},
			TimezoneName: "America/New_York",
		},
	}
}

// Each op advances to the next matching time, mirroring the processTimeRange loop.
func BenchmarkGetNextTime(b *testing.B) {
	for name, spec := range benchSpecs(b) {
		b.Run(name, func(b *testing.B) {
			cs, err := newSpecBuilderForTest(0, 0).NewCompiledSpec(spec)
			require.NoError(b, err)
			start := time.Date(2026, 10, 16, 0, 23, 19, 0, time.UTC)
			t := start
			b.ReportAllocs()
			for b.Loop() {
				next, err := cs.GetNextTime("seed", t)
				if err != nil || next.Next.IsZero() {
					b.Fatalf("unexpected result %v %v", next, err)
				}
				t = next.Next
				// Hourly specs would otherwise run into year 2100.
				if t.Year() > 2090 {
					t = start
				}
			}
		})
	}
}
