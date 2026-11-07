package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func benchSpecs(b *testing.B) map[string]*schedulepb.ScheduleSpec {
	var prod schedulepb.ScheduleSpec
	require.NoError(b, protojson.Unmarshal([]byte(everySecondSpecJSON), &prod))
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
