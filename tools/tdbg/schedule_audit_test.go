package tdbg

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAuditInputs_Validate(t *testing.T) {
	base := func() *auditInputs {
		return &auditInputs{
			Namespaces:  []string{"ns"},
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			OutputDir:   "/tmp/out",
		}
	}

	t.Run("valid passes", func(t *testing.T) {
		require.NoError(t, base().validate())
	})

	t.Run("missing output-dir accepted (stdout mode)", func(t *testing.T) {
		in := base()
		in.OutputDir = ""
		require.NoError(t, in.validate())
	})

	t.Run("schedule-id requires single namespace", func(t *testing.T) {
		in := base()
		in.ScheduleID = "sched1"
		in.Namespaces = []string{"ns1", "ns2"}
		require.ErrorContains(t, in.validate(), "--schedule-id requires exactly one namespace")
	})

	t.Run("schedule-id with one namespace OK", func(t *testing.T) {
		in := base()
		in.ScheduleID = "sched1"
		require.NoError(t, in.validate())
	})

	t.Run("end must be after start", func(t *testing.T) {
		in := base()
		in.WindowStart = mustParseTime("2026-05-19T20:00:00Z")
		in.WindowEnd = mustParseTime("2026-05-19T18:00:00Z")
		require.ErrorContains(t, in.validate(), "must be after")
	})

	t.Run("end == start rejected", func(t *testing.T) {
		in := base()
		in.WindowEnd = in.WindowStart
		require.ErrorContains(t, in.validate(), "must be after")
	})

	t.Run("window at 7d cap accepted", func(t *testing.T) {
		in := base()
		in.WindowEnd = in.WindowStart.Add(7 * 24 * time.Hour)
		require.NoError(t, in.validate())
	})

	t.Run("window over 7d cap rejected", func(t *testing.T) {
		in := base()
		in.WindowEnd = in.WindowStart.Add(7*24*time.Hour + time.Second)
		require.ErrorContains(t, in.validate(), "max is 168h")
	})

}

func TestAuditCommandIsRegistered(t *testing.T) {
	app := NewCliApp()
	var found bool
	for _, top := range app.Commands {
		if top.Name != "schedule" {
			continue
		}
		for _, ss := range top.Subcommands {
			if ss.Name == "audit" {
				found = true
				break
			}
		}
	}
	require.True(t, found, "audit subcommand not registered under schedule")
}

func TestExpectedFireTimes(t *testing.T) {
	hourlyStructured := func() *schedulepb.StructuredCalendarSpec {
		return &schedulepb.StructuredCalendarSpec{
			Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
			DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
			Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
		}
	}

	t.Run("hourly", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{hourlyStructured()},
		}
		start := time.Date(2026, 5, 19, 18, 0, 0, 0, time.UTC)
		end := time.Date(2026, 5, 19, 23, 0, 0, 0, time.UTC)

		actual, err := expectedFireTimes(spec, "schedid", start, end)
		require.NoError(t, err)

		expected := []time.Time{
			time.Date(2026, 5, 19, 19, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 20, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 21, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 22, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 23, 0, 0, 0, time.UTC),
		}
		require.Equal(t, expected, actual)
	})

	t.Run("every 15 min", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 59, Step: 15}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		start := time.Date(2026, 5, 19, 18, 0, 0, 0, time.UTC)
		end := time.Date(2026, 5, 19, 19, 0, 0, 0, time.UTC)
		actual, err := expectedFireTimes(spec, "x", start, end)
		require.NoError(t, err)

		expected := []time.Time{
			time.Date(2026, 5, 19, 18, 15, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 18, 30, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 18, 45, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 19, 0, 0, 0, time.UTC),
		}
		require.Equal(t, expected, actual)
	})

	t.Run("cron_string hourly", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			CronString: []string{"0 * * * *"},
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T21:00:00Z"))
		require.NoError(t, err)
		require.Equal(t, []time.Time{
			mustParseTime("2026-05-19T19:00:00Z"),
			mustParseTime("2026-05-19T20:00:00Z"),
			mustParseTime("2026-05-19T21:00:00Z"),
		}, actual)
	})

	t.Run("legacy calendar daily at 09:00", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{{
				Hour:   "9",
				Minute: "0",
				Second: "0",
			}},
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T00:00:00Z"),
			mustParseTime("2026-05-21T12:00:00Z"))
		require.NoError(t, err)
		require.Equal(t, []time.Time{
			mustParseTime("2026-05-19T09:00:00Z"),
			mustParseTime("2026-05-20T09:00:00Z"),
			mustParseTime("2026-05-21T09:00:00Z"),
		}, actual)
	})

	t.Run("exclude_structured_calendar drops hour 2", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{hourlyStructured()},
			ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 2, End: 2, Step: 1}},
				DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
			}},
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T00:30:00Z"),
			mustParseTime("2026-05-19T04:00:00Z"))
		require.NoError(t, err)
		// 02:00 is excluded; 01:00, 03:00, 04:00 fire.
		require.Equal(t, []time.Time{
			mustParseTime("2026-05-19T01:00:00Z"),
			mustParseTime("2026-05-19T03:00:00Z"),
			mustParseTime("2026-05-19T04:00:00Z"),
		}, actual)
	})

	t.Run("timezone_name shifts daily fire to UTC offset", func(t *testing.T) {
		// Daily at 9 AM Pacific. May is DST (UTC-7) so 9 AM PT = 16:00 UTC.
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 9, End: 9, Step: 1}},
				DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
			}},
			TimezoneName: "America/Los_Angeles",
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T00:00:00Z"),
			mustParseTime("2026-05-20T00:00:00Z"))
		require.NoError(t, err)
		require.Equal(t, []time.Time{mustParseTime("2026-05-19T16:00:00Z")}, actual)
	})

	t.Run("DST spring-forward records compiler behavior", func(t *testing.T) {
		// US spring-forward 2026: March 8 at 02:00 AM ET -> 03:00 AM ET. Hour 2:30 doesn't exist locally on March 8. We don't
		// assert a specific outcome -- the compiler may skip, advance, or duplicate. What we want is for the result to be
		// stable across runs.
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 30, End: 30, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 2, End: 2, Step: 1}},
				DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
			}},
			TimezoneName: "America/New_York",
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-03-07T00:00:00Z"),
			mustParseTime("2026-03-11T00:00:00Z"))
		require.NoError(t, err)
		// March 7 and March 9-10 produce normal fires (one each). March 8's 02:30 ET fire is in the DST gap; the compiler
		// should not produce a fire for that day. So we expect 3 fires across 4 calendar days.
		require.Len(t, actual, 3, "DST gap day should not produce a fire")
	})

	t.Run("end_time in past returns no fires", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{hourlyStructured()},
			EndTime:            timestamppb.New(mustParseTime("2026-01-01T00:00:00Z")),
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T22:00:00Z"))
		require.NoError(t, err)
		require.Empty(t, actual)
	})

	t.Run("start_time in future returns no fires", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{hourlyStructured()},
			StartTime:          timestamppb.New(mustParseTime("2027-01-01T00:00:00Z")),
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T22:00:00Z"))
		require.NoError(t, err)
		require.Empty(t, actual)
	})

	t.Run("interval with phase produces 15-min cadence offset by phase", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(15 * time.Minute),
				Phase:    durationpb.New(6*time.Minute + 7*time.Second),
			}},
		}
		actual, err := expectedFireTimes(spec, "sid",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T19:00:00Z"))
		require.NoError(t, err)
		require.NotEmpty(t, actual)
		for i := 1; i < len(actual); i++ {
			require.Equal(t, 15*time.Minute, actual[i].Sub(actual[i-1]),
				"consecutive fires must be exactly 15min apart")
		}
		// Phase = 6m7s means every fire's (unix_seconds - 367) is a multiple of 900. Spot-check the first one.
		first := actual[0]
		require.Equal(t, int64(0), (first.Unix()-367)%900,
			"fire %s does not align with phase=6m7s", first.Format(time.RFC3339))
	})
}

func TestTimeline(t *testing.T) {
	t.Run("ActiveAt: still running", func(t *testing.T) {
		tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
		tl.addExecution(timelineEntry{
			WorkflowID:  "wf1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		require.Len(t, tl.activeAt(mustParseTime("2026-05-19T19:00:00Z")), 1)
	})

	t.Run("ActiveAt: closed before query time", func(t *testing.T) {
		tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
		closeTime := mustParseTime("2026-05-19T18:30:00Z")
		tl.addExecution(timelineEntry{
			WorkflowID:  "wf1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			CloseTime:   &closeTime,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		require.Empty(t, tl.activeAt(mustParseTime("2026-05-19T19:00:00Z")))
	})

	t.Run("ContinueAsNew chain treated as one active execution", func(t *testing.T) {
		tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
		canTime := mustParseTime("2026-05-19T18:30:00Z")
		// First link: closed via CONTINUED_AS_NEW at 18:30
		tl.addExecution(timelineEntry{
			WorkflowID:  "wfChain",
			RunID:       "run1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			CloseTime:   &canTime,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		// Second link: still running, started at 18:30 (sharing workflowId)
		tl.addExecution(timelineEntry{
			WorkflowID:  "wfChain",
			RunID:       "run2",
			StartTime:   mustParseTime("2026-05-19T18:30:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"), // inherited
		})
		// At 19:00, chain is still active.
		require.Len(t, tl.activeAt(mustParseTime("2026-05-19T19:00:00Z")), 1)
	})
}

func TestClassifyFirings(t *testing.T) {
	t.Run("all matched", func(t *testing.T) {
		expectedFires := []time.Time{
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T19:00:00Z"),
		}
		tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
		closed := mustParseTime("2026-05-19T18:05:00Z")
		tl.addExecution(timelineEntry{
			WorkflowID:  "w1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			CloseTime:   &closed,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		closed2 := mustParseTime("2026-05-19T19:05:00Z")
		tl.addExecution(timelineEntry{
			WorkflowID:  "w2",
			StartTime:   mustParseTime("2026-05-19T19:00:00Z"),
			CloseTime:   &closed2,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			NominalTime: mustParseTime("2026-05-19T19:00:00Z"),
		})
		res := &scheduleResult{}
		classifyFirings(res, expectedFires, tl)
		require.Equal(t, 2, res.Matched)
		require.Empty(t, res.Missed)
	})

	t.Run("real_miss", func(t *testing.T) {
		// Two expected fires; the second has no execution and nothing else was active at that time -> real_miss.
		expectedFires := []time.Time{
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T19:00:00Z"),
		}
		tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
		closed := mustParseTime("2026-05-19T18:05:00Z")
		tl.addExecution(timelineEntry{
			WorkflowID:  "w1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			CloseTime:   &closed,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		res := &scheduleResult{}
		classifyFirings(res, expectedFires, tl)
		require.Equal(t, 1, res.Matched)
		require.Len(t, res.Missed, 1)
		require.Equal(t, "real_miss", res.Categories[res.Missed[0]])
	})

	t.Run("skip_overlap", func(t *testing.T) {
		// First workflow still running at 19:00 -> missed 19:00 is skip_overlap.
		expectedFires := []time.Time{
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T19:00:00Z"),
		}
		tl := &timeline{byWorkflowID: map[string]*workflowChain{}}
		tl.addExecution(timelineEntry{
			WorkflowID:  "w1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		res := &scheduleResult{}
		classifyFirings(res, expectedFires, tl)
		require.Equal(t, "skip_overlap", res.Categories[mustParseTime("2026-05-19T19:00:00Z")])
	})
}

type fakeScheduleLoader struct {
	entries []scheduleEntry
	// describes, if set, returns these from DescribeSchedule. Lets tests model schedules created or modified after the
	// audit window starts.
	describes map[string]scheduleDescription
	// describeErrors, if set for a given scheduleID, makes DescribeSchedule return that error instead of looking up
	// a normal description. Used to model the "deleted between list and describe" race.
	describeErrors map[string]error
	// retention, if non-zero, is returned by NamespaceRetention. Defaults to 0, which the auditor treats as "no retention
	// guard" -- fine for most tests that use synthetic times.
	retention time.Duration
}

func (f *fakeScheduleLoader) ListSchedules(_ context.Context, _ string) ([]scheduleEntry, error) {
	return f.entries, nil
}

func (f *fakeScheduleLoader) LookupSchedule(_ context.Context, _, scheduleID string) (scheduleEntry, error) {
	for _, e := range f.entries {
		if e.ID == scheduleID {
			return e, nil
		}
	}
	return scheduleEntry{}, status.Error(codes.NotFound, "schedule not found")
}

func (f *fakeScheduleLoader) DescribeSchedule(_ context.Context, _, scheduleID string) (scheduleDescription, error) {
	if err, ok := f.describeErrors[scheduleID]; ok {
		return scheduleDescription{}, err
	}
	if f.describes == nil {
		return scheduleDescription{}, nil
	}
	return f.describes[scheduleID], nil
}

func (f *fakeScheduleLoader) NamespaceRetention(_ context.Context, _ string) (time.Duration, error) {
	return f.retention, nil
}

type fakeExecutionLoader struct {
	byScheduleID map[string][]timelineEntry
}

func (f *fakeExecutionLoader) ListExecutions(_ context.Context, _, scheduleIDFilter string, _, _ time.Time) (map[string][]timelineEntry, error) {
	if f.byScheduleID == nil {
		return map[string][]timelineEntry{}, nil
	}
	if scheduleIDFilter != "" {
		if entries, ok := f.byScheduleID[scheduleIDFilter]; ok {
			return map[string][]timelineEntry{scheduleIDFilter: entries}, nil
		}
		return map[string][]timelineEntry{}, nil
	}
	return f.byScheduleID, nil
}

func TestScheduleAuditor(t *testing.T) {
	t.Run("real_miss", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}}}
		closed := mustParseTime("2026-05-19T18:05:00Z")
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{
			"s1": {
				{
					WorkflowID:  "w1",
					StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
					CloseTime:   &closed,
					Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
				},
			},
		}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		// 18:00 matched; 19:00 and 20:00 missed with no prior fire that's still running -> real_miss.
		require.Equal(t, 2, results[0].Counts["real_miss"])
	})

	t.Run("concurrency", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		var schedules []scheduleEntry
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		for i := range 100 {
			id := fmt.Sprintf("s%d", i)
			schedules = append(schedules, scheduleEntry{ID: id, Spec: spec, WorkflowType: "W"})
			exec.byScheduleID[id] = nil // no executions -> all missed
		}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   &fakeScheduleLoader{entries: schedules},
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 100)
	})

	t.Run("schedule-id filter: only analyzes selected", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{entries: []scheduleEntry{
			{ID: "s1", Spec: spec, WorkflowType: "W"},
			{ID: "s2", Spec: spec, WorkflowType: "W"},
			{ID: "s3", Spec: spec, WorkflowType: "W"},
		}}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			ScheduleID:  "s2",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "s2", results[0].ScheduleID)
	})

	t.Run("schedule-id filter: unknown returns error", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []scheduleEntry{{ID: "s1"}}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			ScheduleID:  "missing",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{},
			Progress:    io.Discard,
		}
		_, err := a.run(context.Background())
		require.ErrorContains(t, err, `schedule "missing" not found in namespace "ns"`)
	})

	t.Run("paused schedule is skipped", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{entries: []scheduleEntry{
			{ID: "active", Spec: spec, WorkflowType: "W"},
			{ID: "paused", Spec: spec, WorkflowType: "W", Paused: true},
		}}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		// Only the active schedule should produce a result.
		require.Len(t, results, 1)
		require.Equal(t, "active", results[0].ScheduleID)
	})

	t.Run("window past retention is skipped", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		// Retention 30d, window 40d ago -- well past safe boundary.
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: time.Now().Add(-40 * 24 * time.Hour),
			WindowEnd:   time.Now().Add(-39 * 24 * time.Hour),
			Schedules: &fakeScheduleLoader{
				entries:   []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}},
				retention: 30 * 24 * time.Hour,
			},
			Executions: &fakeExecutionLoader{},
			Progress:   io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Nil(t, results, "expected nil results when window is past retention")
	})

	t.Run("long runner closed during window -> skip_overlap", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		// Workflow started 7 days before the window and closed mid-window.
		closeTime := mustParseTime("2026-05-19T19:30:00Z")
		exec := &fakeExecutionLoader{
			byScheduleID: map[string][]timelineEntry{
				"s1": {
					{
						WorkflowID:  "wLongRunner",
						StartTime:   mustParseTime("2026-05-12T17:09:00Z"),
						CloseTime:   &closeTime,
						Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						NominalTime: mustParseTime("2026-05-12T17:09:00Z"),
					},
				},
			},
		}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T21:00:00Z"),
			Schedules:   &fakeScheduleLoader{entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}}},
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		// All three expected fires (19:00, 20:00, 21:00) fall inside the closed chain's [start, close) interval up to 19:30,
		// so at least the 19:00 fire must be skip_overlap; 20:00 and 21:00 land after close and are legitimately real_miss in
		// this synthetic test (no other blocker), but the key assertion is that we *did* see the chain.
		require.GreaterOrEqual(t, r.Counts["skip_overlap"], 1, "expected the closed long-runner to block at least one fire")
	})

	t.Run("still-running workflow started long before window -> skip_overlap", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		// Window 18:00-21:00. Workflow started 7 days earlier and still running. The alive-during-window query returns this
		// via the CloseTime IS NULL branch, regardless of how far back StartTime is.
		exec := &fakeExecutionLoader{
			byScheduleID: map[string][]timelineEntry{
				"s1": {
					{
						WorkflowID:  "wLongRunner",
						StartTime:   mustParseTime("2026-05-12T17:09:00Z"),
						Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						NominalTime: mustParseTime("2026-05-12T17:09:00Z"),
					},
				},
			},
		}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T21:00:00Z"),
			Schedules:   &fakeScheduleLoader{entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}}},
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		require.Equal(t, 3, r.Expected, "three hourly fires expected (19:00, 20:00, 21:00)")
		require.Zero(t, r.Counts["real_miss"], "the long-running workflow blocks every fire -> no real_miss")
		require.Equal(t, 3, r.Counts["skip_overlap"], "every fire should be skip_overlap")
	})

	t.Run("postProcess: created after windowEnd excluded from results", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{
			entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}},
			describes: map[string]scheduleDescription{
				// Created one day after the window ends.
				"s1": {CreateTime: mustParseTime("2026-05-21T00:00:00Z")},
			},
		}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Empty(t, results, "schedule was created after windowEnd, should produce no result")
	})

	t.Run("postProcess: created mid-window truncates expected", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{
			entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}},
			// Created in the middle of a 4-hour window (18:00 - 22:00).
			describes: map[string]scheduleDescription{
				"s1": {CreateTime: mustParseTime("2026-05-19T20:30:00Z")},
			},
		}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		// First-pass would have flagged 4 expected (19:00, 20:00, 21:00, 22:00) all real_miss. Re-analysis with
		// createTime=20:30 truncates to fires at 21:00 and 22:00 only -> 2 real_miss.
		require.Equal(t, 2, r.Counts["real_miss"],
			"expected 2 real_miss (21:00, 22:00); 19:00 and 20:00 were pre-creation and shouldn't count")
	})

	t.Run("postProcess: modified during window -> inconclusive", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{
			entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}},
			describes: map[string]scheduleDescription{
				// Created well before window, but spec updated mid-window.
				"s1": {
					CreateTime: mustParseTime("2026-01-01T00:00:00Z"),
					UpdateTime: mustParseTime("2026-05-19T19:30:00Z"),
				},
			},
		}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		// With no executions and 4 expected hourly fires (19:00, 20:00, 21:00, 22:00), the initial analysis would produce 4
		// real_miss. Because the spec was updated mid-window, all 4 should be moved to inconclusive_schedule_changed.
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		require.Zero(t, r.Counts["real_miss"], "expected real_miss reclassified")
		require.Equal(t, 4, r.Counts["inconclusive_schedule_changed"])
	})

	t.Run("postProcess: exhausted excluded from results", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{
			entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}},
			describes: map[string]scheduleDescription{
				// Pre-existing schedule, never modified, but exhausted.
				"s1": {
					CreateTime: mustParseTime("2026-01-01T00:00:00Z"),
					Exhausted:  true,
				},
			},
		}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Empty(t, results, "exhausted schedule should be dropped")
	})

	t.Run("postProcess: unsupported policy -> unsupported bucket", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{
			entries: []scheduleEntry{{ID: "s1", Spec: spec, WorkflowType: "W"}},
			describes: map[string]scheduleDescription{
				"s1": {
					CreateTime:        mustParseTime("2026-01-01T00:00:00Z"),
					UnsupportedReason: "overlap_buffer_all",
				},
			},
		}
		exec := &fakeExecutionLoader{byScheduleID: map[string][]timelineEntry{}}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		require.Zero(t, r.Counts["real_miss"], "real_miss reclassified")
		require.Equal(t, 4, r.Counts["unsupported_policy"])
		require.Equal(t, "overlap_buffer_all", r.UnsupportedReason)
	})

	t.Run("postProcess: NotFound race (deleted between list and describe)", func(t *testing.T) {
		// Hourly spec; window contains 19:00 and 20:00. With no executions, both are real_miss -> postProcess runs.
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
				{
					Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
					Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				},
			},
		}
		loader := &fakeScheduleLoader{
			entries: []scheduleEntry{
				{ID: "deleted", Spec: spec, WorkflowType: "W"},
				{ID: "alive", Spec: spec, WorkflowType: "W"},
			},
			describeErrors: map[string]error{
				// "deleted" returns NotFound when postProcess calls DescribeSchedule.
				"deleted": status.Error(codes.NotFound, "schedule not found"),
			},
		}
		a := &scheduleAuditor{
			Namespace:   "ns",
			WindowStart: mustParseTime("2026-05-19T18:30:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:30:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{},
			Progress:    io.Discard,
		}
		results, err := a.run(context.Background())
		require.NoError(t, err, "NotFound during postProcess must not propagate as an error")
		// "deleted" gets dropped silently; "alive" gets describe -> no UpdateTime/Exhausted/etc. -> row stands.
		require.Len(t, results, 1)
		require.Equal(t, "alive", results[0].ScheduleID)
	})
}

func TestWriter_Stdout_FlatCSV(t *testing.T) {
	t.Run("emits header + row with exact column order and values", func(t *testing.T) {
		missTime := mustParseTime("2026-05-19T19:00:00Z")
		skipTime := mustParseTime("2026-05-19T20:00:00Z")
		results := []scheduleResult{{
			Namespace: "ns1", ScheduleID: "s1", WorkflowType: "Foo,Bar",
			JitterSeconds: 30, CatchupWindowSeconds: 600,
			Expected: 5, Actual: 4, Matched: 3,
			Missed: []time.Time{missTime, skipTime},
			Categories: map[time.Time]string{
				missTime: categoryRealMiss,
				skipTime: categorySkipOverlap,
			},
			Counts: map[string]int{
				categoryRealMiss:    1,
				categorySkipOverlap: 1,
			},
			UnsupportedReason: "",
		}}
		var buf bytes.Buffer
		require.NoError(t, writeFlatCSV(&buf, results))
		rows, err := csv.NewReader(&buf).ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2, "expected header + 1 data row")
		require.Equal(t, flatCSVBaseHeader, rows[0])
		require.Equal(t, []string{
			"ns1", "s1", "Foo;Bar", // comma in workflow type sanitized to semicolon
			"30",          // jitter_s
			"5", "4", "3", // expected, actual, matched
			"2",                // missed
			"1", "1", "0", "0", // real_miss, skip_overlap, inconclusive, unsupported
			"",                     // unsupported_reason
			"600",                  // catchup_window_s
			"2026-05-19T19:00:00Z", // real_miss_times: only the real_miss, not the skip
		}, rows[1])
	})

	t.Run("skips results with no missed fires", func(t *testing.T) {
		results := []scheduleResult{{
			Namespace: "ns1", ScheduleID: "perfect", WorkflowType: "W",
			Expected: 5, Actual: 5, Matched: 5,
			Missed: nil,
		}}
		var buf bytes.Buffer
		require.NoError(t, writeFlatCSV(&buf, results))
		rows, err := csv.NewReader(&buf).ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 1, "header only, no data row for results with no misses")
		require.Equal(t, flatCSVBaseHeader, rows[0])
	})

	t.Run("truncates real_miss_times beyond 20 with ...+N more suffix", func(t *testing.T) {
		base := mustParseTime("2026-05-19T00:00:00Z")
		var missed []time.Time
		categories := map[time.Time]string{}
		for i := range 25 {
			tm := base.Add(time.Duration(i) * time.Minute)
			missed = append(missed, tm)
			categories[tm] = categoryRealMiss
		}
		results := []scheduleResult{{
			Namespace: "ns1", ScheduleID: "s1", WorkflowType: "W",
			Expected: 25, Missed: missed, Categories: categories,
			Counts: map[string]int{categoryRealMiss: 25},
		}}
		var buf bytes.Buffer
		require.NoError(t, writeFlatCSV(&buf, results))
		rows, err := csv.NewReader(&buf).ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2)
		realMissTimes := rows[1][len(flatCSVBaseHeader)-1]
		require.Contains(t, realMissTimes, "2026-05-19T00:00:00Z", "earliest time present")
		require.Contains(t, realMissTimes, "2026-05-19T00:19:00Z", "20th time present")
		require.NotContains(t, realMissTimes, "2026-05-19T00:20:00Z", "21st time elided")
		require.Contains(t, realMissTimes, "...+5 more")
	})
}

// TestUnsupportedPolicyReason exercises every policy/state combination unsupportedPolicyReason recognizes, plus the
// nil-policies and supported-policies cases that must return empty.
func TestUnsupportedPolicyReason(t *testing.T) {
	cases := []struct {
		name     string
		policies *schedulepb.SchedulePolicies
		expected string
	}{
		{"nil policies", nil, ""},
		{"default SKIP (unspecified)", &schedulepb.SchedulePolicies{}, ""},
		{
			"explicit SKIP",
			&schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
			"",
		},
		{
			"BUFFER_ONE is supported",
			&schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE},
			"",
		},
		{
			"BUFFER_ALL flagged",
			&schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
			"overlap_buffer_all",
		},
		{
			"ALLOW_ALL flagged",
			&schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
			"overlap_allow_all",
		},
		{
			"CANCEL_OTHER flagged",
			&schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER},
			"overlap_cancel_other",
		},
		{
			"TERMINATE_OTHER flagged",
			&schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER},
			"overlap_terminate_other",
		},
		{
			"KeepOriginalWorkflowId only",
			&schedulepb.SchedulePolicies{KeepOriginalWorkflowId: true},
			"keep_original_workflow_id",
		},
		{
			"KeepOriginalWorkflowId + BUFFER_ALL joined with semicolon",
			&schedulepb.SchedulePolicies{
				KeepOriginalWorkflowId: true,
				OverlapPolicy:          enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			},
			"keep_original_workflow_id;overlap_buffer_all",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, unsupportedPolicyReason(tc.policies))
		})
	}
}

// TestPostProcess_NotFoundRace exercises the race-handling branch where DescribeSchedule returns NotFound for a
// schedule that ListSchedules saw moments earlier. The row should be silently dropped (no error returned).
// TestDecodePayloads covers the skip-on-failure contract of decodeScheduledByID and decodeNominalStartTime so the
// caller can drop visibility rows with malformed search-attribute payloads instead of grouping them under empty keys.
func TestDecodePayloads(t *testing.T) {
	makePayload := func(encoding string, data []byte) *commonpb.Payload {
		return &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte(encoding)},
			Data:     data,
		}
	}

	t.Run("decodeScheduledByID nil payload returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(nil)
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID wrong encoding returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(makePayload("proto/binary", []byte("anything")))
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID malformed json returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(makePayload("json/plain", []byte("not-json-quoted")))
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID valid json returns the string", func(t *testing.T) {
		actual, ok := decodeScheduledByID(makePayload("json/plain", []byte(`"my-schedule-id"`)))
		require.True(t, ok)
		require.Equal(t, "my-schedule-id", actual)
	})

	t.Run("decodeNominalStartTime nil payload returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(nil)
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime wrong encoding returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(makePayload("proto/binary", []byte("anything")))
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime non-RFC3339 returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(makePayload("json/plain", []byte(`"not-a-timestamp"`)))
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime valid RFC3339 parses", func(t *testing.T) {
		actual, ok := decodeNominalStartTime(makePayload("json/plain", []byte(`"2026-05-19T18:00:00Z"`)))
		require.True(t, ok)
		require.Equal(t, mustParseTime("2026-05-19T18:00:00Z"), actual)
	})
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
