package scheduleaudit

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestScheduledTimes(t *testing.T) {
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
		sts, err := ScheduledTimes(spec, "",
			time.Date(2026, 5, 19, 18, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 23, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Equal(t, []time.Time{
			time.Date(2026, 5, 19, 19, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 20, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 21, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 22, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 19, 23, 0, 0, 0, time.UTC),
		}, nominalTimes(sts))
	})

	t.Run("cron_string hourly", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{CronString: []string{"0 * * * *"}}
		sts, err := ScheduledTimes(spec, "",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T21:00:00Z"))
		require.NoError(t, err)
		require.Equal(t, []time.Time{
			mustParseTime("2026-05-19T19:00:00Z"),
			mustParseTime("2026-05-19T20:00:00Z"),
			mustParseTime("2026-05-19T21:00:00Z"),
		}, nominalTimes(sts))
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
		sts, err := ScheduledTimes(spec, "",
			mustParseTime("2026-05-19T00:00:00Z"),
			mustParseTime("2026-05-20T00:00:00Z"))
		require.NoError(t, err)
		require.Equal(t, []time.Time{mustParseTime("2026-05-19T16:00:00Z")}, nominalTimes(sts))
	})

	t.Run("end_time in past returns no fires", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{hourlyStructured()},
			EndTime:            timestamppb.New(mustParseTime("2026-01-01T00:00:00Z")),
		}
		sts, err := ScheduledTimes(spec, "",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T22:00:00Z"))
		require.NoError(t, err)
		require.Empty(t, sts)
	})

	t.Run("interval with phase produces 15-min cadence offset by phase", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(15 * time.Minute),
				Phase:    durationpb.New(6*time.Minute + 7*time.Second),
			}},
		}
		sts, err := ScheduledTimes(spec, "",
			mustParseTime("2026-05-19T18:00:00Z"),
			mustParseTime("2026-05-19T19:00:00Z"))
		require.NoError(t, err)
		actual := nominalTimes(sts)
		require.NotEmpty(t, actual)
		for i := 1; i < len(actual); i++ {
			require.Equal(t, 15*time.Minute, actual[i].Sub(actual[i-1]),
				"consecutive fires must be exactly 15min apart")
		}
		require.Equal(t, int64(0), (actual[0].Unix()-367)%900, "first fire must align with phase=6m7s")
	})
}

func TestStartedWorkflows(t *testing.T) {
	t.Run("still running is active after start", func(t *testing.T) {
		sw := startedWorkflows{}
		sw.add(Execution{
			WorkflowID:  "wf1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		require.True(t, sw.anyActiveAt(mustParseTime("2026-05-19T19:00:00Z")))
	})

	t.Run("closed before query time is inactive", func(t *testing.T) {
		sw := startedWorkflows{}
		closeTime := mustParseTime("2026-05-19T18:30:00Z")
		sw.add(Execution{
			WorkflowID:  "wf1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			CloseTime:   &closeTime,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		require.False(t, sw.anyActiveAt(mustParseTime("2026-05-19T19:00:00Z")))
	})

	t.Run("ContinueAsNew chain treated as one active action", func(t *testing.T) {
		sw := startedWorkflows{}
		canTime := mustParseTime("2026-05-19T18:30:00Z")
		sw.add(Execution{
			WorkflowID:  "wfChain",
			RunID:       "run1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			CloseTime:   &canTime,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		sw.add(Execution{
			WorkflowID:  "wfChain",
			RunID:       "run2",
			StartTime:   mustParseTime("2026-05-19T18:30:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		require.Len(t, sw, 1)
		require.True(t, sw.anyActiveAt(mustParseTime("2026-05-19T19:00:00Z")))
	})
}

func TestOverlapClassOf(t *testing.T) {
	cases := []struct {
		policy enumspb.ScheduleOverlapPolicy
		want   overlapClass
	}{
		{enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, dropsOnOverlap},
		{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, dropsOnOverlap},
		{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, dropsOnOverlap},
		{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, delaysOnOverlap},
		{enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, delaysOnOverlap},
		{enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, delaysOnOverlap},
		{enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, concurrent},
	}
	for _, tc := range cases {
		t.Run(overlapPolicyName(tc.policy), func(t *testing.T) {
			require.Equal(t, tc.want, overlapClassOf(tc.policy))
		})
	}
}

func TestClassify(t *testing.T) {
	scheduled := []time.Time{
		mustParseTime("2026-05-19T18:00:00Z"),
		mustParseTime("2026-05-19T19:00:00Z"),
	}

	completed := func(sw startedWorkflows, id, start, closeAt string) {
		c := mustParseTime(closeAt)
		sw.add(Execution{
			WorkflowID:  id,
			StartTime:   mustParseTime(start),
			CloseTime:   &c,
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			NominalTime: mustParseTime(start),
		})
	}

	t.Run("all matched", func(t *testing.T) {
		sw := startedWorkflows{}
		completed(sw, "w1", "2026-05-19T18:00:00Z", "2026-05-19T18:05:00Z")
		completed(sw, "w2", "2026-05-19T19:00:00Z", "2026-05-19T19:05:00Z")
		r := &Result{}
		Classify(r, scheduled, sw, sw, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
		require.Equal(t, 2, r.Matched)
		require.Empty(t, r.Missed)
	})

	t.Run("real_miss when nothing active", func(t *testing.T) {
		sw := startedWorkflows{}
		completed(sw, "w1", "2026-05-19T18:00:00Z", "2026-05-19T18:05:00Z")
		r := &Result{}
		Classify(r, scheduled, sw, sw, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
		require.Equal(t, 1, r.Matched)
		require.Equal(t, categoryRealMiss, r.Missed[mustParseTime("2026-05-19T19:00:00Z")])
	})

	t.Run("drops policy + overlap -> skip_overlap", func(t *testing.T) {
		sw := startedWorkflows{}
		// A workflow still running across 19:00 blocks that fire.
		sw.add(Execution{
			WorkflowID:  "w1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		r := &Result{}
		Classify(r, scheduled, sw, sw, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
		require.Equal(t, categorySkipOverlap, r.Missed[mustParseTime("2026-05-19T19:00:00Z")])
	})

	t.Run("delays policy + overlap -> real_miss (not skip)", func(t *testing.T) {
		sw := startedWorkflows{}
		// Same active workflow, but BUFFER_ALL never drops: the 19:00 fire should have run later, so it's a real miss.
		sw.add(Execution{
			WorkflowID:  "w1",
			StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
		})
		r := &Result{}
		Classify(r, scheduled, sw, sw, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
		require.Equal(t, categoryRealMiss, r.Missed[mustParseTime("2026-05-19T19:00:00Z")])
		require.Zero(t, r.Count(categorySkipOverlap))
	})
}

type fakeScheduleLoader struct {
	entries     []ScheduleEntry
	retention   time.Duration
	namespaceID string
}

func (f *fakeScheduleLoader) ListScheduleIDs(_ context.Context, _ string, yield func(id string) error) error {
	for _, e := range f.entries {
		if err := yield(e.ID); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeScheduleLoader) LookupSchedule(_ context.Context, _, scheduleID string) (ScheduleEntry, error) {
	for _, e := range f.entries {
		if e.ID == scheduleID {
			return e, nil
		}
	}
	return ScheduleEntry{}, status.Error(codes.NotFound, "schedule not found")
}

func (f *fakeScheduleLoader) DescribeNamespace(_ context.Context, _ string) (NamespaceInfo, error) {
	return NamespaceInfo{ID: f.namespaceID, Retention: f.retention}, nil
}

type fakeExecutionLoader struct {
	byScheduleID map[string][]Execution
}

func (f *fakeExecutionLoader) ListExecutions(_ context.Context, _, scheduleID string, _, _ time.Time) ([]Execution, error) {
	return f.byScheduleID[scheduleID], nil
}

// runAudit drives the streaming Auditor over the given targets and returns the emitted results (order is not
// deterministic under concurrency) along with any error.
func runAudit(a *Auditor, targets ...Target) ([]Result, error) {
	in := make(chan Target, len(targets))
	for _, t := range targets {
		in <- t
	}
	close(in)
	var results []Result
	err := a.Run(context.Background(), in, func(r Result) error {
		results = append(results, r)
		return nil
	})
	return results, err
}

func TestAuditor(t *testing.T) {
	t.Run("real_miss", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{{ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W"}}}
		closed := mustParseTime("2026-05-19T18:05:00Z")
		exec := &fakeExecutionLoader{byScheduleID: map[string][]Execution{
			"s1": {{
				WorkflowID:  "w1",
				StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
				CloseTime:   &closed,
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
			}},
		}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T17:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		// Window (17:00, 20:00] yields 18:00, 19:00, 20:00. Only the 18:00 fire ran; nothing was running at 19:00/20:00
		// (w1 closed at 18:05) -> both real_miss.
		require.Equal(t, 1, results[0].Matched)
		require.Equal(t, 2, results[0].Count(categoryRealMiss))
		require.Equal(t, "ns", results[0].Namespace)
		require.Len(t, results[0].Observed, 1)
		require.Len(t, results[0].Scheduled, 3)
	})

	t.Run("concurrency: whole namespace streams every schedule", func(t *testing.T) {
		var schedules []ScheduleEntry
		exec := &fakeExecutionLoader{byScheduleID: map[string][]Execution{}}
		for i := range 100 {
			id := fmt.Sprintf("s%d", i)
			schedules = append(schedules, ScheduleEntry{ID: id, Spec: hourlyAllHoursSpec(), WorkflowType: "W"})
			exec.byScheduleID[id] = nil
		}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Concurrency: 16,
			RPS:         16,
			Schedules:   &fakeScheduleLoader{entries: schedules},
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 100)
	})

	t.Run("schedule-id target: only analyzes selected", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{
			{ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W"},
			{ID: "s2", Spec: hourlyAllHoursSpec(), WorkflowType: "W"},
			{ID: "s3", Spec: hourlyAllHoursSpec(), WorkflowType: "W"},
		}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns", ScheduleID: "s2"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "s2", results[0].ScheduleID)
	})

	t.Run("schedule-id target: unknown returns error", func(t *testing.T) {
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   &fakeScheduleLoader{entries: []ScheduleEntry{{ID: "s1"}}},
			Executions:  &fakeExecutionLoader{},
			Progress:    io.Discard,
		}
		_, err := runAudit(a, Target{Namespace: "ns", ScheduleID: "missing"})
		require.ErrorContains(t, err, `schedule "missing" not found in namespace "ns"`)
	})

	t.Run("listed schedule deleted mid-audit is skipped, not fatal", func(t *testing.T) {
		// The loader lists "ghost" (via the wrapper) but LookupSchedule only knows "real", so ghost's describe 404s.
		// Because ghost was discovered by listing (not named explicitly), the NotFound is swallowed.
		wrapped := &listExtraLoader{
			ScheduleLoader: &fakeScheduleLoader{entries: []ScheduleEntry{{ID: "real", Spec: hourlyAllHoursSpec(), WorkflowType: "W"}}},
			extraIDs:       []string{"ghost"},
		}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   wrapped,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "real", results[0].ScheduleID)
	})

	t.Run("paused schedule is skipped", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{
			{ID: "active", Spec: hourlyAllHoursSpec(), WorkflowType: "W"},
			{ID: "paused", Spec: hourlyAllHoursSpec(), WorkflowType: "W", Paused: true},
		}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "active", results[0].ScheduleID)
	})

	t.Run("window entirely past retention yields nothing after clamping", func(t *testing.T) {
		// WindowEnd (39d ago) is older than the retention boundary (30d ago), so clamping the start to the boundary
		// leaves an empty window -> no scheduled times -> no result.
		a := &Auditor{
			WindowStart: time.Now().Add(-40 * 24 * time.Hour),
			WindowEnd:   time.Now().Add(-39 * 24 * time.Hour),
			Schedules: &fakeScheduleLoader{
				entries:   []ScheduleEntry{{ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W"}},
				retention: 30 * 24 * time.Hour,
			},
			Executions: &fakeExecutionLoader{},
			Progress:   io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Empty(t, results, "expected no results when the whole window is past retention")
	})

	t.Run("window partly past retention clamps start to the boundary", func(t *testing.T) {
		// WindowStart (40d ago) precedes the retention boundary (30d ago) but WindowEnd (10d ago) is within it, so the
		// audited window is clamped to (boundary, WindowEnd] and the result reflects the clamped start.
		windowStart := time.Now().Add(-40 * 24 * time.Hour)
		retention := 30 * 24 * time.Hour
		a := &Auditor{
			WindowStart: windowStart,
			WindowEnd:   time.Now().Add(-10 * 24 * time.Hour),
			Schedules: &fakeScheduleLoader{
				entries:   []ScheduleEntry{{ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W"}},
				retention: retention,
			},
			Executions: &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:   io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		boundary := time.Now().Add(-retention)
		require.WithinDuration(t, boundary, results[0].WindowStart, time.Minute,
			"window start should be clamped to the retention boundary")
		require.True(t, results[0].WindowStart.After(windowStart), "clamped start must be after the original start")
	})

	t.Run("still-running workflow started before window -> skip_overlap under SKIP", func(t *testing.T) {
		exec := &fakeExecutionLoader{byScheduleID: map[string][]Execution{
			"s1": {{
				WorkflowID:  "wLongRunner",
				StartTime:   mustParseTime("2026-05-12T17:09:00Z"),
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				NominalTime: mustParseTime("2026-05-12T17:09:00Z"),
			}},
		}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T21:00:00Z"),
			Schedules:   &fakeScheduleLoader{entries: []ScheduleEntry{{ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W"}}},
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		require.Equal(t, 3, r.Expected, "three hourly fires expected (19:00, 20:00, 21:00)")
		require.Zero(t, r.Count(categoryRealMiss), "the long-running workflow blocks every fire under SKIP")
		require.Equal(t, 3, r.Count(categorySkipOverlap))
	})

	t.Run("created after windowEnd excluded", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{{
			ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W",
			CreateTime: mustParseTime("2026-05-21T00:00:00Z"),
		}}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T20:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Empty(t, results)
	})

	t.Run("created mid-window truncates expected", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{{
			ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W",
			CreateTime: mustParseTime("2026-05-19T20:30:00Z"),
		}}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, 2, results[0].Count(categoryRealMiss),
			"only 21:00 and 22:00 remain expected after truncating to CreateTime=20:30")
	})

	t.Run("modified during window -> inconclusive", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{{
			ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W",
			CreateTime: mustParseTime("2026-01-01T00:00:00Z"),
			UpdateTime: mustParseTime("2026-05-19T19:30:00Z"),
		}}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		require.Zero(t, r.Count(categoryRealMiss))
		require.Equal(t, 4, r.Count(categoryInconclusiveChanged))
	})

	t.Run("exhausted excluded", func(t *testing.T) {
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{{
			ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W",
			CreateTime: mustParseTime("2026-01-01T00:00:00Z"),
			Exhausted:  true,
		}}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  &fakeExecutionLoader{byScheduleID: map[string][]Execution{}},
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Empty(t, results)
	})

	t.Run("ALLOW_ALL: unmatched fires are real_miss despite concurrent workflows", func(t *testing.T) {
		closeTime := mustParseTime("2026-05-19T23:30:00Z")
		exec := &fakeExecutionLoader{byScheduleID: map[string][]Execution{
			"s1": {
				{WorkflowID: "concurrent-A", StartTime: mustParseTime("2026-05-19T17:00:00Z"), CloseTime: &closeTime, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, NominalTime: mustParseTime("2026-05-19T17:00:00Z")},
				{WorkflowID: "concurrent-B", StartTime: mustParseTime("2026-05-19T17:30:00Z"), CloseTime: &closeTime, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, NominalTime: mustParseTime("2026-05-19T17:30:00Z")},
			},
		}}
		loader := &fakeScheduleLoader{entries: []ScheduleEntry{{
			ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W",
			CreateTime: mustParseTime("2026-01-01T00:00:00Z"),
			Policies:   &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
		}}}
		a := &Auditor{
			WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
			Schedules:   loader,
			Executions:  exec,
			Progress:    io.Discard,
		}
		results, err := runAudit(a, Target{Namespace: "ns"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		r := results[0]
		require.Equal(t, 4, r.Count(categoryRealMiss))
		require.Zero(t, r.Count(categorySkipOverlap))
	})
}

func TestScheduledTimesJitter(t *testing.T) {
	spec := &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
			Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
			DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
			Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
		}},
		Jitter: durationpb.New(30 * time.Minute),
	}
	start := mustParseTime("2026-05-19T00:00:00Z")
	end := mustParseTime("2026-05-20T00:00:00Z")

	got, err := ScheduledTimes(spec, jitterSeed("ns-id", "s1"), start, end)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	sawJitter := false
	for _, st := range got {
		require.False(t, st.Jittered.Before(st.Nominal), "jittered must not precede nominal")
		require.LessOrEqual(t, st.Jittered.Sub(st.Nominal), 30*time.Minute, "jitter bounded by spec")
		if st.Jittered.After(st.Nominal) {
			sawJitter = true
		}
	}
	require.True(t, sawJitter, "over 24 hourly fires with a 30m jitter window, at least one should be jittered")

	// Deterministic: the same seed reproduces the scheduler's jittered times exactly.
	again, err := ScheduledTimes(spec, jitterSeed("ns-id", "s1"), start, end)
	require.NoError(t, err)
	require.Equal(t, got, again)
}

func TestActionDelays(t *testing.T) {
	// Four hourly fires (19:00-22:00). wfA/wfLong start on time; wfB is a pure "system was slow" case (no blocker, 40m
	// late); wfC is held behind the still-running wfLong and then takes a further 5m to start.
	closeA := mustParseTime("2026-05-19T19:30:00Z")
	closeB := mustParseTime("2026-05-19T20:50:00Z")
	closeLong := mustParseTime("2026-05-19T22:10:00Z")
	closeC := mustParseTime("2026-05-19T22:30:00Z")
	completed := enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	exec := &fakeExecutionLoader{byScheduleID: map[string][]Execution{
		"s1": {
			{WorkflowID: "wfA", StartTime: mustParseTime("2026-05-19T19:00:00Z"), CloseTime: &closeA, Status: completed, NominalTime: mustParseTime("2026-05-19T19:00:00Z")},
			{WorkflowID: "wfB", StartTime: mustParseTime("2026-05-19T20:40:00Z"), CloseTime: &closeB, Status: completed, NominalTime: mustParseTime("2026-05-19T20:00:00Z")},
			{WorkflowID: "wfLong", StartTime: mustParseTime("2026-05-19T21:00:00Z"), CloseTime: &closeLong, Status: completed, NominalTime: mustParseTime("2026-05-19T21:00:00Z")},
			{WorkflowID: "wfC", StartTime: mustParseTime("2026-05-19T22:15:00Z"), CloseTime: &closeC, Status: completed, NominalTime: mustParseTime("2026-05-19T22:00:00Z")},
		},
	}}
	loader := &fakeScheduleLoader{namespaceID: "ns-id", entries: []ScheduleEntry{{
		ID: "s1", Spec: hourlyAllHoursSpec(), WorkflowType: "W",
		Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
	}}}
	a := &Auditor{
		WindowStart: mustParseTime("2026-05-19T18:00:00Z"),
		WindowEnd:   mustParseTime("2026-05-19T22:00:00Z"),
		Schedules:   loader,
		Executions:  exec,
		Progress:    io.Discard,
	}
	results, err := runAudit(a, Target{Namespace: "ns"})
	require.NoError(t, err)
	require.Len(t, results, 1)

	byWF := map[string]ActionDelay{}
	for _, d := range results[0].Delays {
		byWF[d.WorkflowID] = d
	}
	require.Len(t, byWF, 4)

	// hourlyAllHoursSpec has no jitter, so actual == nominal everywhere.
	for id, d := range byWF {
		require.Equal(t, d.Nominal, d.Actual, "%s: no jitter -> actual == nominal", id)
		require.Zero(t, d.JitterOffset, "%s", id)
	}

	// On-time starts: no delay of any kind.
	require.Zero(t, byWF["wfA"].DispatchDelay)
	require.Zero(t, byWF["wfLong"].DispatchDelay)

	// wfB: system was slow to start it -- 40m dispatch delay with no overlap wait.
	require.Zero(t, byWF["wfB"].OverlapWait)
	require.Equal(t, 40*time.Minute, byWF["wfB"].DispatchDelay)
	require.Equal(t, 40*time.Minute, byWF["wfB"].E2EDelay)

	// wfC: held behind wfLong (closed 22:10), then a further 5m of system delay.
	c := byWF["wfC"]
	require.Equal(t, closeLong, c.Desired, "eligible when the blocking action closed")
	require.Equal(t, 10*time.Minute, c.OverlapWait)
	require.Equal(t, 5*time.Minute, c.DispatchDelay)
	require.Equal(t, 15*time.Minute, c.E2EDelay) // overlap_wait + dispatch_delay
}

// listExtraLoader wraps a ScheduleLoader to yield extra schedule IDs during listing that LookupSchedule doesn't know
// about -- emulating a schedule deleted between listing and describe.
type listExtraLoader struct {
	ScheduleLoader
	extraIDs []string
}

func (l *listExtraLoader) ListScheduleIDs(ctx context.Context, namespace string, yield func(id string) error) error {
	for _, id := range l.extraIDs {
		if err := yield(id); err != nil {
			return err
		}
	}
	return l.ScheduleLoader.ListScheduleIDs(ctx, namespace, yield)
}

// hourlyAllHoursSpec fires at the top of every hour, every day -- a deterministic generator of scheduled times.
func hourlyAllHoursSpec() *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
			Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
			DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
			Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
		}},
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
