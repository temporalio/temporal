package scheduler_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// everySecondSchedule is the shape of the production schedules that hit the catch-up
// death spiral: every second, a far-future keep-alive calendar, a 365d catchup window.
func everySecondSchedule(t testing.TB, overlap enumspb.ScheduleOverlapPolicy) *schedulepb.Schedule {
	var spec schedulepb.ScheduleSpec
	require.NoError(t, protojson.Unmarshal([]byte(everySecondKeepaliveSpecJSON), &spec))
	sched := defaultSchedule()
	sched.Spec = &spec
	sched.Policies.OverlapPolicy = overlap
	sched.Policies.CatchupWindow = durationpb.New(365 * 24 * time.Hour)
	return sched
}

func catchupTweakables(fn func(*scheduler.Tweakables)) scheduler.Tweakables {
	tw := scheduler.DefaultTweakables
	fn(&tw)
	return tw
}

func newCatchupProcessor(tw scheduler.Tweakables, mh metrics.Handler, logger log.Logger) scheduler.SpecProcessor {
	return scheduler.NewSpecProcessor(
		&scheduler.Config{Tweakables: func(string) scheduler.Tweakables { return tw }},
		mh, logger, newLegacySpecBuilder(0, 0))
}

// captureLogger records "LEVEL message" for each entry. It doesn't implement
// log.WithLogger, so log.With wraps it and tagged loggers still record here.
type captureLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *captureLogger) add(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, level+" "+msg)
}

func (l *captureLogger) count(level, msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if e == level+" "+msg {
			n++
		}
	}
	return n
}

func (l *captureLogger) Debug(msg string, _ ...tag.Tag)  { l.add("DEBUG", msg) }
func (l *captureLogger) Info(msg string, _ ...tag.Tag)   { l.add("INFO", msg) }
func (l *captureLogger) Warn(msg string, _ ...tag.Tag)   { l.add("WARN", msg) }
func (l *captureLogger) Error(msg string, _ ...tag.Tag)  { l.add("ERROR", msg) }
func (l *captureLogger) DPanic(msg string, _ ...tag.Tag) { l.add("DPANIC", msg) }
func (l *captureLogger) Panic(msg string, _ ...tag.Tag)  { l.add("PANIC", msg) }
func (l *captureLogger) Fatal(msg string, _ ...tag.Tag)  { l.add("FATAL", msg) }

// catchupRange runs one automated (generator) ProcessTimeRange over `gap` before end,
// with the generator's buffer limit.
func catchupRange(
	t *testing.T,
	schedule *schedulepb.Schedule,
	tw scheduler.Tweakables,
	gap time.Duration,
	manual bool,
) (*scheduler.ProcessedTimeRange, *scheduler.Scheduler, metricstest.CaptureSnapshot, *captureLogger, time.Time) {
	env := newTestEnv(t, withSchedule(schedule))
	sched := env.Scheduler
	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	end := time.Date(2026, 10, 16, 4, 47, 11, 0, time.UTC)
	limit := tw.MaxBufferSize
	logger := &captureLogger{}
	res, err := newCatchupProcessor(tw, rec, logger).ProcessTimeRange(
		sched, end.Add(-gap), end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", manual, &limit)
	require.NoError(t, err)
	return res, sched, capture.Snapshot(), logger, end
}

func TestProcessTimeRange_Catchup(t *testing.T) {
	skip := enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	chunked := metrics.ScheduleCatchupChunked.Name()
	skipped := metrics.ScheduleCatchupRangeSkipped.Name()
	catchupRangeTimer := metrics.ScheduleCatchupRange.Name()
	const skipMsg = "Schedule skipped rest of catch-up range: no later match could start"

	t.Run("unbounded-walk-without-a-limit", func(t *testing.T) {
		// The behavior that produced the death spiral: every match in the gap is
		// walked in one call, and everything past the buffer is counted as dropped.
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.MaxCatchupMatchesPerTask = 0 })
		res, _, m, _, end := catchupRange(t, everySecondSchedule(t, skip), tw, 6*time.Hour, false)
		require.False(t, res.More)
		require.Len(t, res.BufferedStarts, 1000)
		require.EqualValues(t, 6*3600-1000, res.DroppedCount)
		require.Equal(t, end, res.LastActionTime)
		require.Empty(t, m[chunked])
	})

	t.Run("chunked", func(t *testing.T) {
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.MaxCatchupMatchesPerTask = 500 })
		res, _, m, _, end := catchupRange(t, everySecondSchedule(t, skip), tw, 6*time.Hour, false)
		start := end.Add(-6 * time.Hour)
		require.True(t, res.More)
		require.Len(t, res.BufferedStarts, 500)
		require.Zero(t, res.DroppedCount)
		require.Equal(t, start.Add(500*time.Second), res.LastActionTime, "resume after the last match walked")
		require.Equal(t, start.Add(501*time.Second), res.NextWakeupTime, "the first unprocessed match")
		require.Len(t, m[chunked], 1)
		require.Equal(t, 6*time.Hour, m[catchupRangeTimer][0].Value)
	})

	t.Run("chunked-misses-are-counted-exactly", func(t *testing.T) {
		// Misses never reach the buffer, so only the chunk limit bounds them.
		schedule := everySecondSchedule(t, skip)
		schedule.Policies.CatchupWindow = durationpb.New(time.Hour)
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.MaxCatchupMatchesPerTask = 1000 })
		res, sched, _, _, end := catchupRange(t, schedule, tw, 3*time.Hour, false)
		require.True(t, res.More)
		require.Empty(t, res.BufferedStarts)
		require.EqualValues(t, 1000, sched.Info.MissedCatchupWindow)
		require.Equal(t, end.Add(-3*time.Hour+1000*time.Second), res.LastActionTime)
	})

	t.Run("skip-shortcut", func(t *testing.T) {
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.EnableCatchupSkipShortcut = true })
		res, sched, m, lg, end := catchupRange(t, everySecondSchedule(t, skip), tw, 30*24*time.Hour, false)
		require.False(t, res.More)
		require.Len(t, res.BufferedStarts, 1)
		require.Equal(t, end.Add(-30*24*time.Hour+time.Second), res.BufferedStarts[0].ActualTime.AsTime(), "the earliest match, as before")
		require.Zero(t, res.DroppedCount, "the skipped tail is not counted")
		require.Equal(t, end, res.LastActionTime)
		require.True(t, res.NextWakeupTime.After(end))
		require.Zero(t, sched.Info.MissedCatchupWindow)
		require.Len(t, m[skipped], 1)
		require.Equal(t, metrics.ScheduleCatchupSkipReasonSkipPolicy, m[skipped][0].Tags[metrics.ScheduleMissedReasonTag])
		require.Len(t, m[catchupRangeTimer], 1)
		require.Equal(t, 1, lg.count("WARN", skipMsg))
	})

	t.Run("buffer-full-shortcut", func(t *testing.T) {
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.EnableCatchupSkipShortcut = true })
		res, _, m, lg, end := catchupRange(t, everySecondSchedule(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL), tw, 30*24*time.Hour, false)
		require.False(t, res.More)
		require.Len(t, res.BufferedStarts, 1000)
		require.EqualValues(t, 1, res.DroppedCount, "the first drop, then the jump")
		require.Equal(t, end, res.LastActionTime)
		require.Equal(t, metrics.ScheduleCatchupSkipReasonBufferFull, m[skipped][0].Tags[metrics.ScheduleMissedReasonTag])
		require.Equal(t, 1, lg.count("WARN", skipMsg))
	})

	t.Run("short-skip-logs-at-info", func(t *testing.T) {
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.EnableCatchupSkipShortcut = true })
		res, _, m, lg, _ := catchupRange(t, everySecondSchedule(t, skip), tw, 5*time.Second, false)
		require.Len(t, res.BufferedStarts, 1)
		require.Len(t, m[skipped], 1)
		require.Empty(t, m[catchupRangeTimer])
		require.Equal(t, 1, lg.count("INFO", skipMsg))
		require.Zero(t, lg.count("WARN", skipMsg))
	})

	t.Run("single-match-is-not-a-skip", func(t *testing.T) {
		tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.EnableCatchupSkipShortcut = true })
		res, _, m, lg, _ := catchupRange(t, everySecondSchedule(t, skip), tw, time.Second, false)
		require.Len(t, res.BufferedStarts, 1)
		require.Empty(t, m[skipped])
		require.Zero(t, lg.count("INFO", skipMsg)+lg.count("WARN", skipMsg))
	})

	t.Run("no-shortcut-for-jittered-specs", func(t *testing.T) {
		// Jumping to end could skip a match whose nominal time has passed but whose
		// jittered time hasn't; chunking still bounds the walk.
		schedule := everySecondSchedule(t, skip)
		schedule.Spec.Jitter = durationpb.New(500 * time.Millisecond)
		tw := catchupTweakables(func(tw *scheduler.Tweakables) {
			tw.EnableCatchupSkipShortcut = true
			tw.MaxCatchupMatchesPerTask = 2000
		})
		res, _, m, _, _ := catchupRange(t, schedule, tw, time.Hour, false)
		require.True(t, res.More)
		require.Len(t, res.BufferedStarts, 1000)
		require.EqualValues(t, 1000, res.DroppedCount)
		require.Empty(t, m[skipped])
	})

	t.Run("manual-is-never-chunked-or-skipped", func(t *testing.T) {
		tw := catchupTweakables(func(tw *scheduler.Tweakables) {
			tw.EnableCatchupSkipShortcut = true
			tw.MaxCatchupMatchesPerTask = 10
			tw.MaxBufferSize = 5000
		})
		res, _, m, _, _ := catchupRange(t, everySecondSchedule(t, skip), tw, time.Hour, true)
		require.False(t, res.More)
		require.Len(t, res.BufferedStarts, 3600)
		require.Empty(t, m[chunked])
		require.Empty(t, m[skipped])
	})
}

// TestGenerator_ChunkedCatchup delivers the generator's own tasks through the real CHASM
// engine after a 3h stall: each task walks at most one chunk, commits its high water
// mark, and arms a follow-up task right away until it has caught up.
func TestGenerator_ChunkedCatchup(t *testing.T) {
	const chunk = 1000
	created := time.Date(2026, 10, 16, 0, 0, 0, 500000000, time.UTC)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(created)

	tw := catchupTweakables(func(tw *scheduler.Tweakables) { tw.MaxCatchupMatchesPerTask = chunk })
	realProcessor := newCatchupProcessor(tw, metrics.NoopMetricsHandler, log.NewNoopLogger())
	var results []*scheduler.ProcessedTimeRange
	processor := scheduler.NewMockSpecProcessor(gomock.NewController(t))
	processor.EXPECT().NextTime(gomock.Any(), gomock.Any()).DoAndReturn(realProcessor.NextTime).AnyTimes()
	processor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).DoAndReturn(func(
		s *scheduler.Scheduler, start, end time.Time, policy enumspb.ScheduleOverlapPolicy,
		workflowID, backfillID string, manual bool, limit *int,
	) (*scheduler.ProcessedTimeRange, error) {
		res, err := realProcessor.ProcessTimeRange(s, start, end, policy, workflowID, backfillID, manual, limit)
		if err == nil {
			results = append(results, res)
		}
		return res, err
	}).AnyTimes()

	env := newSchedulerTestEngine(t, everySecondSchedule(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP),
		withEngineTimeSource(timeSource), withEngineSpecProcessor(processor))
	results = nil

	// Stall: 3h pass without the generator's timer being delivered.
	now := created.Add(3 * time.Hour)
	timeSource.Update(now)
	for range 20 {
		_, err := env.engine.FirePureTasks(env.rootRef, now)
		require.NoError(t, err)
		if len(results) > 0 && !results[len(results)-1].More {
			break
		}
		// Follow-up tasks are armed just after the previous task's time.
		now = now.Add(time.Millisecond)
		timeSource.Update(now)
	}

	// 3h of matches = 10800 in chunks of 1000.
	require.Len(t, results, 11)
	for i, r := range results[:len(results)-1] {
		require.True(t, r.More, "chunk %d", i)
		require.Equal(t, created.Truncate(time.Second).Add(time.Duration(i+1)*chunk*time.Second), r.LastActionTime, "chunk %d", i)
	}
	require.False(t, results[len(results)-1].More)
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Equal(t, now.Truncate(time.Second), s.Generator.Get(ctx).LastProcessedTime.AsTime())
		return nil
	}))
}
