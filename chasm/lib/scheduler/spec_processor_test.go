package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	testSpecProcessor struct {
		scheduler.SpecProcessor
		mockMetrics *metrics.MockHandler
	}

	specProcessorSuite struct {
		schedulerSuite
		processor *testSpecProcessor
	}
)

func TestSpecProcessorSuite(t *testing.T) {
	suite.Run(t, &specProcessorSuite{})
}
func (s *specProcessorSuite) SetupTest() {
	s.schedulerSuite.SetupTest()
	s.processor = newTestSpecProcessor(s.controller)
}

func newTestSpecProcessor(ctrl *gomock.Controller) *testSpecProcessor {
	mockMetrics := metrics.NewMockHandler(ctrl)
	mockMetrics.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	mockMetrics.EXPECT().WithTags(gomock.Any()).Return(mockMetrics).AnyTimes()
	mockMetrics.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	return &testSpecProcessor{
		SpecProcessor: scheduler.NewSpecProcessor(
			&scheduler.Config{
				Tweakables: func(_ string) scheduler.Tweakables {
					return scheduler.DefaultTweakables
				},
			},
			mockMetrics,
			log.NewTestLogger(),
			newLegacySpecBuilder(0, 0),
		),
	}
}

func (s *specProcessorSuite) TestProcessTimeRange_LimitedActions() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval)

	// A schedule with an action limit and remaining actions should buffer actions.
	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 1

	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(1, len(res.BufferedStarts))

	// When a schedule has an action limit that has been exceeded, we don't bother
	// buffering additional actions.
	sched.Schedule.State.RemainingActions = 0

	res, err = s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(0, len(res.BufferedStarts))

	// Manual starts should always be allowed.
	backfillID := "backfill"
	res, err = s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), backfillID, true, nil)
	s.NoError(err)
	s.Equal(1, len(res.BufferedStarts))
	bufferedStart := res.BufferedStarts[0]
	s.True(bufferedStart.Manual)
	s.Contains(bufferedStart.RequestId, backfillID)
	s.NotEmpty(bufferedStart.WorkflowId)
}

func (s *specProcessorSuite) TestProcessTimeRange_UpdateAfterHighWatermark() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)

	// Below window would give 6 actions, but the update time halves that.
	base := time.Now()
	start := base.Add(-defaultInterval * 3)
	end := base.Add(defaultInterval * 3)

	// Actions taking place in time before the last update time should be dropped.
	sched.Info.UpdateTime = timestamppb.Now()

	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(3, len(res.BufferedStarts))
}

// Tests that an update between a nominal time and jittered time for a start, that doesn't
// modify that start, will still start it.
func (s *specProcessorSuite) TestProcessTimeRange_UpdateBetweenNominalAndJitter() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	schedule := defaultSchedule()
	schedule.Policies.CatchupWindow = durationpb.New(2 * time.Hour)
	schedule.Spec = &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(1 * time.Hour),
		}},
		Jitter: durationpb.New(1 * time.Hour),
	}
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)

	// Generate a start with a long jitter period.
	base := time.Date(2025, 03, 31, 1, 0, 0, 0, time.UTC)
	start := base.Add(-1 * time.Minute)
	end := start.Add(1 * time.Hour)

	// Set our update time between the start's nominal and jittered time.
	updateTime := start.Add(10 * time.Minute)
	sched.Info.UpdateTime = timestamppb.New(updateTime)

	// A single start should have been buffered.
	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(1, len(res.BufferedStarts))

	// Validates the test case.
	actualTime := res.BufferedStarts[0].GetActualTime().AsTime()
	nominalTime := res.BufferedStarts[0].GetNominalTime().AsTime()
	s.True(nominalTime.Before(updateTime))
	s.True(actualTime.After(updateTime))
}

func (s *specProcessorSuite) TestProcessTimeRange_CatchupWindow() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)

	// When an action would fall outside of the schedule's catchup window, it should
	// be dropped.
	end := time.Now()
	start := end.Add(-defaultCatchupWindow * 2)

	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(5, len(res.BufferedStarts))
}

func (s *specProcessorSuite) TestProcessTimeRange_Limit() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval * 5)

	// When a limit pointer is provided, its value should be decremented with each
	// action buffered, ProcessTimeRange should return once the limit has been
	// exhausted.
	limit := 2

	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, &limit)
	s.NoError(err)
	s.Equal(2, len(res.BufferedStarts))
	s.Equal(0, limit)
}

func (s *specProcessorSuite) TestProcessTimeRange_OverlapPolicy() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval * 5)

	// Check that a default overlap policy (SKIP) is applied, even when left unspecified.
	sched.Schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED

	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(5, len(res.BufferedStarts))
	for _, b := range res.BufferedStarts {
		s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, b.OverlapPolicy)
	}

	// Check that a specified overlap policy is applied.
	overlapPolicy := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	sched.Schedule.Policies.OverlapPolicy = overlapPolicy

	res, err = s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(5, len(res.BufferedStarts))
	for _, b := range res.BufferedStarts {
		s.Equal(overlapPolicy, b.OverlapPolicy)
	}
}

func (s *specProcessorSuite) TestProcessTimeRange_Basic() {
	ctx := chasm.NewMutableContext(context.Background(), s.node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval * 5)

	// Validate returned BufferedStarts for unique action times and request IDs.
	res, err := s.processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	s.NoError(err)
	s.Equal(5, len(res.BufferedStarts))

	uniqueTimes := make(map[time.Time]bool)
	uniqueIDs := make(map[string]bool)
	for _, b := range res.BufferedStarts {
		s.False(b.Manual)

		actualTime := b.ActualTime.AsTime()
		s.False(uniqueTimes[actualTime])
		s.False(uniqueIDs[b.RequestId])
		uniqueTimes[actualTime] = true
		uniqueIDs[b.RequestId] = true

		// Validate WorkflowId format: scheduled-wf-{RFC3339 timestamp}
		nominalTime := b.NominalTime.AsTime()
		expectedTimestamp := nominalTime.Truncate(time.Second).Format(time.RFC3339)
		s.Equal("scheduled-wf-"+expectedTimestamp, b.WorkflowId)
	}

	// Validate next wakeup time.
	s.GreaterOrEqual(res.NextWakeupTime, end)
	s.Less(res.NextWakeupTime, end.Add(defaultInterval*2))
}

func TestProcessTimeRange_ComputeLimitExceeded(t *testing.T) {
	sched, _, _ := setupSchedulerForTest(t)

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	sched.Schedule.Spec = &schedulepb.ScheduleSpec{
		Calendar:        []*schedulepb.CalendarSpec{everySecond},
		ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
	}

	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	specBuilder := newLegacySpecBuilder(0, 100)
	processor := scheduler.NewSpecProcessor(
		&scheduler.Config{
			Tweakables: func(_ string) scheduler.Tweakables { return scheduler.DefaultTweakables },
		},
		rec,
		log.NewTestLogger(),
		specBuilder,
	)

	end := time.Now()
	start := end.Add(-defaultInterval)

	res, err := processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	require.NoError(t, err, "an over-excluded spec must not error out of ProcessTimeRange (would DLQ the generator task)")
	require.Empty(t, res.BufferedStarts, "no action should be buffered for an over-excluded spec")
	require.True(t, res.NextWakeupTime.IsZero(), "no wakeup should be armed; the schedule takes no further action")

	sched.Schedule.State.Paused = true
	res, err = processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	require.NoError(t, err)
	require.Empty(t, res.BufferedStarts)
	require.True(t, res.NextWakeupTime.IsZero())

	recorded := capture.Snapshot()[metrics.ScheduleComputeLimitExceeded.Name()]
	require.Len(t, recorded, 2, "expected the compute-limit counter to fire on both the active and paused paths")
}

func TestProcessTimeRange_ComputeLimitWarning(t *testing.T) {
	sched, _, _ := setupSchedulerForTest(t)

	end := time.Now()
	start := end.Add(-defaultInterval)

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	sched.Schedule.Spec = &schedulepb.ScheduleSpec{
		Calendar:        []*schedulepb.CalendarSpec{everySecond},
		ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
		// A short end time bounds the (otherwise unlimited) scan so the test stays fast while
		// still crossing the small warn threshold below.
		EndTime: timestamppb.New(end.Add(30 * time.Second)),
	}

	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	// Warn threshold only; leave the hard limit at its default (disabled).
	specBuilder := newLegacySpecBuilder(5, 0)
	processor := scheduler.NewSpecProcessor(
		&scheduler.Config{
			Tweakables: func(_ string) scheduler.Tweakables { return scheduler.DefaultTweakables },
		},
		rec,
		log.NewTestLogger(),
		specBuilder,
	)

	res, err := processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	require.NoError(t, err, "crossing the warn threshold must not error out of ProcessTimeRange")
	require.Empty(t, res.BufferedStarts, "no action should be buffered for an over-excluded spec")
	require.True(t, res.NextWakeupTime.IsZero(), "no wakeup should be armed for an over-excluded spec")

	sched.Schedule.State.Paused = true
	res, err = processor.ProcessTimeRange(sched, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
	require.NoError(t, err)
	require.Empty(t, res.BufferedStarts)
	require.True(t, res.NextWakeupTime.IsZero())

	require.Empty(t, capture.Snapshot()[metrics.ScheduleComputeLimitExceeded.Name()],
		"the hard limit is disabled, so the exceeded counter must not fire")
	recorded := capture.Snapshot()[metrics.ScheduleComputeLimitWarning.Name()]
	require.Len(t, recorded, 2, "expected the warn counter to fire on both the active and paused paths")
}
