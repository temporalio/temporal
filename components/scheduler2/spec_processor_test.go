package scheduler2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/scheduler2"
	scheduler1 "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	testSpecProcessor struct {
		scheduler2.SpecProcessor

		mockMetrics *metrics.MockHandler
	}
)

func newTestSpecProcessor(ctrl *gomock.Controller) *testSpecProcessor {
	mockMetrics := metrics.NewMockHandler(ctrl)
	mockMetrics.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	mockMetrics.EXPECT().WithTags(gomock.Any()).Return(mockMetrics).AnyTimes()
	mockMetrics.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	return &testSpecProcessor{
		SpecProcessor: scheduler2.SpecProcessor{
			Config: &scheduler2.Config{
				Tweakables: func(_ string) scheduler2.Tweakables {
					return scheduler2.DefaultTweakables
				},
			},
			MetricsHandler: mockMetrics,
			Logger:         log.NewTestLogger(),
			SpecBuilder:    scheduler1.NewSpecBuilder(),
		},
		mockMetrics: mockMetrics,
	}
}

func setupSpecProcessor(t *testing.T) *testSpecProcessor {
	ctrl := gomock.NewController(t)
	return newTestSpecProcessor(ctrl)
}

func TestProcessTimeRange_LimitedActions(t *testing.T) {
	processor := setupSpecProcessor(t)
	scheduler := *scheduler2.NewScheduler(namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval)

	// A schedule with an action limit and remaining actions should buffer actions.
	scheduler.Schedule.State.LimitedActions = true
	scheduler.Schedule.State.RemainingActions = 1

	res, err := processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.BufferedStarts))

	// When a schedule has an action limit that has been exceeded, we don't bother
	// buffering additional actions.
	scheduler.Schedule.State.RemainingActions = 0

	res, err = processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.BufferedStarts))

	// Manual starts should always be allowed.
	res, err = processor.ProcessTimeRange(scheduler, start, end, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.BufferedStarts))
	require.True(t, res.BufferedStarts[0].Manual)
}

func TestProcessTimeRange_UpdateAfterHighWatermark(t *testing.T) {
	processor := setupSpecProcessor(t)
	scheduler := *scheduler2.NewScheduler(namespace, namespaceID, scheduleID, defaultSchedule(), nil)

	// Below window would give 6 actions, but the update time halves that.
	base := time.Now()
	start := base.Add(-defaultInterval * 3)
	end := base.Add(defaultInterval * 3)

	// Actions taking place in time before the last update time should be dropped.
	scheduler.Info.UpdateTime = timestamppb.Now()

	res, err := processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(res.BufferedStarts))
}

func TestProcessTimeRange_CatchupWindow(t *testing.T) {
	processor := setupSpecProcessor(t)
	scheduler := *scheduler2.NewScheduler(namespace, namespaceID, scheduleID, defaultSchedule(), nil)

	// When an action would fall outside of the schedule's catchup window, it should
	// be dropped.
	end := time.Now()
	start := end.Add(-defaultCatchupWindow * 2)

	res, err := processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(res.BufferedStarts))
}

func TestProcessTimeRange_Limit(t *testing.T) {
	processor := setupSpecProcessor(t)
	scheduler := *scheduler2.NewScheduler(namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval * 5)

	// When a limit pointer is provided, its value should be decremented with each
	// action buffered, ProcessTimeRange should return once the limit has been
	// exhausted.
	limit := 2

	res, err := processor.ProcessTimeRange(scheduler, start, end, false, &limit)
	require.NoError(t, err)
	require.Equal(t, 2, len(res.BufferedStarts))
	require.Equal(t, 0, limit)
}

func TestProcessTimeRange_OverlapPolicy(t *testing.T) {
	processor := setupSpecProcessor(t)
	scheduler := *scheduler2.NewScheduler(namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval * 5)

	// Check that a default overlap policy (SKIP) is applied, even when left unspecified.
	scheduler.Schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED

	res, err := processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(res.BufferedStarts))
	for _, b := range res.BufferedStarts {
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, b.OverlapPolicy)
	}

	// Check that a specified overlap policy is applied.
	overlapPolicy := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	scheduler.Schedule.Policies.OverlapPolicy = overlapPolicy

	res, err = processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(res.BufferedStarts))
	for _, b := range res.BufferedStarts {
		require.Equal(t, overlapPolicy, b.OverlapPolicy)
	}
}

func TestProcessTimeRange_Basic(t *testing.T) {
	processor := setupSpecProcessor(t)
	scheduler := *scheduler2.NewScheduler(namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	end := time.Now()
	start := end.Add(-defaultInterval * 5)

	// Validate returned BufferedStarts for unique action times and request IDs.
	res, err := processor.ProcessTimeRange(scheduler, start, end, false, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(res.BufferedStarts))

	uniqueTimes := make(map[time.Time]bool)
	uniqueIDs := make(map[string]bool)
	for _, b := range res.BufferedStarts {
		require.False(t, b.Manual)

		actualTime := b.ActualTime.AsTime()
		require.False(t, uniqueTimes[actualTime])
		require.False(t, uniqueIDs[b.RequestId])
		uniqueTimes[actualTime] = true
		uniqueIDs[b.RequestId] = true
	}

	// Validate next wakeup time.
	require.GreaterOrEqual(t, res.NextWakeupTime, end)
	require.Less(t, res.NextWakeupTime, end.Add(defaultInterval*2))
}
