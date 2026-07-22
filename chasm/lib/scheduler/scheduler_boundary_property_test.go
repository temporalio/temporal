package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
)

func TestSchedulerGeneratorDeadlineBoundaryProperty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		delta  time.Duration
		starts int
	}{
		{name: "before", delta: -time.Nanosecond, starts: 0},
		{name: "exact", starts: 1},
		{name: "after", delta: time.Nanosecond, starts: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newSchedulerPropertyEnv(t, false)
			env.drain(t, schedulerConformanceDrainLimit)
			env.timeSource.Update(schedulerPropertyStartTime.Add(defaultInterval).Add(test.delta))
			env.drain(t, schedulerConformanceDrainLimit)
			require.Len(t, env.services.StartCalls(), test.starts)
		})
	}
}

func TestSchedulerBoundedBackfillProperty(t *testing.T) {
	t.Parallel()
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	env.backfill(t, schedulerPropertyStartTime.Add(-3*defaultInterval), schedulerPropertyStartTime, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	env.drain(t, schedulerConformanceDrainLimit)
	require.Len(t, env.services.StartCalls(), 4)
	require.Equal(t, int64(4), env.describe(t).GetInfo().GetActionCount())
}

func TestSchedulerDeleteInvalidatesOutstandingTasksProperty(t *testing.T) {
	t.Parallel()
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	queued, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	var retainedTask tasks.Task
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility || len(categoryTasks) == 0 {
			continue
		}
		retainedTask = categoryTasks[0]
		break
	}
	require.NotNil(t, retainedTask)
	env.delete(t)
	_, err = env.handler.DescribeSchedule(env.engineCtx, &schedulerpb.DescribeScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: scheduleID}})
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	env.timeSource.Update(retainedTask.GetVisibilityTime())
	result, err := env.engine.ExecuteTask(t.Context(), env.ref, retainedTask)
	require.NoError(t, err)
	require.True(t, result.Dropped)
	require.Empty(t, env.services.StartCalls())
}
