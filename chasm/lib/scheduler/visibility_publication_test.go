package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
)

func TestVisibilityPublicationAcrossTransactions(t *testing.T) {
	engine := newSchedulerTestEngine(t, defaultSchedule(), withEngineVisibilityCoalesceInterval(30*time.Second))
	initialTasks, err := engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	initialVisibilityTaskCount := len(initialTasks[tasks.CategoryVisibility])
	require.Positive(t, initialVisibilityTaskCount)
	var initial *schedulepb.ScheduleListInfo
	require.NoError(t, engine.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		initial = s.Memo(ctx).(*schedulepb.ScheduleListInfo)
		require.NotNil(t, s.VisibilityPublication)
		return nil
	}))

	require.NoError(t, engine.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).BufferedStarts = append(s.Invoker.Get(ctx).BufferedStarts,
			&schedulespb.BufferedStart{RequestId: "first"})
		return nil
	}))
	deferredTasks, err := engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	require.Len(t, deferredTasks[tasks.CategoryVisibility], initialVisibilityTaskCount)
	var deadline time.Time
	require.NoError(t, engine.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Equal(t, initial, s.Memo(ctx))
		require.Equal(t, int64(0), s.VisibilityPublication.BufferedStartsCount)
		require.NotNil(t, s.VisibilityPublication.RefreshDeadline)
		deadline = s.VisibilityPublication.RefreshDeadline.AsTime()
		return nil
	}))
	require.NoError(t, engine.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).BufferedStarts = append(s.Invoker.Get(ctx).BufferedStarts,
			&schedulespb.BufferedStart{RequestId: "second"})
		return nil
	}))
	deferredTasks, err = engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	require.Len(t, deferredTasks[tasks.CategoryVisibility], initialVisibilityTaskCount)
	require.NoError(t, engine.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Equal(t, deadline, s.VisibilityPublication.RefreshDeadline.AsTime())
		return nil
	}))

	handler := &scheduler.SchedulerVisibilityRefreshTaskHandler{}
	require.NoError(t, engine.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		task := &schedulerpb.SchedulerVisibilityRefreshTask{Generation: s.VisibilityPublication.RefreshGeneration}
		return handler.Execute(ctx, s, chasm.TaskAttributes{ScheduledTime: deadline}, task)
	}))
	refreshedTasks, err := engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	require.Len(t, refreshedTasks[tasks.CategoryVisibility], initialVisibilityTaskCount+1)
	require.NoError(t, engine.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Equal(t, int64(2), s.VisibilityPublication.BufferedStartsCount)
		require.Nil(t, s.VisibilityPublication.RefreshDeadline)
		return nil
	}))
}

func TestVisibilityPublicationFlagDisableAcrossTransactions(t *testing.T) {
	enabled := true
	engine := newSchedulerTestEngine(t, defaultSchedule(),
		withEngineVisibilityCoalescingEnabledFn(func() bool { return enabled }))
	initialTasks, err := engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	initialCount := len(initialTasks[tasks.CategoryVisibility])
	require.NoError(t, engine.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).BufferedStarts = append(s.Invoker.Get(ctx).BufferedStarts,
			&schedulespb.BufferedStart{RequestId: "pending"})
		return nil
	}))
	deferredTasks, err := engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	require.Len(t, deferredTasks[tasks.CategoryVisibility], initialCount)

	enabled = false
	require.NoError(t, engine.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).BufferedStarts = append(s.Invoker.Get(ctx).BufferedStarts,
			&schedulespb.BufferedStart{RequestId: "after-disable"})
		return nil
	}))
	publishedTasks, err := engine.engine.Tasks(engine.rootRef)
	require.NoError(t, err)
	require.Len(t, publishedTasks[tasks.CategoryVisibility], initialCount+1)
	require.NoError(t, engine.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Nil(t, s.VisibilityPublication)
		require.Equal(t, s.ListInfo(ctx), s.Memo(ctx))
		return nil
	}))
}

func TestVisibilityPublicationCoalescesRoutineChanges(t *testing.T) {
	env := newTestEnv(t, withVisibilityCoalesceInterval(30*time.Second))
	ctx := env.MutableContext()
	s := env.Scheduler
	handler := &scheduler.SchedulerVisibilityRefreshTaskHandler{}

	require.NotNil(t, s.VisibilityPublication)
	initial := s.VisibilityPublication
	invoker := s.Invoker.Get(ctx)
	invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{RequestId: "first"})
	changed, err := s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	deadline := s.VisibilityPublication.RefreshDeadline.AsTime()
	require.True(t, env.TimeSource.Now().Add(30*time.Second).Equal(deadline))
	require.Equal(t, int64(0), s.VisibilityPublication.BufferedStartsCount)
	require.Same(t, initial, s.VisibilityPublication)

	invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{RequestId: "second"})
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, deadline, s.VisibilityPublication.RefreshDeadline.AsTime())

	task := &schedulerpb.SchedulerVisibilityRefreshTask{Generation: s.VisibilityPublication.RefreshGeneration}
	valid, err := handler.Validate(ctx, s, chasm.TaskInvocation{
		TaskAttributes: chasm.TaskAttributes{ScheduledTime: deadline},
	}, task)
	require.NoError(t, err)
	require.True(t, valid)
	require.NoError(t, handler.Execute(ctx, s, chasm.TaskAttributes{ScheduledTime: deadline}, task))
	valid, err = handler.Validate(ctx, s, chasm.TaskInvocation{
		TaskAttributes: chasm.TaskAttributes{ScheduledTime: deadline},
	}, task)
	require.NoError(t, err)
	require.False(t, valid)
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int64(2), s.VisibilityPublication.BufferedStartsCount)
	require.Nil(t, s.VisibilityPublication.RefreshDeadline)
	valid, err = handler.Validate(ctx, s, chasm.TaskInvocation{
		TaskAttributes: chasm.TaskAttributes{ScheduledTime: deadline},
	}, task)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestVisibilityPublicationFlushesPauseAndClose(t *testing.T) {
	env := newTestEnv(t, withVisibilityCoalesceInterval(30*time.Second))
	ctx := env.MutableContext()
	s := env.Scheduler

	s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "pending"}}
	changed, err := s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotNil(t, s.VisibilityPublication.RefreshDeadline)

	s.Schedule.State.Paused = true
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, s.VisibilityPublication.Paused)
	require.Equal(t, int64(1), s.VisibilityPublication.BufferedStartsCount)
	require.Nil(t, s.VisibilityPublication.RefreshDeadline)

	s.Closed = true
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "Completed", s.VisibilityPublication.ExecutionStatus)
}

func TestVisibilityPublicationFlushesPatch(t *testing.T) {
	env := newTestEnv(t, withVisibilityCoalesceInterval(30*time.Second))
	ctx := env.MutableContext()
	s := env.Scheduler
	s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "pending"}}
	changed, err := s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotNil(t, s.VisibilityPublication.RefreshDeadline)

	_, err = s.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{},
		},
	})
	require.NoError(t, err)
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int64(1), s.VisibilityPublication.BufferedStartsCount)
	require.Nil(t, s.VisibilityPublication.RefreshDeadline)
}

func TestVisibilityPublicationFlushesWhenDisabled(t *testing.T) {
	enabled := true
	env := newTestEnv(t, withVisibilityCoalescingEnabledFn(func() bool { return enabled }))
	ctx := env.MutableContext()
	s := env.Scheduler
	s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "first"}}
	changed, err := s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotNil(t, s.VisibilityPublication.RefreshDeadline)
	deadline := s.VisibilityPublication.RefreshDeadline.AsTime()
	generation := s.VisibilityPublication.RefreshGeneration

	enabled = false
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	require.Nil(t, s.VisibilityPublication)
	require.Equal(t, s.ListInfo(ctx), s.Memo(ctx))
	valid, err := (&scheduler.SchedulerVisibilityRefreshTaskHandler{}).Validate(ctx, s, chasm.TaskInvocation{
		TaskAttributes: chasm.TaskAttributes{ScheduledTime: deadline},
	}, &schedulerpb.SchedulerVisibilityRefreshTask{Generation: generation})
	require.NoError(t, err)
	require.False(t, valid)

	s.Invoker.Get(ctx).BufferedStarts = append(s.Invoker.Get(ctx).BufferedStarts,
		&schedulespb.BufferedStart{RequestId: "second"})
	changed, err = s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, s.VisibilityPublication)
}

func TestVisibilityPublicationDefaultDisabled(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	s := env.Scheduler
	require.Positive(t, scheduler.DefaultTweakables.VisibilityCoalesceInterval)
	s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "pending"}}
	changed, err := s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, s.VisibilityPublication)
}

func TestVisibilityPublicationZeroIntervalDisabled(t *testing.T) {
	env := newTestEnv(t, withVisibilityCoalesceIntervalFn(func() time.Duration { return 0 }))
	ctx := env.MutableContext()
	s := env.Scheduler
	s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "pending"}}
	changed, err := s.PrepareVisibility(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, s.VisibilityPublication)
}
