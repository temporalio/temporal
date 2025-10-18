package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testEnv holds setup for Scheduler component tests.
type testEnv struct {
	t           *testing.T
	controller  *gomock.Controller
	nodeBackend *chasm.MockNodeBackend
	registry    *chasm.Registry
	timeSource  *clock.EventTimeSource
	logger      log.Logger
	node        *chasm.Node
	scheduler   *scheduler.Scheduler
}

// newTestEnv creates a test environment with a Scheduler component.
func newTestEnv(t *testing.T) *testEnv {
	controller := gomock.NewController(t)
	nodeBackend := chasm.NewMockNodeBackend(controller)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	registry := chasm.NewRegistry(logger)
	err := registry.Register(&scheduler.Library{})
	require.NoError(t, err)

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	// Setup MS backend expectations
	tv := testvars.New(t)
	nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	nodeBackend.EXPECT().AddTasks(gomock.Any()).AnyTimes()

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger)
	ctx := chasm.NewMutableContext(context.Background(), node)

	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	node.SetRootComponent(sched)

	return &testEnv{
		t:           t,
		controller:  controller,
		nodeBackend: nodeBackend,
		registry:    registry,
		timeSource:  timeSource,
		logger:      logger,
		node:        node,
		scheduler:   sched,
	}
}

// newContext creates a new CHASM mutable context for the test environment.
func (e *testEnv) newContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), e.node)
}

// closeTransaction closes the CHASM transaction and verifies no error.
func (e *testEnv) closeTransaction() {
	_, err := e.node.CloseTransaction()
	require.NoError(e.t, err)
}

func TestDescribe_Basic(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	req := &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		Request: &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		},
	}

	resp, err := env.scheduler.Describe(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Response)
	require.NotNil(t, resp.Response.Schedule)
	require.NotNil(t, resp.Response.Info)
	require.Equal(t, env.scheduler.Schedule, resp.Response.Schedule)
	require.Equal(t, env.scheduler.Info, resp.Response.Info)
}

func TestDescribe_ReturnsCorrectScheduleFields(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	// Verify the schedule has the expected structure
	require.NotNil(t, env.scheduler.Schedule.Spec)
	require.NotNil(t, env.scheduler.Schedule.Action)
	require.NotNil(t, env.scheduler.Schedule.Policies)
	require.NotNil(t, env.scheduler.Schedule.State)

	req := &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		Request: &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		},
	}

	resp, err := env.scheduler.Describe(ctx, req)

	require.NoError(t, err)
	require.Equal(t, env.scheduler.Schedule.Spec, resp.Response.Schedule.Spec)
	require.Equal(t, env.scheduler.Schedule.Action, resp.Response.Schedule.Action)
	require.Equal(t, env.scheduler.Schedule.Policies, resp.Response.Schedule.Policies)
	require.Equal(t, env.scheduler.Schedule.State, resp.Response.Schedule.State)
}

func TestDescribe_ReturnsCorrectInfo(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	// Verify info fields are initialized
	require.NotNil(t, env.scheduler.Info)
	require.NotNil(t, env.scheduler.Info.CreateTime)
	require.False(t, env.scheduler.Info.CreateTime.AsTime().IsZero())

	req := &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	}

	resp, err := env.scheduler.Describe(ctx, req)

	require.NoError(t, err)
	require.Equal(t, env.scheduler.Info.CreateTime, resp.Info.CreateTime)
	require.Equal(t, env.scheduler.Info.UpdateTime, resp.Info.UpdateTime)
	require.Equal(t, env.scheduler.Info.ActionCount, resp.Info.ActionCount)
}

func TestDescribe_ConflictTokenMatchesSchedulerState(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	initialToken := env.scheduler.ConflictToken

	req := &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	}

	resp, err := env.scheduler.Describe(ctx, req)

	require.NoError(t, err)
	require.Equal(t, initialToken, resp.ConflictToken)
	require.Greater(t, resp.ConflictToken, int64(0), "conflict token should be initialized")
}

func TestDelete_Basic(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	require.False(t, env.scheduler.Closed, "scheduler should start open")

	req := &schedulerpb.DeleteScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	}

	resp, err := env.scheduler.Delete(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, env.scheduler.Closed, "scheduler should be closed after delete")
	require.Equal(t, chasm.LifecycleStateCompleted, env.scheduler.LifecycleState(ctx))
}

func TestCreate_Basic(t *testing.T) {
	controller := gomock.NewController(t)
	nodeBackend := chasm.NewMockNodeBackend(controller)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	registry := chasm.NewRegistry(logger)
	err := registry.Register(&scheduler.Library{})
	require.NoError(t, err)

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	tv := testvars.New(t)
	nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	nodeBackend.EXPECT().AddTasks(gomock.Any()).AnyTimes()

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger)
	ctx := chasm.NewMutableContext(context.Background(), node)

	req := &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
		Schedule:    defaultSchedule(),
	}

	sched, resp, err := scheduler.Create(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, sched)
	require.NotNil(t, resp)
	require.Equal(t, req.Schedule, sched.Schedule)
	require.NotNil(t, sched.Info)
	require.NotNil(t, sched.Info.CreateTime)
	require.False(t, sched.Info.CreateTime.AsTime().IsZero())
	require.Greater(t, resp.ConflictToken, int64(0))
	require.Equal(t, sched.ConflictToken, resp.ConflictToken)
}

func TestUpdate_Basic(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	initialToken := env.scheduler.ConflictToken
	initialUpdateTime := env.scheduler.Info.UpdateTime.AsTime()

	newSchedule := defaultSchedule()
	newSchedule.State.Paused = true

	req := &schedulerpb.UpdateScheduleRequest{
		NamespaceId:   namespaceID,
		ScheduleId:    scheduleID,
		Schedule:      newSchedule,
		ConflictToken: initialToken,
	}

	resp, err := env.scheduler.Update(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, newSchedule, env.scheduler.Schedule)
	require.True(t, env.scheduler.Schedule.State.Paused)
	require.True(t, env.scheduler.Info.UpdateTime.AsTime().After(initialUpdateTime))
	require.Greater(t, resp.ConflictToken, initialToken)
	require.Equal(t, env.scheduler.ConflictToken, resp.ConflictToken)
}

func TestUpdate_ConflictTokenMismatch(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	req := &schedulerpb.UpdateScheduleRequest{
		NamespaceId:   namespaceID,
		ScheduleId:    scheduleID,
		Schedule:      defaultSchedule(),
		ConflictToken: 999, // Wrong token
	}

	_, err := env.scheduler.Update(ctx, req)
	require.ErrorIs(t, err, scheduler.ErrConflictTokenMismatch)
}

func TestPatch_Pause(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	initialToken := env.scheduler.ConflictToken
	initialUpdateTime := env.scheduler.Info.UpdateTime.AsTime()
	require.False(t, env.scheduler.Schedule.State.Paused)

	req := &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
		Patch: &schedulepb.SchedulePatch{
			Pause: "pausing for maintenance",
		},
		ConflictToken: initialToken,
	}

	resp, err := env.scheduler.Patch(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, env.scheduler.Schedule.State.Paused)
	require.Equal(t, "pausing for maintenance", env.scheduler.Schedule.State.Notes)
	require.True(t, env.scheduler.Info.UpdateTime.AsTime().After(initialUpdateTime))
	require.Greater(t, resp.ConflictToken, initialToken)
	require.Equal(t, env.scheduler.ConflictToken, resp.ConflictToken)
}

func TestPatch_Unpause(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	// First pause the schedule
	env.scheduler.Schedule.State.Paused = true
	env.scheduler.Schedule.State.Notes = "was paused"

	initialToken := env.scheduler.ConflictToken
	initialUpdateTime := env.scheduler.Info.UpdateTime.AsTime()

	req := &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
		Patch: &schedulepb.SchedulePatch{
			Unpause: "resuming operations",
		},
		ConflictToken: initialToken,
	}

	resp, err := env.scheduler.Patch(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.False(t, env.scheduler.Schedule.State.Paused)
	require.Equal(t, "resuming operations", env.scheduler.Schedule.State.Notes)
	require.True(t, env.scheduler.Info.UpdateTime.AsTime().After(initialUpdateTime))
	require.Greater(t, resp.ConflictToken, initialToken)
	require.Equal(t, env.scheduler.ConflictToken, resp.ConflictToken)
}

func TestPatch_CreatesBackfillers(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	initialToken := env.scheduler.ConflictToken
	require.Empty(t, env.scheduler.Backfillers)

	req := &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
		Patch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
			BackfillRequest: []*schedulepb.BackfillRequest{
				{
					StartTime: timestamppb.New(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
					EndTime:   timestamppb.New(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)),
				},
			},
		},
		ConflictToken: initialToken,
	}

	resp, err := env.scheduler.Patch(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, env.scheduler.Backfillers, 2)

	foundTrigger := false
	foundBackfill := false
	for _, field := range env.scheduler.Backfillers {
		backfiller, err := field.Get(ctx)
		require.NoError(t, err)
		if backfiller.GetTriggerRequest() != nil {
			foundTrigger = true
		}
		if backfiller.GetBackfillRequest() != nil {
			foundBackfill = true
		}
	}
	require.True(t, foundTrigger)
	require.True(t, foundBackfill)
}

func TestPatch_ConflictTokenMismatch(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.newContext()

	req := &schedulerpb.PatchScheduleRequest{
		NamespaceId:   namespaceID,
		ScheduleId:    scheduleID,
		Patch:         &schedulepb.SchedulePatch{},
		ConflictToken: 999, // Wrong token
	}

	_, err := env.scheduler.Patch(ctx, req)
	require.ErrorIs(t, err, scheduler.ErrConflictTokenMismatch)
}
