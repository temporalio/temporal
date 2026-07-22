package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

func TestSchedulerInitialPatchPauseStateAudit(t *testing.T) {
	tests := []struct {
		name            string
		initiallyPaused bool
		patch           *schedulepb.SchedulePatch
		wantPaused      bool
		wantNote        string
	}{
		{name: "pause active schedule", patch: &schedulepb.SchedulePatch{Pause: "maintenance"}, wantPaused: true, wantNote: "maintenance"},
		{name: "unpause paused schedule", initiallyPaused: true, patch: &schedulepb.SchedulePatch{Unpause: "resume"}, wantNote: "resume"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newSchedulerPropertyEnvWithPolicyAndInitialPatch(t, test.initiallyPaused, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, test.patch)
			description := env.describe(t)
			require.Equal(t, test.wantPaused, description.GetSchedule().GetState().GetPaused())
			require.Equal(t, test.wantNote, description.GetSchedule().GetState().GetNotes())
		})
	}
}

func TestSchedulerCapacityDeferredBackfillRetainsStartAudit(t *testing.T) {
	modelConfig := model.Config{StartTime: schedulerPropertyStartTime, Interval: defaultInterval, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	created, err := model.Transition(modelConfig, model.State{}, model.Create{})
	require.NoError(t, err)
	start := schedulerPropertyStartTime.Add(-3 * defaultInterval)
	want, err := model.Transition(modelConfig, created.State, model.Backfill{ID: "history", Start: start, End: schedulerPropertyStartTime})
	require.NoError(t, err)
	require.Len(t, modelStartEffects(want.Effects), 4)

	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		invoker := component.(*scheduler.Scheduler).Invoker.Get(ctx)
		for range 2 * schedulerPropertyMaxBufferSize {
			invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
		}
		return nil
	})
	require.NoError(t, err)

	env.backfill(t, start, schedulerPropertyStartTime, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	require.Empty(t, env.services.StartCalls())

	_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		component.(*scheduler.Scheduler).Invoker.Get(ctx).BufferedStarts = nil
		return nil
	})
	require.NoError(t, err)
	env.timeSource.Update(nextSchedulerTaskTime(t, env))
	_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	require.Len(t, env.services.StartCalls(), 4)
}

func TestSchedulerConcurrentBackfillsMakeProgressAudit(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	start := schedulerPropertyStartTime.Add(-defaultInterval)
	requests := make([]*schedulepb.BackfillRequest, 0, 10)
	for range 10 {
		requests = append(requests, &schedulepb.BackfillRequest{StartTime: timestamppb.New(start), EndTime: timestamppb.New(schedulerPropertyStartTime)})
	}
	_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.PatchScheduleRequest{
		Namespace: namespace, ScheduleId: scheduleID, Patch: &schedulepb.SchedulePatch{BackfillRequest: requests},
	}})
	require.NoError(t, err)
	_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	require.NotEmpty(t, env.services.StartCalls())
}

func TestSchedulerGeneratedBackfillModelConformance(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		count := rapid.IntRange(1, 4).Draw(t, "backfill intervals")
		deferForCapacity := rapid.Bool().Draw(t, "defer for capacity")
		start := schedulerPropertyStartTime.Add(-time.Duration(count) * defaultInterval)
		config := model.Config{StartTime: schedulerPropertyStartTime, Interval: defaultInterval, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
		created, err := model.Transition(config, model.State{}, model.Create{})
		require.NoError(t, err)
		want, err := model.Transition(config, created.State, model.Backfill{ID: "generated", Start: start, End: schedulerPropertyStartTime})
		require.NoError(t, err)

		env := newSchedulerPropertyEnv(t, false)
		env.drain(t, schedulerConformanceDrainLimit)
		if deferForCapacity {
			_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
				invoker := component.(*scheduler.Scheduler).Invoker.Get(ctx)
				for range 2 * schedulerPropertyMaxBufferSize {
					invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
				}
				return nil
			})
			require.NoError(t, err)
		}
		env.backfill(t, start, schedulerPropertyStartTime, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
		_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
		require.NoError(t, err)
		if deferForCapacity {
			_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
				component.(*scheduler.Scheduler).Invoker.Get(ctx).BufferedStarts = nil
				return nil
			})
			require.NoError(t, err)
			env.timeSource.Update(nextSchedulerTaskTime(t, env))
			_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
			require.NoError(t, err)
		}
		require.Len(t, env.services.StartCalls(), len(modelStartEffects(want.Effects)))
	})
}

func nextSchedulerTaskTime(t schedulerPropertyTestingT, env *schedulerPropertyEnv) time.Time {
	t.Helper()
	queued, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	now := env.timeSource.Now()
	var next time.Time
	for _, categoryTasks := range queued {
		for _, task := range categoryTasks {
			visibility := task.GetVisibilityTime()
			if visibility.After(now) && (next.IsZero() || visibility.Before(next)) {
				next = visibility
			}
		}
	}
	require.False(t, next.IsZero())
	return next
}

func TestSchedulerPauseOnFailureInvalidatesConflictTokenAudit(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	_, err := env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		component.(*scheduler.Scheduler).Schedule.Policies.PauseOnFailure = true
		return nil
	})
	require.NoError(t, err)
	env.drain(t, schedulerConformanceDrainLimit)
	env.trigger(t)
	env.drain(t, schedulerConformanceDrainLimit)
	requestID := env.services.StartCalls()[0].Request.GetRequestId()
	before := env.describe(t)

	_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		return component.(*scheduler.Scheduler).HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: requestID,
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{Failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{},
			}},
			CloseTime: timestamppb.New(env.timeSource.Now()),
		})
	})
	require.NoError(t, err)

	_, err = env.handler.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.UpdateScheduleRequest{
		Namespace: namespace, ScheduleId: scheduleID, ConflictToken: before.GetConflictToken(), Schedule: proto.CloneOf(before.GetSchedule()), Memo: &commonpb.Memo{},
	}})
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
}
