package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"pgregory.net/rapid"
)

func TestSchedulerMigrationFailureReloadAndRetryProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	schedulerRPCProfiles{}.migrationRetryable().Expect(&env.services.Migrate, migrateWorkflowMethod, "first migration", nil)
	_, err := env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{NamespaceId: namespaceID, ScheduleId: scheduleID})
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	migrationTask := runnable[0]

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.Error(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	require.True(t, env.describe(t).GetSchedule().GetState().GetPaused())
	env.reload(t)

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.NoError(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	env.assertScripts(t)

	result, err := env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.NoError(t, err)
	require.True(t, result.Dropped)
}

func TestSchedulerCallbackRecoveryUsesGeneratedClientsProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		env := newSchedulerPropertyEnv(t, false)
		profiles := schedulerRPCProfiles{}
		behavior := rpcgen.Draw(t, "DescribeWorkflowExecution callback behavior", profiles.describeRunning(), profiles.describeCompleted())
		behavior.Expect(&env.services.Describe, describeWorkflowMethod, "callback recovery", nil)
		env.drain(t, schedulerConformanceDrainLimit)
		env.trigger(t)
		env.drain(t, schedulerConformanceDrainLimit)
		initialCall := env.services.StartCalls()[0]

		_, err := env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
			schedule := component.(*scheduler.Scheduler)
			start := schedule.Invoker.Get(ctx).GetBufferedStarts()[0]
			start.HasCallback = false
			ctx.AddTask(schedule, chasm.TaskAttributes{}, &schedulerpb.SchedulerCallbacksTask{})
			return nil
		})
		require.NoError(t, err)
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Len(t, runnable, 1)
		callbackTask := runnable[0]
		_, err = env.engine.ExecuteTask(t.Context(), env.ref, callbackTask)
		require.NoError(t, err)

		require.Len(t, env.services.Describe.Calls(), 1)
		wantStartCalls := 1
		if behavior.Label == "running" {
			wantStartCalls = 2
			startCalls := env.services.StartCalls()
			require.Equal(t, initialCall.Request.GetRequestId(), startCalls[1].Request.GetRequestId())
			require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, startCalls[1].Request.GetWorkflowIdConflictPolicy())
		}
		require.Len(t, env.services.StartCalls(), wantStartCalls)
		env.reload(t)
		result, err := env.engine.ExecuteTask(t.Context(), env.ref, callbackTask)
		require.NoError(t, err)
		require.True(t, result.Dropped)
		require.Len(t, env.services.Describe.Calls(), 1)
		require.Len(t, env.services.StartCalls(), wantStartCalls)
		env.assertScripts(t)
	})
}

func TestSchedulerMigrationTerminalFailureRestoresScheduleProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	schedulerRPCProfiles{}.migrationTerminal().Expect(&env.services.Migrate, migrateWorkflowMethod, "terminal migration", nil)
	_, err := env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{NamespaceId: namespaceID, ScheduleId: scheduleID})
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, runnable[0])
	require.NoError(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	require.False(t, env.describe(t).GetSchedule().GetState().GetPaused())
	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Empty(t, runnable)
	env.assertScripts(t)
}
