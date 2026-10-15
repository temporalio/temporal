package tests

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestScheduleVisibilityLifecycleV2(t *testing.T) {
	runScheduleVisibilityLifecycleTests(t, chasmscheduler.DefaultTweakables)
}

func runScheduleVisibilityLifecycleTests(t *testing.T, tweakables chasmscheduler.Tweakables) {
	t.Run("create pause update resume delete", func(t *testing.T) {
		testScheduleVisibilityMutations(t, tweakables)
	})
	t.Run("automatic actions", func(t *testing.T) {
		testScheduleVisibilityActions(t, tweakables)
	})
	t.Run("manual trigger while paused", func(t *testing.T) {
		testScheduleVisibilityManualTrigger(t, tweakables)
	})
	t.Run("running and buffered actions", func(t *testing.T) {
		testScheduleVisibilityBuffer(t, tweakables)
	})
	t.Run("idle close", func(t *testing.T) {
		testScheduleVisibilityIdleClose(t, tweakables)
	})
}

func scheduleVisibilityEnv(t *testing.T, tweakables chasmscheduler.Tweakables) *testcore.TestEnv {
	opts := append(scheduleCommonOpts(t), testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweakables))
	return newScheduleEnv(t, opts...)
}

func scheduleVisibleWithQuery(env *testcore.TestEnv, scheduleID, query string) bool {
	visible, err := scheduleVisibilityQueryContains(env, scheduleID, query)
	return err == nil && visible
}

func scheduleMissingWithQuery(env *testcore.TestEnv, scheduleID, query string) bool {
	visible, err := scheduleVisibilityQueryContains(env, scheduleID, query)
	return err == nil && !visible
}

func scheduleVisibilityQueryContains(env *testcore.TestEnv, scheduleID, query string) (bool, error) {
	response, err := env.FrontendClient().ListSchedules(chasmContextFactory(testcore.NewContext()), &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 10,
		Query:           query,
	})
	if err != nil {
		return false, err
	}
	for _, entry := range response.GetSchedules() {
		if entry.GetScheduleId() == scheduleID {
			return true, nil
		}
	}
	return false, nil
}

func testScheduleVisibilityMutations(t *testing.T, tweakables chasmscheduler.Tweakables) {
	env := scheduleVisibilityEnv(t, tweakables)
	ctx := chasmContextFactory(testcontext.For(t))
	scheduleID := testcore.RandomizeStr("visibility-mutations")
	createdAt := time.Now().UTC()
	schedule := &schedulepb.Schedule{
		Spec:   intervalSpec(time.Hour),
		Action: startWorkflowAction(env, testcore.RandomizeStr("visibility-action"), "visibility-action-type"),
		State:  &schedulepb.ScheduleState{Paused: true, Notes: "created"},
	}
	createSchedule(ctx, t, env, scheduleID, schedule)

	entry := getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		return entry.GetInfo().GetPaused() && entry.GetInfo().GetNotes() == "created" &&
			len(entry.GetInfo().GetFutureActionTimes()) > 0
	})
	require.True(t, entry.GetInfo().GetFutureActionTimes()[0].AsTime().After(createdAt))
	require.True(t, scheduleVisibleWithQuery(env, scheduleID, fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused)))
	require.True(t, scheduleVisibleWithQuery(env, scheduleID,
		fmt.Sprintf(`%s > "%s"`, chasmscheduler.ScheduleNextActionTimeName, createdAt.Format(time.RFC3339Nano))))
	require.True(t, scheduleVisibleWithQuery(env, scheduleID,
		fmt.Sprintf("%s = 0 AND %s = 0", chasmscheduler.ScheduleRunningWorkflowCountName,
			chasmscheduler.ScheduleBufferedStartsCountName)))

	patchSchedule(ctx, t, env, scheduleID, &schedulepb.SchedulePatch{Unpause: "resume"})
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		return !entry.GetInfo().GetPaused()
	})
	require.True(t, scheduleVisibleWithQuery(env, scheduleID, fmt.Sprintf("%s = false", sadefs.TemporalSchedulePaused)))
	require.True(t, scheduleMissingWithQuery(env, scheduleID, fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused)))

	schedule.Spec.Interval[0].Interval = durationpb.New(2 * time.Hour)
	schedule.State = &schedulepb.ScheduleState{Notes: "updated"}
	_, err := env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: scheduleID,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("update"),
	})
	require.NoError(t, err)
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		return entry.GetInfo().GetNotes() == "updated" &&
			entry.GetInfo().GetSpec().GetInterval()[0].GetInterval().AsDuration() == 2*time.Hour
	})

	memoValue := payload.EncodeString("visible memo")
	searchValue := payload.EncodeString("visible attribute")
	_, err = env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: scheduleID,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("update-visibility"),
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			"visibilityMemo": memoValue,
		}},
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": searchValue,
		}},
	})
	require.NoError(t, err)
	entry = getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		return entry.GetMemo().GetFields()["visibilityMemo"] != nil &&
			entry.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"] != nil
	})
	require.Equal(t, memoValue.GetData(), entry.GetMemo().GetFields()["visibilityMemo"].GetData())
	require.Equal(t, searchValue.GetData(), entry.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"].GetData())
	require.True(t, scheduleVisibleWithQuery(env, scheduleID, `CustomKeywordField = "visible attribute"`))

	patchSchedule(ctx, t, env, scheduleID, &schedulepb.SchedulePatch{Pause: "hold"})
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		return entry.GetInfo().GetPaused()
	})
	require.True(t, scheduleVisibleWithQuery(env, scheduleID, fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused)))
	require.True(t, scheduleMissingWithQuery(env, scheduleID, fmt.Sprintf("%s = false", sadefs.TemporalSchedulePaused)))

	_, err = env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: scheduleID,
		Identity:   "test",
	})
	require.NoError(t, err)
	await.RequireTruef(t, func() bool {
		return scheduleMissingWithQuery(env, scheduleID, "")
	}, awaitTimeout, pollInterval, "deleted schedule should leave visibility")
}

func testScheduleVisibilityActions(t *testing.T, tweakables chasmscheduler.Tweakables) {
	env := scheduleVisibilityEnv(t, tweakables)
	ctx := chasmContextFactory(testcontext.For(t))
	scheduleID := testcore.RandomizeStr("visibility-actions")
	workflowType := testcore.RandomizeStr("visibility-workflow")
	var runs atomic.Int32
	registerCountingWorkflow(env, workflowType, &runs)
	createdAt := time.Now().UTC()
	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, testcore.RandomizeStr("visibility-action"), workflowType),
		State:  &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 2},
	})

	await.RequireTruef(t, func() bool {
		response, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
		})
		return err == nil && runs.Load() == 2 && response.GetSchedule().GetState().GetRemainingActions() == 0
	}, awaitTimeout, pollInterval, "limited schedule should complete both actions")
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		info := entry.GetInfo()
		if len(info.GetRecentActions()) != 2 || len(info.GetFutureActionTimes()) != 0 {
			return false
		}
		for _, action := range info.GetRecentActions() {
			if action.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return false
			}
		}
		return true
	})
	require.True(t, scheduleVisibleWithQuery(env, scheduleID,
		fmt.Sprintf("%s = 0 AND %s = 0", chasmscheduler.ScheduleRunningWorkflowCountName,
			chasmscheduler.ScheduleBufferedStartsCountName)))
	require.True(t, scheduleMissingWithQuery(env, scheduleID,
		fmt.Sprintf(`%s > "%s"`, chasmscheduler.ScheduleNextActionTimeName, createdAt.Format(time.RFC3339Nano))))
}

func testScheduleVisibilityManualTrigger(t *testing.T, tweakables chasmscheduler.Tweakables) {
	env := scheduleVisibilityEnv(t, tweakables)
	ctx := chasmContextFactory(testcontext.For(t))
	scheduleID := testcore.RandomizeStr("visibility-manual")
	workflowType := testcore.RandomizeStr("visibility-manual-workflow")
	var runs atomic.Int32
	registerCountingWorkflow(env, workflowType, &runs)
	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec:   intervalSpec(time.Hour),
		Action: startWorkflowAction(env, testcore.RandomizeStr("visibility-manual-action"), workflowType),
		State:  &schedulepb.ScheduleState{Paused: true},
	})
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		return entry.GetInfo().GetPaused()
	})
	patchSchedule(ctx, t, env, scheduleID, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	await.RequireTruef(t, func() bool { return runs.Load() == 1 }, awaitTimeout, pollInterval,
		"manual trigger should start one action")
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		info := entry.GetInfo()
		return info.GetPaused() && len(info.GetRecentActions()) == 1
	})
	require.True(t, scheduleVisibleWithQuery(env, scheduleID, fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused)))
}

func testScheduleVisibilityBuffer(t *testing.T, tweakables chasmscheduler.Tweakables) {
	env := scheduleVisibilityEnv(t, tweakables)
	ctx := chasmContextFactory(testcontext.For(t))
	scheduleID := testcore.RandomizeStr("visibility-buffer")
	workflowType := testcore.RandomizeStr("visibility-buffer-workflow")
	var runs atomic.Int32
	registerGatedWorkflow(env, workflowType, &runs)
	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, testcore.RandomizeStr("visibility-buffer-action"), workflowType),
		State:  &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 2},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	})

	await.RequireTruef(t, func() bool {
		response, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
		})
		return err == nil && runs.Load() == 1 && len(response.GetInfo().GetRunningWorkflows()) == 1 &&
			response.GetInfo().GetBufferSize() == 1
	}, awaitTimeout, pollInterval, "one action should run with one buffered")
	await.RequireTruef(t, func() bool {
		return scheduleVisibleWithQuery(env, scheduleID,
			fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleRunningWorkflowCountName)) &&
			scheduleVisibleWithQuery(env, scheduleID,
				fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleBufferedStartsCountName))
	}, awaitTimeout, pollInterval, "running and buffered counts should reach visibility")
	require.Equal(t, 1, completeRunningWorkflows(ctx, t, env, scheduleID))
	await.RequireTruef(t, func() bool { return runs.Load() == 2 }, awaitTimeout, pollInterval,
		"buffered action should start after the first completes")
	require.Equal(t, 1, completeRunningWorkflows(ctx, t, env, scheduleID))
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, func(entry *schedulepb.ScheduleListEntry) bool {
		info := entry.GetInfo()
		if len(info.GetRecentActions()) != 2 {
			return false
		}
		for _, action := range info.GetRecentActions() {
			if action.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return false
			}
		}
		return true
	})
	await.RequireTruef(t, func() bool {
		return scheduleVisibleWithQuery(env, scheduleID,
			fmt.Sprintf("%s = 0 AND %s = 0", chasmscheduler.ScheduleRunningWorkflowCountName,
				chasmscheduler.ScheduleBufferedStartsCountName))
	}, awaitTimeout, pollInterval, "running and buffered counts should return to zero")
}

func testScheduleVisibilityIdleClose(t *testing.T, tweakables chasmscheduler.Tweakables) {
	tweakables.IdleTime = 6 * time.Second
	env := scheduleVisibilityEnv(t, tweakables)
	ctx := chasmContextFactory(testcontext.For(t))
	scheduleID := testcore.RandomizeStr("visibility-idle")
	createdAt := time.Now().UTC()
	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{},
		Action: startWorkflowAction(env, testcore.RandomizeStr("visibility-idle-action"), "visibility-idle-workflow"),
	})
	getScheduleEntryFromVisibility(env, scheduleID, chasmContextFactory, nil)
	await.RequireTruef(t, func() bool {
		return scheduleVisibleWithQuery(env, scheduleID,
			fmt.Sprintf(`%s > "%s"`, chasmscheduler.ScheduleIdleCloseTimeName, createdAt.Format(time.RFC3339Nano)))
	}, awaitTimeout, pollInterval, "idle close deadline should be queryable")
	await.RequireTruef(t, func() bool { return scheduleClosed(ctx, env, scheduleID) }, awaitTimeout, pollInterval,
		"idle schedule should close")
	await.RequireTruef(t, func() bool {
		return scheduleMissingWithQuery(env, scheduleID, "")
	}, awaitTimeout, pollInterval, "idle-closed schedule should leave visibility")
}
