package tests

import (
	"compress/gzip"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestGenerateSchedulerVersionCeilingReplayHistory records a V1 scheduler history at the
// checked-out revision. Run this test at both the producer and compatibility-base revisions, then
// put both outputs in service/worker/scheduler/testdata; TestReplays will exercise both histories.
//
// Example:
//
//	SCHEDULER_REPLAY_OUTPUT=/tmp/replay_version_ceiling_base.json.gz \
//	SCHEDULER_VERSION_CEILING=12 \
//	SCHEDULER_VERSION_OVERRIDE=13 \
//	go test -tags integration,test_dep ./tests \
//	  -run '^TestGenerateSchedulerVersionCeilingReplayHistory$' -count=1
//
// A replay fixture must exercise and assert the behavior it is intended to preserve before it is
// captured. To add one, call generateSchedulerReplayHistory with a scenario that configures the
// producer, drives the behavior, and returns the run after making that assertion. Add the output
// to scheduler/testdata; TestReplays automatically runs it against each replay configuration.
//
// For rollback compatibility, copy the producer fixture into the base checkout's scheduler
// testdata directory and run its TestReplays. That executes the history with the actual base
// worker instead of relying on a duplicate "legacy" workflow kept in the PR.
func TestGenerateSchedulerVersionCeilingReplayHistory(t *testing.T) {
	generateSchedulerReplayHistory(t, func(t *testing.T, ctx context.Context, env *testcore.TestEnv) *commonpb.WorkflowExecution {
		ceiling, err := strconv.Atoi(os.Getenv("SCHEDULER_VERSION_CEILING"))
		require.NoError(t, err, "SCHEDULER_VERSION_CEILING must be an integer")
		override := -1
		if value, ok := os.LookupEnv("SCHEDULER_VERSION_OVERRIDE"); ok {
			override, err = strconv.Atoi(value)
			require.NoError(t, err, "SCHEDULER_VERSION_OVERRIDE must be an integer")
		}
		env.OverrideDynamicConfig(dynamicconfig.SchedulerV1VersionCeiling, ceiling)
		env.OverrideDynamicConfig(dynamicconfig.SchedulerV1VersionOverride, override)
		scheduleID := testcore.RandomizeStr("version-ceiling-replay")
		workflowID := scheduler.WorkflowIDPrefix + scheduleID

		// A paused hourly schedule records only the version marker and continue-as-new. Set
		// SCHEDULER_REPLAY_ACTIVE=1 to instead capture a behavior-rich history: an active, fast
		// interval that actually starts workflows (StartWorkflow commands, buffer, next-time
		// cache), which is what a reverse-replay artifact needs to exercise on the old peer.
		spec := intervalSpec(time.Hour)
		state := &schedulepb.ScheduleState{Paused: true}
		if os.Getenv("SCHEDULER_REPLAY_ACTIVE") == "1" {
			spec = intervalSpec(time.Second)
			state = &schedulepb.ScheduleState{Paused: false}
		}
		createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
			Spec:   spec,
			Action: startWorkflowAction(env, testcore.RandomizeStr("unused-action"), "unused-workflow"),
			State:  state,
		})

		execution := waitForSchedulerWorkflowExecution(t, ctx, env, workflowID)
		// The first task records the version ceiling in a MutableSideEffect marker. Without it,
		// this fixture would not cover the configuration-driven behavior.
		var recorded scheduler.TweakablePolicies
		await.RequireTruef(t, func() bool {
			var ok bool
			recorded, ok = recordedSchedulerTweakables(ctx, env, execution)
			return ok
		}, 30*time.Second, 100*time.Millisecond, "V1 scheduler did not record tweakables")
		require.Equal(t, ceiling, recorded.VersionCeiling)
		require.True(t, recorded.VersionCeilingSet)
		wantVersion := scheduler.SchedulerWorkflowVersion(scheduler.TriggerImmediatelyTimestamp)
		if override >= int(wantVersion) && override <= int(scheduler.LatestSchedulerWorkflowVersion) {
			wantVersion = scheduler.SchedulerWorkflowVersion(override)
		}
		if ceiling >= 0 {
			wantVersion = min(wantVersion, scheduler.SchedulerWorkflowVersion(ceiling))
		}
		require.Equal(t, wantVersion, recorded.Version)

		// An active fixture must actually capture a scheduled action (the StartWorkflow command,
		// buffer, and next-time cache the reverse-replay artifact exists to exercise). The
		// tweakables marker lands on the first task, well before the first interval fires, so
		// returning here would let the force-CAN signal below race the action and produce a
		// history no richer than the paused fixture. Wait until at least one action is recorded.
		if os.Getenv("SCHEDULER_REPLAY_ACTIVE") == "1" {
			await.RequireTruef(t, func() bool {
				desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
				})
				return err == nil && desc.GetInfo().GetActionCount() >= 1
			}, 30*time.Second, 100*time.Millisecond, "active V1 scheduler did not start an action before force-CAN")
		}
		return execution
	})
}

// generateSchedulerReplayHistory captures one completed V1 scheduler run after scenario has
// verified the producer behavior it needs in the history. The caller owns the scenario-specific
// configuration and assertions; this helper owns the common V1 worker, forced Continue-As-New,
// and fixture serialization. The checked-in replay test should then use the desired current
// behavior/configuration to replay the generated history.
func generateSchedulerReplayHistory(
	t *testing.T,
	scenario func(t *testing.T, ctx context.Context, env *testcore.TestEnv) *commonpb.WorkflowExecution,
) {
	t.Helper()
	output := os.Getenv("SCHEDULER_REPLAY_OUTPUT")
	if output == "" {
		t.Skip("set SCHEDULER_REPLAY_OUTPUT to generate a scheduler replay fixture")
	}

	env := newScheduleEnv(t, append(
		scheduleCommonOpts(t),
		testcore.WithDedicatedCluster(),
		testcore.WithWorkerService("V1 scheduler replay fixture"),
	)...)
	ctx := testcore.NewContext()
	execution := scenario(t, ctx, env)
	require.NotNil(t, execution, "scheduler replay scenario must return the run to capture")
	require.NotEmpty(t, execution.GetWorkflowId(), "scheduler replay scenario must return a workflow ID")
	require.NotEmpty(t, execution.GetRunId(), "scheduler replay scenario must return a run ID")

	_, err := env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: execution,
		SignalName:        scheduler.SignalNameForceCAN,
		Identity:          "scheduler replay fixture generator",
	})
	require.NoError(t, err)

	await.RequireTruef(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	}, 30*time.Second, 100*time.Millisecond, "V1 scheduler did not continue as new")

	history := &historypb.History{}
	iter := env.SdkClient().GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		history.Events = append(history.Events, event)
	}

	f, err := os.Create(output)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	w := gzip.NewWriter(f)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	data, err := protojson.Marshal(history)
	require.NoError(t, err)
	_, err = w.Write(data)
	require.NoError(t, err)
}

func waitForSchedulerWorkflowExecution(
	t *testing.T,
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) *commonpb.WorkflowExecution {
	t.Helper()
	var execution *commonpb.WorkflowExecution
	await.RequireTruef(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		if err != nil {
			return false
		}
		execution = desc.GetWorkflowExecutionInfo().GetExecution()
		return execution.GetRunId() != ""
	}, 30*time.Second, 100*time.Millisecond, "V1 scheduler workflow did not start")
	return execution
}

func recordedSchedulerTweakables(
	ctx context.Context,
	env *testcore.TestEnv,
	execution *commonpb.WorkflowExecution,
) (scheduler.TweakablePolicies, bool) {
	iter := env.SdkClient().GetWorkflowHistory(
		ctx,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return scheduler.TweakablePolicies{}, false
		}
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs.GetMarkerName() != "MutableSideEffect" {
			continue
		}
		data := attrs.GetDetails()["data"].GetPayloads()
		if len(data) != 2 {
			continue
		}
		var id string
		if payload.Decode(data[0], &id) != nil || id != "tweakables" {
			continue
		}
		var payloads commonpb.Payloads
		if proto.Unmarshal(data[1].GetData(), &payloads) != nil || len(payloads.GetPayloads()) != 1 {
			continue
		}
		var tweakables scheduler.TweakablePolicies
		if payload.Decode(payloads.GetPayloads()[0], &tweakables) != nil {
			return scheduler.TweakablePolicies{}, false
		}
		return tweakables, true
	}
	return scheduler.TweakablePolicies{}, false
}
