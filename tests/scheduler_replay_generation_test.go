package tests

import (
	"compress/gzip"
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
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestGenerateSchedulerVersionCeilingReplayHistory records a small V1 scheduler history at the
// checked-out revision. Run this test at both the producer and compatibility-base revisions, then
// put both outputs in service/worker/scheduler/testdata; TestReplays will exercise both histories.
//
// Example:
//
//	SCHEDULER_REPLAY_OUTPUT=/tmp/replay_version_ceiling_base.json.gz \
//	SCHEDULER_VERSION_CEILING=12 \
//	go test -tags integration,test_dep ./tests \
//	  -run '^TestGenerateSchedulerVersionCeilingReplayHistory$' -count=1
//
// For the rollback direction, copy the producer fixture into the base checkout's scheduler
// testdata directory and run its TestReplays. That executes the history with the actual base
// worker instead of relying on a duplicate "legacy" workflow kept in the PR.
func TestGenerateSchedulerVersionCeilingReplayHistory(t *testing.T) {
	output := os.Getenv("SCHEDULER_REPLAY_OUTPUT")
	if output == "" {
		t.Skip("set SCHEDULER_REPLAY_OUTPUT to generate a scheduler replay fixture")
	}
	ceiling, err := strconv.Atoi(os.Getenv("SCHEDULER_VERSION_CEILING"))
	require.NoError(t, err, "SCHEDULER_VERSION_CEILING must be an integer")

	env := newScheduleEnv(t, append(
		scheduleCommonOpts(t),
		testcore.WithDedicatedCluster(),
		testcore.WithWorkerService("V1 scheduler replay fixture"),
	)...)
	env.OverrideDynamicConfig(dynamicconfig.SchedulerV1VersionCeiling, ceiling)
	ctx := env.Context()
	scheduleID := testcore.RandomizeStr("version-ceiling-replay")
	workflowID := scheduler.WorkflowIDPrefix + scheduleID

	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec:   intervalSpec(time.Hour),
		Action: startWorkflowAction(env, testcore.RandomizeStr("unused-action"), "unused-workflow"),
		State:  &schedulepb.ScheduleState{Paused: true},
	})

	var runID string
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		if err != nil {
			return false
		}
		runID = desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		return runID != ""
	}, 30*time.Second, 100*time.Millisecond, "V1 scheduler workflow did not start")

	// Wait until the first task has recorded tweakables before forcing the run to close.
	require.Eventually(t, func() bool {
		iter := env.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				return false
			}
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "V1 scheduler did not complete its first workflow task")

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		SignalName: scheduler.SignalNameForceCAN,
		Identity:   "scheduler replay fixture generator",
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	}, 30*time.Second, 100*time.Millisecond, "V1 scheduler did not continue as new")

	history := &historypb.History{}
	iter := env.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
