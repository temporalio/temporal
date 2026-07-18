package tests

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestGenerateMigrationReplayFixture regenerates the checked-in replay fixture
// service/worker/scheduler/testdata/replay_migration_v1_to_v2.json.gz, which
// guards the RefreshBeforeMigrationCheck (v13) run-loop change against future
// nondeterministic edits. It captures the history of a real V1 scheduler
// workflow that auto-migrated to CHASM (exercising the early-refresh +
// eligibility branch), and writes it in the same protojson format
// client.HistoryFromJSON expects.
//
// It is inert unless GENERATE_REPLAY_FIXTURE is set, so it never runs in CI:
//
//	GENERATE_REPLAY_FIXTURE=1 go test -tags=test_dep ./tests/ \
//	    -run TestGenerateMigrationReplayFixture -count=1 -v
//
// After regenerating, run `go test ./service/worker/scheduler/ -run TestReplays`
// to confirm the new fixture replays cleanly.
func TestGenerateMigrationReplayFixture(t *testing.T) {
	if os.Getenv("GENERATE_REPLAY_FIXTURE") == "" {
		t.Skip("set GENERATE_REPLAY_FIXTURE=1 to regenerate the migration replay fixture")
	}

	// The migration this fixture captures only happens once the early-refresh
	// branch is active, which is gated on RefreshBeforeMigrationCheck. That is
	// intentionally not yet the shipped CurrentTweakablePolicies.Version (activated
	// in a follow-up deploy for rollback safety). Force it here so regeneration
	// works regardless of the current rollout state.
	prevVersion := scheduler.CurrentTweakablePolicies.Version
	scheduler.CurrentTweakablePolicies.Version = scheduler.RefreshBeforeMigrationCheck
	defer func() { scheduler.CurrentTweakablePolicies.Version = prevVersion }()

	env := testcore.NewEnv(t, testcore.WithWorkerService("V1 scheduler"))

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-fixture")
	wid := testcore.RandomizeStr("sched-migrate-fixture-wf")
	wt := testcore.RandomizeStr("sched-migrate-fixture-wt")
	nsName := env.Namespace().String()

	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

	// Short-lived workflow that completes well within the interval, so the
	// schedule is a continuous stream of quick actions -- the shape that
	// exercises the read-ordering fix.
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, 200*time.Millisecond)
	}, workflow.RegisterOptions{Name: wt})

	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Second)}},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule:   sched,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Let it become an established, actively-firing V1 schedule.
	await.RequireTrue(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: nsName, ScheduleId: sid,
		})
		return err == nil && len(descResp.GetInfo().GetRecentActions()) > 0
	}, 15*time.Second, 500*time.Millisecond)

	// Enable automatic migration; the running-workflow guard stays at its default.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, true)
	env.OverrideDynamicConfig(dynamicconfig.CHASMSchedulerMigrationRolloutPercent, 100)

	// The V1 scheduler workflow completes only when migration succeeds.
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	await.RequireTruef(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: env.NamespaceID().String(),
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 1*time.Second, "V1 schedule should auto-migrate to V2")

	// Capture the full history of the migrated V1 scheduler workflow.
	var events []*historypb.HistoryEvent
	var token []byte
	for {
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     nsName,
			Execution:     &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
			NextPageToken: token,
		})
		require.NoError(t, err)
		events = append(events, resp.GetHistory().GetEvents()...)
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}
	require.NotEmpty(t, events)

	data, err := protojson.Marshal(&historypb.History{Events: events})
	require.NoError(t, err)

	outPath := filepath.Join("..", "service", "worker", "scheduler", "testdata", "replay_migration_v1_to_v2.json.gz")
	var buf bytes.Buffer
	gw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	_, err = gw.Write(data)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	require.NoError(t, os.WriteFile(outPath, buf.Bytes(), 0o644))

	t.Logf("wrote %s (%d events)", outPath, len(events))
}
