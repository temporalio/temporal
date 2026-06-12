package tests

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/service/worker/scheduler/versionguard"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	generateSnapshotsEnv = "GENERATE_SCHEDULER_VERSION_SNAPSHOTS"
	snapshotTestdataDir  = "../service/worker/scheduler/testdata"

	scheduleIdentity = "test"

	// The two action workflow types the scenarios start. fast returns immediately; slow
	// stays running, so triggered actions remain candidates for Info.RunningWorkflows.
	fastWorkflowType = "version-snapshot-fast-wt"
	slowWorkflowType = "version-snapshot-slow-wt"
)

// TestGenerateSchedulerVersionSnapshots generates the golden version snapshots that
// TestVersionSnapshotsComplete demands: one real-server history per (version, scenario),
// recorded with worker.schedulerVersionCeiling clamped to that version. It only runs when
// GENERATE_SCHEDULER_VERSION_SNAPSHOTS=1 and only creates MISSING files; regenerating an
// existing version's snapshot requires deleting it first, which makes a frozen-gate
// violation a deliberate, review-visible act.
//
// The scenarios drive a real schedule through the frontend and sleep on real time (the v8
// gate needs a scheduled fire before an update), so a full run takes several minutes.
// Transient cluster error logs over such a long run can fail the parent test even when
// every snapshot subtest passed; the written snapshots are still valid (each is
// self-validated before it lands on disk), and a rerun confirms by skipping over them.
func TestGenerateSchedulerVersionSnapshots(t *testing.T) {
	if os.Getenv(generateSnapshotsEnv) != "1" {
		t.Skipf("snapshot generator skipped; set %s=1 to run", generateSnapshotsEnv)
	}
	requireScenarioCoverage(t)

	env := testcore.NewEnv(t,
		testcore.WithWorkerService("scheduler version snapshots"),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	)
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: fastWorkflowType},
	)
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return workflow.Sleep(ctx, 10*time.Minute) },
		workflow.RegisterOptions{Name: slowWorkflowType},
	)

	for version := 1; version <= versionguard.MaxVersion(); version++ {
		for _, sc := range scenarios {
			t.Run(fmt.Sprintf("v%02d_%s", version, sc.name), func(t *testing.T) {
				target := filepath.Join(snapshotTestdataDir, versionguard.SnapshotName(version, sc.name))
				if _, err := os.Stat(target); err == nil {
					t.Logf("snapshot exists, not regenerating: %s", target)
					return
				}

				cleanup := env.OverrideDynamicConfig(dynamicconfig.SchedulerVersionCeiling, version)
				defer cleanup()

				sid := fmt.Sprintf("version-snapshot-%s-v%02d", sc.name, version)
				events := sc.drive(newRun(t, env, sid))

				// Self-validate before persisting: the history must exhibit every gate on
				// the side this clamp demands, and must replay under the current binary.
				requireTokenMatrix(t, events, version, sc.name)
				persistSnapshot(t, target, events)
			})
		}
	}
}

// scenario is one customer story the generator can replay. Every scenario is
// version-agnostic: the same story runs at every clamp, and worker.schedulerVersionCeiling
// (set by the caller) decides which version the workflow records. versionguard's tokens,
// keyed to the same scenario name, then assert that the recorded history exhibits each gate
// on the side the clamp demands. There is therefore no per-version branching here; adding a
// version only needs an existing scenario to keep exercising its gate.
type scenario struct {
	name  string
	drive func(*run) []*historypb.HistoryEvent
}

// scenarios must cover exactly versionguard.Scenarios(); requireScenarioCoverage enforces it.
var scenarios = []scenario{
	{versionguard.ScenarioCore, driveCore},
	{versionguard.ScenarioLimited, driveLimited},
}

func requireScenarioCoverage(t *testing.T) {
	have := make([]string, len(scenarios))
	for i, sc := range scenarios {
		have[i] = sc.name
	}
	require.ElementsMatch(t, versionguard.Scenarios(), have,
		"generator scenarios must match versionguard.Scenarios()")
}

// driveCore plays out an everyday schedule's life. A customer:
//  1. creates an unpaused schedule on a coarse one-minute grid,
//  2. triggers it immediately for an explicit past time and backfills a single point,
//  3. lets it fire once on its own,
//  4. retunes it to a fine five-second grid and tags it with a search attribute,
//  5. backfills a wide past range,
//  6. and forces it to continue-as-new.
//
// This exercises the time-query cache (v1, v2, v7), the timestamped trigger (v12), inclusive
// backfill (v4), update reprocessing (v6), last-action time plus search-attribute upsert
// (v8), action status (v10), and incremental backfill riding the continue-as-new input (v5).
// See versionguard's "core" tokens for the artifact each gate leaves behind.
//
// Director's notes (why the story is shaped this way):
//   - One-minute grid: natural fires land on :00 seconds, which the v6 token's fine-grid
//     filter ignores, so they cannot be mistaken for post-update catch-ups.
//   - BUFFER_ALL: keeps catch-up starts queued instead of SKIP-dropping them while a fired
//     action is still running, so v6's pre-update fire actually records, and the range
//     backfill leaves a remainder for the continue-as-new input (v5) rather than starting
//     everything at once (ALLOW_ALL) or dropping it (SKIP).
//   - One combined trigger+backfill patch: the workflow keeps a single pending-patch slot,
//     so two signals drained in the same iteration would clobber each other.
//   - The 12s and 8s waits are genuine elapsed time: v8 anchors update reprocessing on the
//     last scheduled fire, so a natural fire must occur and time must pass before the update.
func driveCore(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Minute), fastWorkflowType, nil, bufferAllPolicy())

	// Trigger immediately for an explicit off-grid past time (v12), and backfill one on-grid
	// point an hour back, well clear of any natural fire (v4).
	supplied := offGridSecond(time.Now().Add(-83 * time.Second))
	point := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	r.patch(triggerAndBackfillPoint(supplied, point))
	r.awaitActions(1)

	// Let the schedule fire once on its own, then let real time pass so the coming update's
	// reprocessing window covers elapsed five-second-grid points.
	r.awaitNaturalFire(point, supplied)
	r.elapse(12 * time.Second)

	// Retune to a fine five-second grid and tag the schedule with a search attribute (v6, v8).
	r.updateSchedule(calendarEveryFiveSeconds(), fastWorkflowType, customSearchAttributes())

	// Catch-up fires land on the new grid: v6 off-evidence below v6, the v8 last-action anchor.
	r.elapse(8 * time.Second)
	r.awaitMoreActions(2)

	// Backfill a wide past range, then force continue-as-new so its un-drained remainder
	// rides the continue-as-new input as OngoingBackfills (v5).
	r.patch(backfillRange(time.Now().UTC().Add(-6*time.Minute), time.Now().UTC().Add(-30*time.Second)))
	return r.forceCANAndFetch(firstRun)
}

// driveLimited plays out a capped schedule. A customer creates a LimitedActions schedule
// (one remaining action) whose spec is oversized and whose action runs long, then fires two
// ALLOW_ALL triggers and forces continue-as-new.
//
// This exercises future-times capping (v9), memo spec trimming (v11), and whether the
// still-running overlapping actions are tracked in the continue-as-new input (v3). See
// versionguard's "limited" tokens.
//
// Director's notes:
//   - 15 distinct long intervals: more than SpecFieldLengthLimit so v11's trimming is
//     observable, and far enough out that none fire on their own; only the manual triggers act.
//   - Slow action + ALLOW_ALL: the triggered runs stay alive and overlapping, so they are
//     candidates for Info.RunningWorkflows (v3).
//   - The two triggers are serialized with a wait and a real gap: a second patch in one
//     iteration would clobber the first (single pending-patch slot), and two triggers inside
//     the same wall-clock second produce the same timestamped workflow id, wedging the second
//     start on WorkflowExecutionAlreadyStarted while the first long-running action holds it.
func driveLimited(r *run) []*historypb.HistoryEvent {
	intervals := make([]time.Duration, 15)
	for i := range intervals {
		intervals[i] = time.Duration(50+i) * time.Minute
	}
	firstRun := r.startSchedule(intervalGrid(intervals...), slowWorkflowType,
		&schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 1}, nil)

	for i := int64(1); i <= 2; i++ {
		r.patch(immediateTrigger(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
		r.awaitActions(i)
		r.elapse(1500 * time.Millisecond)
	}
	return r.forceCANAndFetch(firstRun)
}

// run is the driver context for one (scenario, version): a single schedule under test plus
// the frontend helpers to drive and observe it. Its methods read like customer actions and
// fail the test on any RPC error.
type run struct {
	t   *testing.T
	env *testcore.TestEnv
	sid string
}

func newRun(t *testing.T, env *testcore.TestEnv, sid string) *run {
	return &run{t: t, env: env, sid: sid}
}

// ctx returns a fresh per-call context: a generation run spans many minutes of real time,
// far beyond any single context deadline the test environment hands out.
func (r *run) ctx() context.Context {
	return v1ContextFactory(testcore.NewContext())
}

// startSchedule creates the schedule and returns the scheduler workflow's first run id,
// which forceCANAndFetch later uses to fetch run one's history after continue-as-new.
func (r *run) startSchedule(spec *schedulepb.ScheduleSpec, workflowType string, state *schedulepb.ScheduleState, policies *schedulepb.SchedulePolicies) string {
	_, err := r.env.FrontendClient().CreateSchedule(r.ctx(), &workflowservice.CreateScheduleRequest{
		Namespace:  r.env.Namespace().String(),
		ScheduleId: r.sid,
		Schedule: &schedulepb.Schedule{
			Spec:     spec,
			Action:   r.action(workflowType),
			State:    state,
			Policies: policies,
		},
		Identity:  scheduleIdentity,
		RequestId: uuid.NewString(),
	})
	require.NoError(r.t, err)
	return r.firstRunID()
}

// updateSchedule replaces the schedule's spec and action and upserts search attributes,
// resolving the conflict token from a Describe first.
func (r *run) updateSchedule(spec *schedulepb.ScheduleSpec, workflowType string, searchAttributes *commonpb.SearchAttributes) {
	desc, err := r.env.FrontendClient().DescribeSchedule(r.ctx(), &workflowservice.DescribeScheduleRequest{
		Namespace:  r.env.Namespace().String(),
		ScheduleId: r.sid,
	})
	require.NoError(r.t, err)
	_, err = r.env.FrontendClient().UpdateSchedule(r.ctx(), &workflowservice.UpdateScheduleRequest{
		Namespace:        r.env.Namespace().String(),
		ScheduleId:       r.sid,
		Schedule:         &schedulepb.Schedule{Spec: spec, Action: r.action(workflowType)},
		ConflictToken:    desc.ConflictToken,
		SearchAttributes: searchAttributes,
		Identity:         scheduleIdentity,
		RequestId:        uuid.NewString(),
	})
	require.NoError(r.t, err)
}

func (r *run) patch(patch *schedulepb.SchedulePatch) {
	_, err := r.env.FrontendClient().PatchSchedule(r.ctx(), &workflowservice.PatchScheduleRequest{
		Namespace:  r.env.Namespace().String(),
		ScheduleId: r.sid,
		Patch:      patch,
		Identity:   scheduleIdentity,
		RequestId:  uuid.NewString(),
	})
	require.NoError(r.t, err)
}

func (r *run) action(workflowType string) *schedulepb.ScheduleAction {
	return &schedulepb.ScheduleAction{
		Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   r.sid + "-wf",
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: r.env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			},
		},
	}
}

func (r *run) firstRunID() string {
	var runID string
	await.RequireTruef(r.t, func() bool {
		resp, err := r.env.FrontendClient().DescribeWorkflowExecution(r.ctx(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: r.env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + r.sid},
		})
		if err != nil {
			return false
		}
		runID = resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		return runID != ""
	}, 15*time.Second, 200*time.Millisecond, "scheduler workflow for %s never appeared", r.sid)
	return runID
}

// awaitActions waits until the schedule has taken at least want actions.
func (r *run) awaitActions(want int64) {
	await.RequireTruef(r.t, func() bool {
		return r.actionCount() >= want
	}, 30*time.Second, 250*time.Millisecond, "schedule %s never reached %d actions", r.sid, want)
}

// awaitMoreActions waits for the action count to grow by delta from its current value.
func (r *run) awaitMoreActions(delta int64) {
	r.awaitActions(r.actionCount() + delta)
}

func (r *run) actionCount() int64 {
	resp, err := r.env.FrontendClient().DescribeSchedule(r.ctx(), &workflowservice.DescribeScheduleRequest{
		Namespace:  r.env.Namespace().String(),
		ScheduleId: r.sid,
	})
	require.NoError(r.t, err)
	return resp.GetInfo().GetActionCount()
}

// awaitNaturalFire waits for a fire on the schedule's own grid: a :00-second nominal that is
// none of the excluded times (the point backfill and the supplied trigger time).
func (r *run) awaitNaturalFire(exclude ...time.Time) {
	await.RequireTruef(r.t, func() bool {
		resp, err := r.env.FrontendClient().DescribeSchedule(r.ctx(), &workflowservice.DescribeScheduleRequest{
			Namespace:  r.env.Namespace().String(),
			ScheduleId: r.sid,
		})
		if err != nil {
			return false
		}
		for _, action := range resp.GetInfo().GetRecentActions() {
			nominal := action.GetScheduleTime().AsTime().UTC().Truncate(time.Second)
			if nominal.Second() != 0 || containsTime(exclude, nominal) {
				continue
			}
			return true
		}
		return false
	}, 90*time.Second, 500*time.Millisecond, "schedule %s never fired naturally", r.sid)
}

// forceCANAndFetch signals force-continue-as-new, waits for the first run to close, and
// returns its full history (the snapshot freezes run one; the continuation is irrelevant).
func (r *run) forceCANAndFetch(firstRunID string) []*historypb.HistoryEvent {
	_, err := r.env.FrontendClient().SignalWorkflowExecution(r.ctx(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         r.env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + r.sid},
		SignalName:        scheduler.SignalNameForceCAN,
		Identity:          scheduleIdentity,
		RequestId:         uuid.NewString(),
	})
	require.NoError(r.t, err)

	execution := &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + r.sid, RunId: firstRunID}
	await.RequireTruef(r.t, func() bool {
		resp, err := r.env.FrontendClient().DescribeWorkflowExecution(r.ctx(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: r.env.Namespace().String(),
			Execution: execution,
		})
		return err == nil &&
			resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	}, 30*time.Second, 250*time.Millisecond, "first run of %s never continued-as-new", r.sid)

	return r.env.GetHistory(r.env.Namespace().String(), execution)
}

// elapse blocks for d of genuine wall-clock time. The scenarios depend on real elapsed time
// (e.g. a scheduled fire that must actually happen), which the environment cannot fast-forward.
func (r *run) elapse(d time.Duration) {
	time.Sleep(d) //nolint:forbidigo // real elapsed wall-clock is the stimulus
}

// Spec and patch builders, named for the intent they carry in the scenarios.

// intervalGrid is a schedule spec firing on the given interval(s).
func intervalGrid(intervals ...time.Duration) *schedulepb.ScheduleSpec {
	spec := &schedulepb.ScheduleSpec{}
	for _, d := range intervals {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(d)})
	}
	return spec
}

// calendarEveryFiveSeconds is a fine grid whose fires land on five-second boundaries,
// distinct from a one-minute grid's :00 fires (which the v6 token's fine-grid filter ignores).
func calendarEveryFiveSeconds() *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{Calendar: []*schedulepb.CalendarSpec{{Second: "*/5", Minute: "*", Hour: "*"}}}
}

func bufferAllPolicy() *schedulepb.SchedulePolicies {
	return &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}
}

// triggerAndBackfillPoint fires an immediate trigger stamped with scheduledTime and backfills
// the single instant point, in one patch (see driveCore's note on the single pending-patch slot).
func triggerAndBackfillPoint(scheduledTime, point time.Time) *schedulepb.SchedulePatch {
	return &schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(scheduledTime)},
		BackfillRequest: []*schedulepb.BackfillRequest{{
			StartTime:     timestamppb.New(point),
			EndTime:       timestamppb.New(point),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}},
	}
}

// backfillRange backfills [from, to] with BUFFER_ALL so the catch-up starts queue rather than
// SKIP-dropping or ALLOW_ALL-starting at once, leaving a remainder for the continue-as-new input.
func backfillRange(from, to time.Time) *schedulepb.SchedulePatch {
	return &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
		StartTime:     timestamppb.New(from),
		EndTime:       timestamppb.New(to),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	}}}
}

func immediateTrigger(overlap enumspb.ScheduleOverlapPolicy) *schedulepb.SchedulePatch {
	return &schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: overlap},
	}
}

func customSearchAttributes() *commonpb.SearchAttributes {
	return &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			versionguard.CustomSearchAttribute: payload.EncodeString("versionguard"),
		},
	}
}

// offGridSecond returns base truncated to the second, nudged earlier until its second is not
// a multiple of five, so a fire stamped with it cannot be mistaken for a five-second-grid fire.
func offGridSecond(base time.Time) time.Time {
	t := base.UTC().Truncate(time.Second)
	for t.Second()%5 == 0 {
		t = t.Add(-time.Second)
	}
	return t
}

func containsTime(times []time.Time, want time.Time) bool {
	for _, t := range times {
		if t.UTC().Truncate(time.Second).Equal(want) {
			return true
		}
	}
	return false
}

// Validation and snapshot IO.

// requireTokenMatrix fails the test if the generated history does not exhibit every version
// token on the side the clamp demands, dumping the recorded artifacts to aid debugging.
func requireTokenMatrix(t *testing.T, events []*historypb.HistoryEvent, version int, scenario string) {
	err := versionguard.Evaluate(events, version, scenario)
	if err == nil {
		return
	}
	logArtifacts(t, events)
	require.NoError(t, err, "generated history does not exercise scenario %q's gates at clamp v%d", scenario, version)
}

func logArtifacts(t *testing.T, events []*historypb.HistoryEvent) {
	a, err := versionguard.Parse(events)
	if err != nil {
		return
	}
	t.Logf("artifacts: tweakables=%v protoCache=%d jsonCache=%d v1Batch=%d actions=%d saUpserts=%v patches=%d updates=%d",
		a.TweakableVersions, a.ProtoCacheMarkers, a.JSONCacheMarkers, a.V1BatchTimeQueries,
		len(a.RecentActions), a.SAUpserts, len(a.Patches), len(a.Updates))
	if a.CANInput != nil {
		t.Logf("CAN input: buffered=%d ongoing=%d running=%d lastProcessed=%v",
			len(a.CANInput.GetState().GetBufferedStarts()),
			len(a.CANInput.GetState().GetOngoingBackfills()),
			len(a.CANInput.GetInfo().GetRunningWorkflows()),
			a.CANInput.GetState().GetLastProcessedTime().AsTime())
	}
	nominals := make([]time.Time, 0, len(a.RecentActions))
	for nominal := range a.RecentActions {
		nominals = append(nominals, nominal)
	}
	sort.Slice(nominals, func(i, j int) bool { return nominals[i].Before(nominals[j]) })
	t.Logf("action nominals: %v", nominals)
	for _, u := range a.Updates {
		t.Logf("update signal at: %v", u.Time)
	}
}

// persistSnapshot serializes events, proves the serialized form replays under the current
// binary, then writes it gzip-compressed to target.
func persistSnapshot(t *testing.T, target string, events []*historypb.HistoryEvent) {
	data, err := protojson.Marshal(&historypb.History{Events: events})
	require.NoError(t, err)
	replaySnapshot(t, data)
	writeSnapshot(t, target, data)
	t.Logf("wrote %s (%d events)", target, len(events))
}

func replaySnapshot(t *testing.T, data []byte) {
	history, err := sdkclient.HistoryFromJSON(bytes.NewReader(data), sdkclient.HistoryJSONOptions{})
	require.NoError(t, err, "serialized snapshot must load through HistoryFromJSON")
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{Name: scheduler.WorkflowType})
	require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), history),
		"serialized snapshot must replay under the current binary")
}

func writeSnapshot(t *testing.T, target string, data []byte) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)
	_, err = zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(target, buf.Bytes(), 0o644))
}
