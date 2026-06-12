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

	// The two action workflow types the scenarios start. fast returns immediately; slow stays
	// running, so triggered actions remain candidates for Info.RunningWorkflows.
	fastWorkflowType = "version-snapshot-fast-wt"
	slowWorkflowType = "version-snapshot-slow-wt"
)

// TestGenerateVersionGates generates the recorded histories TestVersionGates asserts over: for
// each versionguard gate, its minimal scenario recorded against a real server twice, unclamped
// at the current version (the gate must be observed) and clamped just below the gate version
// (absent). It only runs when GENERATE_SCHEDULER_VERSION_SNAPSHOTS=1 and only creates MISSING
// files; regenerating one requires deleting it first, which keeps a frozen-gate change a
// deliberate, review-visible act. Each history is self-validated (observed on the expected
// side + replays under the current binary) before it lands on disk.
//
// The scenarios drive real schedules and sleep on real time (some gates need a scheduled fire
// or an update to be processed), so a full run takes a few minutes. Transient cluster error
// logs over such a run can fail the parent test even when every subtest passed; the written
// files are still valid, and a rerun confirms by skipping over them.
func TestGenerateVersionGates(t *testing.T) {
	if os.Getenv(generateSnapshotsEnv) != "1" {
		t.Skipf("version-gate generator skipped; set %s=1 to run", generateSnapshotsEnv)
	}
	require.Lenf(t, gateDrives, len(versionguard.Gates()),
		"gateDrives must have exactly one scenario per versionguard gate")

	env := testcore.NewEnv(t,
		testcore.WithWorkerService("scheduler version gates"),
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

	for _, g := range versionguard.Gates() {
		drive := gateDrives[g.Name]
		require.NotNilf(t, drive, "no drive scenario for gate %q", g.Name)
		for _, present := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s_%s", g.Name, sideLabel(present)), func(t *testing.T) {
				target := filepath.Join(snapshotTestdataDir, versionguard.GateFixtureName(int(g.Version), g.Name, present))
				if _, err := os.Stat(target); err == nil {
					t.Logf("history exists, not regenerating: %s", target)
					return
				}

				// present: no clamp, so the run records at the current version and the gate is
				// on. absent: a ceiling at version-1 lowers the recorded version just below the
				// gate, turning it off.
				if !present {
					cleanup := env.OverrideDynamicConfig(dynamicconfig.SchedulerVersionCeiling, int(g.Version)-1)
					defer cleanup()
				}

				sid := fmt.Sprintf("gate-%s-%s", g.Name, sideLabel(present))
				events := drive(newRun(t, env, sid))

				// Self-validate before persisting: the gate's observed state must be on the side
				// this run demands, and the history must replay under the current binary.
				obs, err := versionguard.Observe(events)
				require.NoError(t, err)
				if g.IsObserved(obs) != present {
					logObservations(t, obs)
					require.Equalf(t, present, g.IsObserved(obs),
						"gate %q (%s) on the %s side: observed should be %v", g.Name, g.Why, sideLabel(present), present)
				}
				persistSnapshot(t, target, events)
			})
		}
	}
}

func sideLabel(present bool) string {
	if present {
		return "current"
	}
	return "clamped"
}

// gateDrives maps each versionguard gate (by name) to the minimal scenario that exercises it.
// TestGenerateVersionGates asserts every gate has exactly one entry. The
// scenario is clamp-agnostic: the same stimulus runs at both clamps, and the recorded version
// (set by the caller's ceiling override) decides whether the gate's behavior manifests.
var gateDrives = map[string]func(*run) []*historypb.HistoryEvent{
	"allow-all-untracked":     driveAllowAllUntracked,
	"inclusive-backfill":      driveInclusiveBackfill,
	"incremental-backfill":    driveIncrementalBackfill,
	"proto-cache":             driveProtoCache,
	"search-attribute-upsert": driveSearchAttributeUpsert,
	"last-action-time":        driveLastActionTime,
	"future-times-limited":    driveFutureTimesLimited,
	"action-status":           driveActionStatus,
	"memo-spec-trimmed":       driveMemoSpecTrimmed,
	"trigger-supplied-time":   driveTriggerSuppliedTime,
}

// Each drive below is the minimal stimulus for one gate. Comments note only the non-obvious
// shaping; the gate's behavioral meaning lives in versionguard.Gates().

// driveAllowAllUntracked: two ALLOW_ALL triggers start long-running actions. At v3 they are
// not tracked in RunningWorkflows; below, they are. The coarse interval never fires on its
// own; the 1.5s gap keeps the two triggers in distinct wall-clock seconds (else they share a
// timestamped workflow id and the second wedges on WorkflowExecutionAlreadyStarted).
func driveAllowAllUntracked(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Hour), slowWorkflowType, nil, nil)
	for i := int64(1); i <= 2; i++ {
		r.patch(immediateTrigger(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
		r.awaitActions(i)
		r.elapse(1500 * time.Millisecond)
	}
	return r.forceCANAndFetch(firstRun)
}

// driveInclusiveBackfill: a single-point backfill exactly on the one-hour grid. At v4 the
// inclusive start time fires the point; below, the exclusive start skips it. Paused so no
// natural fire interferes; the point cannot be awaited (it only happens at v4), so a fixed
// elapse lets the backfill signal be processed before the continue-as-new.
func driveInclusiveBackfill(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Hour), fastWorkflowType, &schedulepb.ScheduleState{Paused: true}, nil)
	point := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour)
	r.patch(pointBackfill(point))
	r.elapse(3 * time.Second)
	return r.forceCANAndFetch(firstRun)
}

// driveIncrementalBackfill: a wide range backfill on a fine grid (far more nominals than one
// iteration drains), then immediate continue-as-new. At v5 the un-drained remainder rides the
// CAN input as OngoingBackfills; below, the whole range is buffered synchronously as
// BufferedStarts. BUFFER_ALL keeps the catch-ups queued rather than SKIP-dropping them. No
// elapse: the continue-as-new must capture the remainder before the fast actions, which
// re-wake the workflow as they complete, drain OngoingBackfills into the buffer.
func driveIncrementalBackfill(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(calendarEveryFiveSeconds(), fastWorkflowType, nil, bufferAllPolicy())
	r.patch(backfillRange(time.Now().UTC().Add(-6*time.Minute), time.Now().UTC().Add(-10*time.Second)))
	return r.forceCANAndFetch(firstRun)
}

// driveProtoCache: any firing schedule fills the V2 next-time cache (the first iteration's
// memo computation alone does). At v6 it is serialized as proto; below v6 (but at or above
// v2) as JSON. Coarse interval so no fire is needed; the elapse lets the first iteration
// record the cache marker.
func driveProtoCache(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Hour), fastWorkflowType, nil, nil)
	r.elapse(2 * time.Second)
	return r.forceCANAndFetch(firstRun)
}

// driveSearchAttributeUpsert: an update carrying a custom search attribute. At v6 the workflow
// upserts it (an UpsertSearchAttributes command); below, the upsert is skipped.
func driveSearchAttributeUpsert(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Hour), fastWorkflowType, nil, nil)
	r.elapse(2 * time.Second)
	r.updateSchedule(intervalGrid(time.Hour), fastWorkflowType, customSearchAttributes())
	r.elapse(2 * time.Second)
	return r.forceCANAndFetch(firstRun)
}

// driveLastActionTime: one scheduled fire on a fine grid, then continue-as-new. At v8 the CAN
// input's LastProcessedTime equals the last action's nominal time; below, it is the wall-clock
// wakeup, which is later.
func driveLastActionTime(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(calendarEveryFiveSeconds(), fastWorkflowType, nil, nil)
	r.awaitActions(1)
	return r.forceCANAndFetch(firstRun)
}

// driveFutureTimesLimited: a LimitedActions schedule with one remaining action on a coarse
// grid (so it never fires and the remaining count stays one). At v9 the memo advertises at
// most one future action time; below, the full FutureActionCountForList list.
func driveFutureTimesLimited(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Hour), fastWorkflowType,
		&schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 1}, nil)
	r.elapse(2 * time.Second)
	return r.forceCANAndFetch(firstRun)
}

// driveActionStatus: one scheduled fire on a fine grid. At v10 the recorded action carries a
// workflow status (RUNNING at start); below, it stays UNSPECIFIED.
func driveActionStatus(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(calendarEveryFiveSeconds(), fastWorkflowType, nil, nil)
	r.awaitActions(1)
	return r.forceCANAndFetch(firstRun)
}

// driveMemoSpecTrimmed: a schedule whose spec has more than SpecFieldLengthLimit entries on a
// coarse grid (so it never fires). At v11 the memo's spec is trimmed to the limit; below, the
// memo carries the full spec. The continue-as-new input keeps the full (untrimmed) spec.
func driveMemoSpecTrimmed(r *run) []*historypb.HistoryEvent {
	intervals := make([]time.Duration, scheduler.CurrentTweakablePolicies.SpecFieldLengthLimit+5)
	for i := range intervals {
		intervals[i] = time.Duration(50+i) * time.Minute
	}
	firstRun := r.startSchedule(intervalGrid(intervals...), fastWorkflowType, nil, nil)
	r.elapse(2 * time.Second)
	return r.forceCANAndFetch(firstRun)
}

// driveTriggerSuppliedTime: a trigger carrying an explicit off-grid ScheduledTime. At v12 the
// action fires with that nominal time; below, with the signal-processing time. The coarse
// interval never fires on its own, so the only action is the trigger.
func driveTriggerSuppliedTime(r *run) []*historypb.HistoryEvent {
	firstRun := r.startSchedule(intervalGrid(time.Hour), fastWorkflowType, nil, nil)
	supplied := offGridSecond(time.Now().Add(-83 * time.Second))
	r.patch(&schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(supplied)},
	})
	r.awaitActions(1)
	return r.forceCANAndFetch(firstRun)
}

// run is the driver context for one (gate, side): a single schedule under test plus the
// frontend helpers to drive and observe it. Its methods fail the test on any RPC error.
type run struct {
	t   *testing.T
	env *testcore.TestEnv
	sid string
}

func newRun(t *testing.T, env *testcore.TestEnv, sid string) *run {
	return &run{t: t, env: env, sid: sid}
}

// ctx returns a fresh per-call context: a generation run spans minutes of real time, far
// beyond any single context deadline the test environment hands out.
func (r *run) ctx() context.Context {
	return v1ContextFactory(testcore.NewContext())
}

// startSchedule creates the schedule and returns the scheduler workflow's first run id, which
// forceCANAndFetch later uses to fetch run one's history after continue-as-new.
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

// awaitActions waits until the schedule has taken at least want actions, tolerating transient
// RPC errors during the poll.
func (r *run) awaitActions(want int64) {
	await.RequireTruef(r.t, func() bool {
		resp, err := r.env.FrontendClient().DescribeSchedule(r.ctx(), &workflowservice.DescribeScheduleRequest{
			Namespace:  r.env.Namespace().String(),
			ScheduleId: r.sid,
		})
		return err == nil && resp.GetInfo().GetActionCount() >= want
	}, 30*time.Second, 250*time.Millisecond, "schedule %s never reached %d actions", r.sid, want)
}

// forceCANAndFetch signals force-continue-as-new, waits for the first run to close, and
// returns its full history (the recorded history freezes run one; the continuation is irrelevant).
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

// elapse blocks for d of genuine wall-clock time. Some gates depend on real elapsed time (a
// scheduled fire or an update that must actually be processed), which the environment cannot
// fast-forward.
func (r *run) elapse(d time.Duration) {
	time.Sleep(d) //nolint:forbidigo // real elapsed wall-clock is the stimulus
}

// Spec and patch builders, named for the intent they carry in the scenarios.

func intervalGrid(intervals ...time.Duration) *schedulepb.ScheduleSpec {
	spec := &schedulepb.ScheduleSpec{}
	for _, d := range intervals {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(d)})
	}
	return spec
}

// calendarEveryFiveSeconds is a fine grid that fires every five seconds.
func calendarEveryFiveSeconds() *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{Calendar: []*schedulepb.CalendarSpec{{Second: "*/5", Minute: "*", Hour: "*"}}}
}

func bufferAllPolicy() *schedulepb.SchedulePolicies {
	return &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}
}

// pointBackfill backfills the single instant point (StartTime == EndTime) with ALLOW_ALL.
func pointBackfill(point time.Time) *schedulepb.SchedulePatch {
	return &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
		StartTime:     timestamppb.New(point),
		EndTime:       timestamppb.New(point),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}}}
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

// offGridSecond returns base truncated to the second, nudged earlier until its second is not a
// multiple of five, so a fire stamped with it cannot alias a five-second-grid fire.
func offGridSecond(base time.Time) time.Time {
	t := base.UTC().Truncate(time.Second)
	for t.Second()%5 == 0 {
		t = t.Add(-time.Second)
	}
	return t
}

// Validation and history IO.

func logObservations(t *testing.T, o *versionguard.Observations) {
	t.Helper()
	t.Logf("observations: tweakables=%v protoCache=%d jsonCache=%d actions=%d saUpserts=%v patches=%d",
		o.TweakableVersions, o.ProtoCacheMarkers, o.JSONCacheMarkers,
		len(o.RecentActions), o.SAUpserts, len(o.Patches))
	if o.CANInput != nil {
		t.Logf("CAN input: buffered=%d ongoing=%d running=%d lastProcessed=%v futureTimes=%d memoSpec=%d",
			len(o.CANInput.GetState().GetBufferedStarts()),
			len(o.CANInput.GetState().GetOngoingBackfills()),
			len(o.CANInput.GetInfo().GetRunningWorkflows()),
			o.CANInput.GetState().GetLastProcessedTime().AsTime(),
			o.MaxFutureActionTimes, o.MaxMemoSpecEntries)
	}
	nominals := make([]time.Time, 0, len(o.RecentActions))
	for nominal := range o.RecentActions {
		nominals = append(nominals, nominal)
	}
	sort.Slice(nominals, func(i, j int) bool { return nominals[i].Before(nominals[j]) })
	t.Logf("action nominals: %v", nominals)
}

// persistSnapshot serializes events, proves the serialized form replays under the current
// binary, then writes it gzip-compressed to target.
func persistSnapshot(t *testing.T, target string, events []*historypb.HistoryEvent) {
	t.Helper()
	data, err := protojson.Marshal(&historypb.History{Events: events})
	require.NoError(t, err)
	replaySnapshot(t, data)
	writeSnapshot(t, target, data)
	t.Logf("wrote %s (%d events)", target, len(events))
}

func replaySnapshot(t *testing.T, data []byte) {
	t.Helper()
	history, err := sdkclient.HistoryFromJSON(bytes.NewReader(data), sdkclient.HistoryJSONOptions{})
	require.NoError(t, err, "serialized history must load through HistoryFromJSON")
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{Name: scheduler.WorkflowType})
	require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), history),
		"serialized history must replay under the current binary")
}

func writeSnapshot(t *testing.T, target string, data []byte) {
	t.Helper()
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)
	_, err = zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(target, buf.Bytes(), 0o644))
}
