package tests

import (
	"context"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestScheduleVisibilityWriteBenchmark creates a single schedule that fires
// exactly three times and then completes, and counts how many times the
// schedule's lifecycle hits the visibility backend. It runs the same scenario
// against both the legacy V1 scheduler and the CHASM scheduler so the two
// backends' visibility write volume can be compared directly.
//
// Rather than reading a metric, this test wraps the History service's
// VisibilityManager with a counting decorator (see countingVisibilityManager)
// so every RecordWorkflowExecutionStarted / RecordWorkflowExecutionClosed /
// UpsertWorkflowExecution / DeleteWorkflowExecution that lands in the
// visibility store for the test namespace is counted at its source. The CHASM
// visibility manager is built on top of the same manager.VisibilityManager, so
// CHASM scheduler visibility writes are captured too.
func TestScheduleVisibilityWriteBenchmark(t *testing.T) {
	v1 := runScheduleVisibilityScenario(t, "v1", v1ContextFactory,
		testcore.WithWorkerService("V1 scheduler"))
	chasm := runScheduleVisibilityScenario(t, "chasm", chasmContextFactory)

	t.Logf("\n===== schedule visibility-write benchmark (single schedule, 3 fires) =====\n%s\n%s\nTOTAL writes: v1=%d  chasm=%d  (delta=%+d)",
		v1.summary("V1   "),
		chasm.summary("CHASM"),
		v1.total(), chasm.total(), chasm.total()-v1.total())

	// Sanity: both backends must have produced visibility writes for the
	// schedule scenario, otherwise the decorator wasn't wired in.
	require := func(name string, c *scheduleVisCounters) {
		t.Helper()
		if c.total() == 0 {
			t.Fatalf("%s: expected visibility writes but counted none; decorator likely not installed", name)
		}
	}
	require("v1", v1)
	require("chasm", chasm)
}

// runScheduleVisibilityScenario installs a counting visibility decorator on the
// History service, creates a schedule that fires exactly three times then
// completes, waits for it to reach the completed state in visibility, and
// returns the visibility-write counts attributable to the test namespace.
func runScheduleVisibilityScenario(
	t *testing.T,
	label string,
	newContext contextFactory,
	extraOpts ...testcore.TestOption,
) *scheduleVisCounters {
	counters := &scheduleVisCounters{}

	opts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"}),
		// Decorate the History service's visibility manager so every write to
		// the visibility backend is counted. WithFxOptions forces a dedicated
		// cluster, which keeps the counts isolated to this test.
		testcore.WithFxOptions(primitives.HistoryService,
			fx.Decorate(func(base manager.VisibilityManager) manager.VisibilityManager {
				return &countingVisibilityManager{VisibilityManager: base, counters: counters}
			}),
		),
	}
	opts = append(opts, extraOpts...)

	s := testcore.NewEnv(t, opts...)

	// Point the counter at this test's namespace before any schedule activity
	// so background writes from other namespaces are ignored.
	counters.setTarget(s.NamespaceID().String())

	sid := testcore.RandomizeStr("sched-vis-bench-" + label)
	wid := testcore.RandomizeStr("sched-vis-bench-wf-" + label)
	wt := testcore.RandomizeStr("sched-vis-bench-wt-" + label)
	counters.setActionWorkflowType(wt)

	var runs atomic.Int32
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			runs.Add(1)
			return nil
		},
		workflow.RegisterOptions{Name: wt},
	)

	// A 1s interval with LimitedActions=3: the schedule fires three times and
	// then takes no further scheduled actions.
	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		State: &schedulepb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 3,
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:            wid,
					WorkflowType:          &commonpb.WorkflowType{Name: wt},
					TaskQueue:             &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Wait for all three actions to fire and their workflows to run.
	s.Eventually(func() bool { return runs.Load() >= 3 },
		30*time.Second, 200*time.Millisecond, "schedule should fire three times")

	// Wait until visibility reflects the completed schedule: three recent
	// actions and no remaining future action times. This is the deterministic
	// "done" point at which we read the write counts.
	getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		info := ent.GetInfo()
		return len(info.GetRecentActions()) >= 3 && len(info.GetFutureActionTimes()) == 0
	})

	// Let any trailing visibility write for the final state settle, then snapshot.
	time.Sleep(1 * time.Second)
	return counters.snapshot()
}

// scheduleVisCounters accumulates visibility-backend write counts for a single
// target namespace, broken down by operation and workflow type. It is shared
// (by pointer) across every History instance's decorator, so all writes
// aggregate here regardless of how many history hosts the cluster runs.
type scheduleVisCounters struct {
	mu           sync.Mutex
	targetNSID   string
	actionWFType string         // workflow type of the workflows the schedule starts
	started      map[string]int // RecordWorkflowExecutionStarted, by workflow type
	closed       map[string]int // RecordWorkflowExecutionClosed, by workflow type
	upserted     map[string]int // UpsertWorkflowExecution, by workflow type
	deleted      int            // DeleteWorkflowExecution (workflow type unavailable)
}

func (c *scheduleVisCounters) setTarget(nsID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targetNSID = nsID
}

func (c *scheduleVisCounters) setActionWorkflowType(wt string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actionWFType = wt
}

func (c *scheduleVisCounters) matches(nsID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.targetNSID != "" && nsID == c.targetNSID
}

func (c *scheduleVisCounters) record(field *map[string]int, wfType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if *field == nil {
		*field = make(map[string]int)
	}
	(*field)[wfType]++
}

func (c *scheduleVisCounters) recordStarted(wfType string) { c.record(&c.started, wfType) }
func (c *scheduleVisCounters) recordClosed(wfType string)  { c.record(&c.closed, wfType) }
func (c *scheduleVisCounters) recordUpsert(wfType string)  { c.record(&c.upserted, wfType) }
func (c *scheduleVisCounters) recordDeleted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted++
}

// snapshot returns a deep copy safe to read without holding the lock.
func (c *scheduleVisCounters) snapshot() *scheduleVisCounters {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := &scheduleVisCounters{
		targetNSID:   c.targetNSID,
		actionWFType: c.actionWFType,
		started:      copyMap(c.started),
		closed:       copyMap(c.closed),
		upserted:     copyMap(c.upserted),
		deleted:      c.deleted,
	}
	return cp
}

func (c *scheduleVisCounters) upsertCount(wfType string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.upserted[wfType]
}

func (c *scheduleVisCounters) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.deleted
	for _, m := range []map[string]int{c.started, c.closed, c.upserted} {
		for _, v := range m {
			t += v
		}
	}
	return t
}

// summary renders a human-readable breakdown. "schedule" rows are writes for
// the scheduler entity itself; "action wf" rows are writes for the workflows
// the schedule starts.
func (c *scheduleVisCounters) summary(prefix string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	classify := func(wfType string) string {
		if wfType == c.actionWFType {
			return "action wf (" + wfType + ")"
		}
		if wfType == "" {
			return "(unknown)"
		}
		return "schedule (" + wfType + ")"
	}

	// Aggregate per (class, op).
	type key struct{ class, op string }
	agg := map[key]int{}
	add := func(m map[string]int, op string) {
		for wfType, n := range m {
			agg[key{classify(wfType), op}] += n
		}
	}
	add(c.started, "Started")
	add(c.closed, "Closed")
	add(c.upserted, "Upsert")

	keys := make([]key, 0, len(agg))
	for k := range agg {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].class != keys[j].class {
			return keys[i].class < keys[j].class
		}
		return keys[i].op < keys[j].op
	})

	var out strings.Builder
	out.WriteString(prefix + " visibility writes:")
	for _, k := range keys {
		out.WriteString("\n  " + k.class + " " + k.op + ": " + strconv.Itoa(agg[k]))
	}
	if c.deleted > 0 {
		out.WriteString("\n  Delete: " + strconv.Itoa(c.deleted))
	}
	return out.String()
}

// countingVisibilityManager wraps a VisibilityManager and counts every write
// operation that targets the configured namespace. All read methods and the
// rest of the interface are served by the embedded delegate.
type countingVisibilityManager struct {
	manager.VisibilityManager
	counters *scheduleVisCounters
}

func (m *countingVisibilityManager) RecordWorkflowExecutionStarted(ctx context.Context, request *manager.RecordWorkflowExecutionStartedRequest) error {
	if m.counters.matches(request.NamespaceID.String()) {
		m.counters.recordStarted(request.WorkflowTypeName)
	}
	return m.VisibilityManager.RecordWorkflowExecutionStarted(ctx, request)
}

func (m *countingVisibilityManager) RecordWorkflowExecutionClosed(ctx context.Context, request *manager.RecordWorkflowExecutionClosedRequest) error {
	if m.counters.matches(request.NamespaceID.String()) {
		m.counters.recordClosed(request.WorkflowTypeName)
	}
	return m.VisibilityManager.RecordWorkflowExecutionClosed(ctx, request)
}

func (m *countingVisibilityManager) UpsertWorkflowExecution(ctx context.Context, request *manager.UpsertWorkflowExecutionRequest) error {
	if m.counters.matches(request.NamespaceID.String()) {
		m.counters.recordUpsert(request.WorkflowTypeName)
	}
	return m.VisibilityManager.UpsertWorkflowExecution(ctx, request)
}

func (m *countingVisibilityManager) DeleteWorkflowExecution(ctx context.Context, request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	if m.counters.matches(request.NamespaceID.String()) {
		m.counters.recordDeleted()
	}
	return m.VisibilityManager.DeleteWorkflowExecution(ctx, request)
}

func copyMap(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}
	cp := make(map[string]int, len(m))
	maps.Copy(cp, m)
	return cp
}

// TestScheduleVisibilityWritePausedThrottle demonstrates the FutureActionTimes
// throttle on the workload it targets: a non-firing schedule whose only
// per-tick change is the sliding FutureActionTimes projection.
//
// A paused 1s-interval schedule ticks once per second forever (the always-on
// Generator advances its high water mark to keep projecting future times).
// Without throttling, each tick re-publishes the shifted FutureActionTimes (and
// the derived next-action-time search attribute) to visibility — a steady drip
// of writes that carry no meaningful state change. The throttle collapses them.
//
// We A/B the same workload over a fixed observation window with the refresh
// interval set to 0 (refresh every tick — the pre-optimization behavior) vs 60s
// (throttled), counting only scheduler-entity upserts (empty workflow type).
// Unlike the dense 3-fire benchmark — where nearly every write is a meaningful
// buffered→running→completed transition that the throttle intentionally keeps —
// this paused workload isolates exactly the cosmetic churn the throttle removes.
func TestScheduleVisibilityWritePausedThrottle(t *testing.T) {
	const observe = 6 * time.Second

	everyTick := countPausedScheduleEntityUpserts(t, "every-tick", 0, observe)
	throttled := countPausedScheduleEntityUpserts(t, "throttled", time.Minute, observe)

	t.Logf("\n===== paused-schedule visibility-write throttle (1s interval, observed %s) =====\n  refresh=every-tick: %d scheduler-entity upserts\n  refresh=60s:        %d scheduler-entity upserts\n  (DescribeSchedule stays exact in both; only the throttled list/index projection differs)",
		observe, everyTick, throttled)

	if throttled >= everyTick {
		t.Errorf("throttling should reduce paused-tick visibility writes, got every-tick=%d throttled=%d", everyTick, throttled)
	}
}

// countPausedScheduleEntityUpserts creates a paused 1s-interval CHASM schedule
// with the given FutureActionTimes refresh interval, waits for the initial
// projection, then returns the number of scheduler-entity visibility upserts
// observed over the window (excluding creation-time writes).
func countPausedScheduleEntityUpserts(t *testing.T, label string, refreshInterval, observe time.Duration) int {
	counters := &scheduleVisCounters{}

	tweak := chasmscheduler.DefaultTweakables
	tweak.FutureActionTimesRefreshInterval = refreshInterval
	// Cap the event log small so it saturates within a couple of ticks. Once
	// saturated it holds a constant set of identical "generatorTask executed"
	// entries, so StateSizeBytes (also in the visibility memo) stops growing and
	// the FutureActionTimes refresh interval becomes the only per-tick variable
	// distinguishing the two runs. This mirrors a long-lived schedule's
	// steady state without waiting out the default 30-entry log.
	tweak.EventLogMaxEntries = 2

	s := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"}),
		testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweak),
		testcore.WithFxOptions(primitives.HistoryService,
			fx.Decorate(func(base manager.VisibilityManager) manager.VisibilityManager {
				return &countingVisibilityManager{VisibilityManager: base, counters: counters}
			}),
		),
	)
	counters.setTarget(s.NamespaceID().String())

	sid := testcore.RandomizeStr("sched-vis-paused-" + label)
	wid := testcore.RandomizeStr("sched-vis-paused-wf-" + label)
	wt := testcore.RandomizeStr("sched-vis-paused-wt-" + label)

	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	ctx := chasmContextFactory(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec:  &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Second)}}},
			State: &schedulepb.ScheduleState{Paused: true},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	// Wait until the initial projection is published, then measure steady-state
	// churn over the observation window (the scheduler entity has an empty
	// workflow type in visibility writes).
	getScheduleEntryFromVisibility(s, sid, chasmContextFactory, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.FutureActionTimes) > 0
	})
	start := counters.upsertCount("")
	time.Sleep(observe)
	return counters.upsertCount("") - start
}
