package schedulertest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxKeyFireTime is the sentinel returned by MockNodeBackend.LastDeletePureTaskCall
// when the tree has no pending pure task. It matches the value passed by
// Node.closeTransactionGeneratePhysicalPureTask in that case.
var maxKeyFireTime = tasks.MaximumKey.FireTime

// Driver builds a real CHASM scheduler on top of a [chasmtest.Engine] and steps
// it forward through every task it schedules for itself, advancing a controllable
// clock. After each settled transition it invokes AfterStep (if set), which is
// where invariant assertions are plugged in.
//
// A "step" fires every pure task due at the current reference time (via
// [chasm.Node.EachPureTask]) and every side-effect task whose visibility time has
// arrived (via [chasm.Node.ExecuteSideEffectTask]), then advances the clock to the
// next scheduled task. A healthy interval schedule never truly quiesces (it always
// re-arms a generator task), so callers bound the run with maxSteps; quiescence is
// reached only for paused/exhausted/closed schedules.
type Driver struct {
	T          *testing.T
	Engine     *chasmtest.Engine
	Registry   *chasm.Registry
	TimeSource *clock.EventTimeSource
	Logger     log.Logger

	Frontend *workflowservicemock.MockWorkflowServiceClient
	History  *historyservicemock.MockHistoryServiceClient

	// Config is the scheduler config the library was wired with. Snapshot reads
	// Tweakables (MaxBufferSize, MaxActionsPerExecution) from it so the
	// buffer-bound and action-budget invariants can be evaluated.
	Config *scheduler.Config

	ref     chasm.ComponentRef
	execKey chasm.ExecutionKey

	// dispatchedSideEffects tracks physical side-effect tasks already executed,
	// keyed by pointer identity, since MockNodeBackend accumulates tasks append-only.
	dispatchedSideEffects map[*tasks.ChasmTask]struct{}

	// actionRPCs points at the running count of action RPCs (StartWorkflowExecution
	// + TerminateWorkflowExecution + RequestCancelWorkflowExecution) made through
	// the mock clients. It is allocated before the clients so the stub closures can
	// increment it. maxActionsObserved is the high-water mark of action RPCs made by
	// any single side-effect task dispatch (one InvokerExecuteTask), which backs the
	// action-budget invariant.
	actionRPCs         *int
	maxActionsObserved int

	// startRequests records the RequestId of every StartWorkflowExecution RPC, and
	// metricsCapture the scheduler's metrics; together they back the start ledger.
	startRequests  map[string]struct{}
	metricsCapture *metricstest.Capture
	// rpcMu guards actionRPCs and startRequests: InvokerExecuteTask issues
	// starts from concurrent goroutines.
	rpcMu *sync.Mutex

	// maxSideEffectLag caps how far StepPure may move the clock past the oldest
	// undispatched side-effect task. Zero means unbounded.
	maxSideEffectLag time.Duration

	// maxRunDuration auto-completes a started workflow before the clock moves
	// past its StartTime plus this. Zero means workflows only complete through
	// CompleteWorkflow.
	maxRunDuration time.Duration

	// AfterStep, if set, is called after every step's CloseTransaction with the
	// driver, so invariant checks can observe each settled state.
	AfterStep func(d *Driver)
}

type driverConfig struct {
	schedule  *schedulepb.Schedule
	startTime time.Time
	afterStep func(d *Driver)
	config    *scheduler.Config
	maxLag    time.Duration
	maxRun    time.Duration
}

// DriverOption configures a Driver at construction.
type DriverOption func(*driverConfig)

// WithSchedule overrides the schedule definition (default: DefaultSchedule).
func WithSchedule(s *schedulepb.Schedule) DriverOption {
	return func(c *driverConfig) { c.schedule = s }
}

// WithStartTime sets the wall-clock instant the engine's clock starts at.
func WithStartTime(t time.Time) DriverOption {
	return func(c *driverConfig) { c.startTime = t }
}

// WithAfterStep installs a hook called after every step.
func WithAfterStep(fn func(d *Driver)) DriverOption {
	return func(c *driverConfig) { c.afterStep = fn }
}

// WithConfig overrides the scheduler config (default: DefaultConfig). Use it to
// set small Tweakables — e.g. MaxBufferSize or MaxActionsPerExecution — so the
// buffer-bound and action-budget invariants are meaningfully exercised.
func WithConfig(cfg *scheduler.Config) DriverOption {
	return func(c *driverConfig) { c.config = cfg }
}

// WithMaxSideEffectLag bounds how long StepPure may leave a side-effect task
// (e.g. InvokerExecuteTask) undispatched while moving the clock forward.
func WithMaxSideEffectLag(lag time.Duration) DriverOption {
	return func(c *driverConfig) { c.maxLag = lag }
}

// WithMaxRunDuration bounds how long a started workflow runs: the driver
// completes it before moving the clock past StartTime plus d.
func WithMaxRunDuration(d time.Duration) DriverOption {
	return func(c *driverConfig) { c.maxRun = d }
}

// NewDriver constructs a Driver and starts a scheduler execution via the
// production CreateScheduler path. startTime defaults to a fixed instant for
// determinism.
func NewDriver(t *testing.T, opts ...DriverOption) *Driver {
	t.Helper()
	d, cfg := newDriverScaffold(t, opts...)

	req := &schedulerpb.CreateScheduleRequest{
		NamespaceId: NamespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  Namespace,
			ScheduleId: ScheduleID,
			Schedule:   cfg.schedule,
			RequestId:  "create-req",
		},
	}
	d.startExecution(func(mctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.CreateScheduler(mctx, req)
	})
	return d
}

// NewDriverFromMigration constructs a Driver and starts a scheduler execution
// from a migrated V1 state via the production CreateSchedulerFromMigration path.
// This is the V1->V2 import side of a migration round trip.
func NewDriverFromMigration(t *testing.T, req *schedulerpb.CreateFromMigrationStateRequest, opts ...DriverOption) *Driver {
	t.Helper()
	d, _ := newDriverScaffold(t, opts...)
	d.startExecution(func(mctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.CreateSchedulerFromMigration(mctx, req)
	})
	return d
}

// newDriverScaffold builds the engine, registry, clients, and time source, but
// does not start an execution.
func newDriverScaffold(t *testing.T, opts ...DriverOption) (*Driver, *driverConfig) {
	t.Helper()

	cfg := &driverConfig{
		schedule:  DefaultSchedule(),
		startTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		config:    DefaultConfig(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	ts := clock.NewEventTimeSource()
	ts.Update(cfg.startTime)

	frontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)

	// actionRPCs counts every action RPC (start/terminate/cancel) the invoker
	// makes through the mock clients, so Step can measure how many actions a single
	// ExecuteTask takes (the action-budget invariant).
	actionRPCs := new(int)
	startRequests := make(map[string]struct{})
	rpcMu := &sync.Mutex{}
	countAction := func() {
		rpcMu.Lock()
		defer rpcMu.Unlock()
		*actionRPCs++
	}

	// Default happy-path stubs; tests can layer more specific expectations.
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			countAction()
			rpcMu.Lock()
			startRequests[req.GetRequestId()] = struct{}{}
			rpcMu.Unlock()
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil
		}).AnyTimes()
	history.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.TerminateWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			countAction()
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		}).AnyTimes()
	history.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.RequestCancelWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			countAction()
			return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
		}).AnyTimes()
	// The callbacks task (used for migrated running workflows) describes each
	// running workflow; report it completed so watchers resolve deterministically.
	history.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *historyservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					CloseTime: timestamppb.New(ts.Now()),
				},
			}, nil
		}).AnyTimes()

	specProcessor := NewRealSpecProcessor(ctrl, logger)

	registry := chasm.NewRegistry(logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	if err := registry.Register(NewTestLibrary(cfg.config, logger, metricsHandler, ts, specProcessor, frontend, history)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))

	d := &Driver{
		T:                     t,
		Engine:                engine,
		Registry:              registry,
		TimeSource:            ts,
		Logger:                logger,
		Frontend:              frontend,
		History:               history,
		Config:                cfg.config,
		dispatchedSideEffects: make(map[*tasks.ChasmTask]struct{}),
		actionRPCs:            actionRPCs,
		startRequests:         startRequests,
		rpcMu:                 rpcMu,
		metricsCapture:        capture,
		maxSideEffectLag:      cfg.maxLag,
		maxRunDuration:        cfg.maxRun,
		AfterStep:             cfg.afterStep,
	}
	return d, cfg
}

// startExecution starts the scheduler execution using the given start function.
func (d *Driver) startExecution(startFn func(chasm.MutableContext) (*scheduler.Scheduler, error)) {
	d.T.Helper()

	d.execKey = chasm.ExecutionKey{NamespaceID: NamespaceID, BusinessID: ScheduleID}
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)

	_, err := chasm.StartExecution(engineCtx, d.execKey, func(mctx chasm.MutableContext, _ *struct{}) (*scheduler.Scheduler, error) {
		return startFn(mctx)
	}, (*struct{})(nil), chasm.WithRequestID("create-req"))
	if err != nil {
		d.T.Fatalf("failed to start scheduler execution: %v", err)
	}

	d.ref = chasm.NewComponentRef[*scheduler.Scheduler](d.execKey)
}

// Now returns the engine's current clock time.
func (d *Driver) Now() time.Time {
	return d.TimeSource.Now()
}

// ReadScheduler reads the scheduler root component and invokes fn with it.
func (d *Driver) ReadScheduler(fn func(s *scheduler.Scheduler, ctx chasm.Context)) {
	d.T.Helper()
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	err := d.Engine.ReadComponent(engineCtx, d.ref, func(ctx chasm.Context, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			d.T.Fatalf("root component is %T, want *scheduler.Scheduler", c)
		}
		fn(s, ctx)
		return nil
	})
	if err != nil {
		d.T.Fatalf("ReadScheduler failed: %v", err)
	}
}

// node returns the CHASM tree node backing the scheduler execution.
func (d *Driver) node() *chasm.Node {
	d.T.Helper()
	node, err := d.Engine.NodeForRef(d.ref)
	if err != nil {
		d.T.Fatalf("NodeForRef failed: %v", err)
	}
	return node
}

func (d *Driver) backend() *chasm.MockNodeBackend {
	d.T.Helper()
	backend, err := d.Engine.BackendForRef(d.ref)
	if err != nil {
		d.T.Fatalf("BackendForRef failed: %v", err)
	}
	return backend
}

// nextPureFireTime returns the earliest pending pure task time, if any. It is
// recorded by closeTransactionGeneratePhysicalPureTask via
// DeleteCHASMPureTasks(earliestPureTaskTime), or MaximumKey.FireTime if none.
func (d *Driver) nextPureFireTime() (time.Time, bool) {
	pureTime := d.backend().LastDeletePureTaskCall()
	if pureTime.IsZero() || pureTime.Equal(maxKeyFireTime) {
		return time.Time{}, false
	}
	return pureTime, true
}

// oldestSideEffectTime returns the earliest visibility time among undispatched
// side-effect tasks, if any.
func (d *Driver) oldestSideEffectTime() (time.Time, bool) {
	var oldest time.Time
	have := false
	for _, t := range d.pendingSideEffectTasks() {
		if vt := t.GetVisibilityTime(); !have || vt.Before(oldest) {
			oldest, have = vt, true
		}
	}
	return oldest, have
}

// nextFireTime returns the earliest time at which any pending task (pure or
// side-effect) is due, and whether any task is pending at all.
func (d *Driver) nextFireTime() (time.Time, bool) {
	pure, havePure := d.nextPureFireTime()
	side, haveSide := d.oldestSideEffectTime()
	switch {
	case havePure && haveSide:
		if side.Before(pure) {
			return side, true
		}
		return pure, true
	case havePure:
		return pure, true
	default:
		return side, haveSide
	}
}

// pendingSideEffectTasks returns physical side-effect tasks that have not yet
// been dispatched by this driver.
func (d *Driver) pendingSideEffectTasks() []*tasks.ChasmTask {
	backend := d.backend()
	var out []*tasks.ChasmTask
	for category, categoryTasks := range backend.TasksByCategory {
		// Visibility tasks are framework-managed (indexing search attributes) and
		// processed by a separate queue, not the CHASM side-effect executor; their
		// handler panics if invoked directly. Skip them.
		if category.ID() == tasks.CategoryVisibility.ID() {
			continue
		}
		for _, t := range categoryTasks {
			ct, ok := t.(*tasks.ChasmTask)
			if !ok {
				continue
			}
			if _, done := d.dispatchedSideEffects[ct]; done {
				continue
			}
			out = append(out, ct)
		}
	}
	return out
}

// Step advances the clock to the next scheduled task and fires all tasks due at
// that time: pure tasks first, then every due side-effect task. It returns false
// if nothing was pending (the schedule is quiescent).
func (d *Driver) Step() bool {
	d.T.Helper()

	fireTime, have := d.nextFireTime()
	if !have {
		return false
	}
	d.advanceTo(fireTime)
	d.firePureTasks()
	for d.StepSideEffect() {
	}
	return true
}

// StepPure advances the clock to the next pending pure task and fires every pure
// task due then, leaving side-effect tasks undispatched. This models
// InvokerExecuteTask lagging behind the scheduler's timers. It returns false if
// no pure task is pending, or if firing it would move the clock more than
// maxSideEffectLag past the oldest undispatched side-effect task.
func (d *Driver) StepPure() bool {
	d.T.Helper()

	fireTime, have := d.nextPureFireTime()
	if !have {
		return false
	}
	if oldest, ok := d.oldestSideEffectTime(); ok && d.maxSideEffectLag > 0 &&
		fireTime.After(oldest.Add(d.maxSideEffectLag)) {
		return false
	}
	d.advanceTo(fireTime)
	d.firePureTasks()
	return true
}

// StepSideEffect dispatches the oldest side-effect task that is due at the
// current time, without moving the clock. It returns false if none is due.
func (d *Driver) StepSideEffect() bool {
	d.T.Helper()

	now := d.Now()
	var next *tasks.ChasmTask
	for _, ct := range d.pendingSideEffectTasks() {
		if ct.GetVisibilityTime().After(now) {
			continue
		}
		if next == nil || ct.GetVisibilityTime().Before(next.GetVisibilityTime()) {
			next = ct
		}
	}
	if next == nil {
		return false
	}

	d.dispatchedSideEffects[next] = struct{}{}
	actionsBefore := d.actionCount()
	execErr := d.node().ExecuteSideEffectTask(
		chasm.NewEngineContext(context.Background(), d.Engine),
		d.execKey,
		next,
		func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil },
	)
	if execErr != nil {
		// A NotFound means the logical task was superseded/removed before
		// dispatch (the backend accumulates physical tasks append-only, so a
		// stale one can linger). Production drops these; so do we.
		var notFound *serviceerror.NotFound
		if !errors.As(execErr, &notFound) {
			d.T.Fatalf("ExecuteSideEffectTask failed: %v", execErr)
		}
	}
	// Track the most actions any single side-effect task (i.e. one
	// InvokerExecuteTask) took, for the action-budget invariant.
	if taken := d.actionCount() - actionsBefore; taken > d.maxActionsObserved {
		d.maxActionsObserved = taken
	}
	if err := d.Engine.CloseTransaction(context.Background(), d.ref); err != nil {
		d.T.Fatalf("CloseTransaction after side-effect task failed: %v", err)
	}
	if d.AfterStep != nil {
		d.AfterStep(d)
	}
	return true
}

// advanceTo moves the clock to t, never backward, first completing any
// workflow that would exceed maxRunDuration on the way.
func (d *Driver) advanceTo(t time.Time) {
	for d.maxRunDuration > 0 {
		startTime, ok := d.oldestRunningStartTime()
		if !ok {
			break
		}
		due := startTime.Add(d.maxRunDuration)
		if due.After(t) {
			break
		}
		if due.After(d.Now()) {
			d.TimeSource.Update(due)
		}
		d.CompleteWorkflow()
		// The completion may ready a deferred start; don't skip its execute task.
		for d.StepSideEffect() {
		}
	}
	if t.After(d.Now()) {
		d.TimeSource.Update(t)
	}
}

// oldestRunningStartTime returns the StartTime of the oldest started workflow
// that hasn't completed.
func (d *Driver) oldestRunningStartTime() (time.Time, bool) {
	var oldest time.Time
	have := false
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		if s.Closed || s.IsSentinel() {
			return
		}
		for _, start := range s.Invoker.Get(ctx).GetBufferedStarts() {
			if start.GetRunId() == "" || start.GetCompleted() != nil {
				continue
			}
			if st := start.GetStartTime().AsTime(); !have || st.Before(oldest) {
				oldest, have = st, true
			}
		}
	})
	return oldest, have
}

// CompleteWorkflow delivers a successful completion callback for the oldest
// running workflow, through Scheduler.HandleNexusCompletion. It returns false
// if no workflow is running.
func (d *Driver) CompleteWorkflow() bool {
	d.T.Helper()
	completed := false
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		if s.Closed || s.IsSentinel() {
			return nil
		}
		var oldest *schedulespb.BufferedStart
		for _, start := range s.Invoker.Get(mctx).GetBufferedStarts() {
			if start.GetRunId() == "" || start.GetCompleted() != nil {
				continue
			}
			if oldest == nil || start.GetStartTime().AsTime().Before(oldest.GetStartTime().AsTime()) {
				oldest = start
			}
		}
		if oldest == nil {
			return nil
		}
		completed = true
		return s.HandleNexusCompletion(mctx, &persistencespb.ChasmNexusCompletion{
			RequestId: oldest.GetRequestId(),
			Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
			CloseTime: timestamppb.New(d.Now()),
		})
	})
	if err != nil {
		d.T.Fatalf("CompleteWorkflow failed: %v", err)
	}
	if completed && d.AfterStep != nil {
		d.AfterStep(d)
	}
	return completed
}

// firePureTasks fires every pure task due at the current time and commits.
func (d *Driver) firePureTasks() {
	d.T.Helper()
	now := d.Now()
	pureCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	err := d.node().EachPureTask(now, func(executor chasm.NodePureTask, attrs chasm.TaskAttributes, taskInstance any) (bool, error) {
		return executor.ExecutePureTask(pureCtx, attrs, taskInstance)
	})
	if err != nil {
		d.T.Fatalf("EachPureTask failed: %v", err)
	}
	if err := d.Engine.CloseTransaction(context.Background(), d.ref); err != nil {
		d.T.Fatalf("CloseTransaction after pure tasks failed: %v", err)
	}
	if d.AfterStep != nil {
		d.AfterStep(d)
	}
}

// RunToQuiescence steps until no task is pending or maxSteps is reached. It
// returns the number of steps taken and whether the schedule reached quiescence
// (no pending task). A normal running schedule never quiesces and will stop at
// maxSteps with quiescent == false.
func (d *Driver) RunToQuiescence(maxSteps int) (steps int, quiescent bool) {
	d.T.Helper()
	for steps = 0; steps < maxSteps; steps++ {
		if !d.Step() {
			return steps, true
		}
	}
	return steps, false
}

// HasPendingTask reports whether any task (pure or side-effect) is currently
// scheduled to fire.
func (d *Driver) HasPendingTask() bool {
	_, have := d.nextFireTime()
	return have
}

// Patch applies a SchedulePatch through the production Scheduler.Patch path in a
// single transaction. It returns the scheduler's error (e.g. ErrClosed when the
// schedule has already closed) without failing the test, so callers driving
// random operations can tolerate rejected patches. On success the AfterStep hook
// runs so invariants are checked at the new settled state.
func (d *Driver) Patch(patch *schedulepb.SchedulePatch) error {
	d.T.Helper()
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		_, perr := s.Patch(mctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: NamespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  Namespace,
				ScheduleId: ScheduleID,
				Patch:      patch,
			},
		})
		return perr
	})
	if err == nil && d.AfterStep != nil {
		d.AfterStep(d)
	}
	return err
}

// Pause pauses the schedule via Patch.
func (d *Driver) Pause() error {
	return d.Patch(&schedulepb.SchedulePatch{Pause: "paused by driver"})
}

// Unpause unpauses the schedule via Patch.
func (d *Driver) Unpause() error {
	return d.Patch(&schedulepb.SchedulePatch{Unpause: "unpaused by driver"})
}

// TriggerImmediately requests an immediate action via Patch.
func (d *Driver) TriggerImmediately() error {
	return d.Patch(&schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	})
}

// MigrateToWorkflow initiates a V2->V1 migration: it pauses the schedule and
// schedules the SchedulerMigrateToWorkflowTask side-effect task. Step the driver
// afterward to fire the task (which calls the history client's
// StartWorkflowExecution with the exported StartScheduleArgs).
func (d *Driver) MigrateToWorkflow() error {
	d.T.Helper()
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		_, merr := s.MigrateToWorkflow(mctx, &schedulerpb.MigrateToWorkflowRequest{
			NamespaceId: NamespaceID,
			ScheduleId:  ScheduleID,
		})
		return merr
	})
	return err
}

// Snapshot captures the observable scheduler state at a settled point (after a
// step's CloseTransaction). It is the input to invariant checks.
type Snapshot struct {
	// Now is the engine clock at capture time.
	Now time.Time
	// HasPendingTask is true if any task is scheduled to fire.
	HasPendingTask bool

	Closed     bool
	IsSentinel bool
	Paused     bool
	// BackfillerCount is the number of live backfiller components.
	BackfillerCount int
	// IsHeldOpen mirrors Scheduler.isHeldOpen: the schedule must stay open
	// regardless of having no work (paused or pending backfill, non-sentinel).
	IsHeldOpen bool
	// IdleCloseTime is the armed idle-close deadline, or zero if no idle task is armed.
	IdleCloseTime time.Time

	// GeneratorLPT / InvokerLPT are the generator and invoker high-water marks.
	GeneratorLPT time.Time
	InvokerLPT   time.Time

	// BufferedStartsCount is len(invoker.GetBufferedStarts()) — the total buffered
	// starts (pending + retained). MaxBufferSize is the configured cap; the
	// buffer-bound invariant requires BufferedStartsCount <= MaxBufferSize.
	BufferedStartsCount int
	MaxBufferSize       int

	// MaxActionsObserved is the high-water mark of action RPCs (start + terminate +
	// cancel) taken by any single InvokerExecuteTask so far. MaxActionsPerExecution
	// is the configured per-execution cap; the action-budget invariant requires
	// MaxActionsObserved <= MaxActionsPerExecution.
	MaxActionsObserved     int
	MaxActionsPerExecution int

	// Start ledger inputs. PendingStarts are buffered starts with no RunId yet.
	// StartRequested holds every RequestId sent to StartWorkflowExecution.
	// OverlapSkipped and DroppedStarts count the starts the scheduler discarded
	// on purpose (Info.OverlapSkipped, and the ScheduleBufferedStartDropped
	// metric for missed-catchup and paused/limited drops).
	PendingStarts  map[string]struct{}
	StartRequested map[string]struct{}
	OverlapSkipped int64
	DroppedStarts  int64
	// Migrating is true once the schedule hands off to a V1 workflow, which
	// takes its buffered starts with it.
	Migrating bool
	// DeferredStarts are pending starts waiting on a workflow completion
	// (Attempt == -1); RunningWorkflows counts started, uncompleted workflows.
	DeferredStarts   int
	RunningWorkflows int
}

// Snapshot reads current scheduler state into a Snapshot.
func (d *Driver) Snapshot() Snapshot {
	d.T.Helper()
	tweakables := d.Config.Tweakables(Namespace)
	snap := Snapshot{
		Now:                    d.Now(),
		HasPendingTask:         d.HasPendingTask(),
		MaxBufferSize:          tweakables.MaxBufferSize,
		MaxActionsObserved:     d.maxActionsObserved,
		MaxActionsPerExecution: tweakables.MaxActionsPerExecution,
		PendingStarts:          make(map[string]struct{}),
		StartRequested:         d.startRequestsCopy(),
		DroppedStarts:          d.droppedStarts(),
	}
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		snap.Closed = s.Closed
		snap.IsSentinel = s.IsSentinel()
		snap.Paused = s.Schedule.GetState().GetPaused()
		snap.BackfillerCount = len(s.Backfillers)
		snap.OverlapSkipped = s.Info.GetOverlapSkipped()
		snap.Migrating = s.GetWorkflowMigration() != nil
		// Mirror Scheduler.isHeldOpen() from public state.
		snap.IsHeldOpen = !snap.IsSentinel && (snap.Paused || snap.BackfillerCount > 0)
		if s.IdleCloseTime != nil {
			snap.IdleCloseTime = s.IdleCloseTime.AsTime()
		}
		// Sentinels have no generator/invoker sub-components.
		if !snap.IsSentinel {
			if g := s.Generator.Get(ctx); g != nil && g.LastProcessedTime != nil {
				snap.GeneratorLPT = g.LastProcessedTime.AsTime()
			}
			if inv := s.Invoker.Get(ctx); inv != nil {
				snap.BufferedStartsCount = len(inv.GetBufferedStarts())
				snap.addBufferedStarts(inv.GetBufferedStarts())
				if inv.LastProcessedTime != nil {
					snap.InvokerLPT = inv.LastProcessedTime.AsTime()
				}
			}
		}
	})
	return snap
}

// addBufferedStarts classifies buffered starts into pending (deferred or not)
// and running.
func (snap *Snapshot) addBufferedStarts(starts []*schedulespb.BufferedStart) {
	for _, start := range starts {
		switch {
		case start.GetRunId() == "":
			snap.PendingStarts[start.GetRequestId()] = struct{}{}
			if start.GetAttempt() == -1 {
				snap.DeferredStarts++
			}
		case start.GetCompleted() == nil:
			snap.RunningWorkflows++
		default:
		}
	}
}

// droppedStarts sums the ScheduleBufferedStartDropped counter.
func (d *Driver) droppedStarts() int64 {
	var total int64
	for _, rec := range d.metricsCapture.SnapshotMetric(metrics.ScheduleBufferedStartDropped.Name()) {
		if v, ok := rec.Value.(int64); ok {
			total += v
		}
	}
	return total
}

func (d *Driver) actionCount() int {
	d.rpcMu.Lock()
	defer d.rpcMu.Unlock()
	return *d.actionRPCs
}

func (d *Driver) startRequestsCopy() map[string]struct{} {
	d.rpcMu.Lock()
	defer d.rpcMu.Unlock()
	return maps.Clone(d.startRequests)
}
