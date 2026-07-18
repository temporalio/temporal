package schedulertest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
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

	ref     chasm.ComponentRef
	execKey chasm.ExecutionKey

	// dispatchedSideEffects tracks physical side-effect tasks already executed,
	// keyed by pointer identity, since MockNodeBackend accumulates tasks append-only.
	dispatchedSideEffects map[*tasks.ChasmTask]struct{}

	// AfterStep, if set, is called after every step's CloseTransaction with the
	// driver, so invariant checks can observe each settled state.
	AfterStep func(d *Driver)
}

type driverConfig struct {
	schedule  *schedulepb.Schedule
	startTime time.Time
	afterStep func(d *Driver)
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
	// Default happy-path stubs; tests can layer more specific expectations.
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil).AnyTimes()
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
	if err := registry.Register(NewTestLibrary(DefaultConfig(), logger, specProcessor, frontend, history)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	// Scheduler components read namespace-scoped Tweakables via the CHASM context,
	// so each execution's backend must answer GetNamespaceEntry. The transition
	// counter is held constant (matching helper_test.go); the chasmtest engine's
	// default incrementing counter prevents re-armed pure tasks from materializing.
	tv := testvars.New(t)
	engine := chasmtest.NewEngine(t, registry,
		chasmtest.WithTimeSource(ts),
		chasmtest.WithNodeBackendDecorator(func(b *chasm.MockNodeBackend) {
			b.HandleGetNamespaceEntry = tv.Namespace
			b.HandleNextTransitionCount = func() int64 { return 2 }
			b.HandleCurrentVersionedTransition = func() *persistencespb.VersionedTransition {
				return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
			}
		}),
	)

	d := &Driver{
		T:                     t,
		Engine:                engine,
		Registry:              registry,
		TimeSource:            ts,
		Logger:                logger,
		Frontend:              frontend,
		History:               history,
		dispatchedSideEffects: make(map[*tasks.ChasmTask]struct{}),
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

// nextFireTime returns the earliest time at which any pending task (pure or
// side-effect) is due, and whether any task is pending at all.
func (d *Driver) nextFireTime() (time.Time, bool) {
	backend := d.backend()

	var next time.Time
	have := false
	consider := func(t time.Time) {
		if !have || t.Before(next) {
			next = t
			have = true
		}
	}

	// Earliest pending pure task: recorded by closeTransactionGeneratePhysicalPureTask
	// via DeleteCHASMPureTasks(earliestPureTaskTime), or MaximumKey.FireTime if none.
	if pureTime := backend.LastDeletePureTaskCall(); !pureTime.Equal(maxKeyFireTime) {
		consider(pureTime)
	}

	// Earliest undispatched side-effect task.
	for _, t := range d.pendingSideEffectTasks() {
		consider(t.GetVisibilityTime())
	}

	return next, have
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
// that time. It returns false if nothing was pending (the schedule is quiescent).
func (d *Driver) Step() bool {
	d.T.Helper()

	fireTime, have := d.nextFireTime()
	if !have {
		return false
	}

	// Never move the clock backward; immediate tasks (zero scheduled time) fire now.
	now := d.Now()
	if fireTime.Before(now) {
		fireTime = now
	}
	d.TimeSource.Update(fireTime)

	node := d.node()

	// Fire all pure tasks due at fireTime, then commit.
	pureCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	err := node.EachPureTask(fireTime, func(executor chasm.NodePureTask, attrs chasm.TaskAttributes, taskInstance any) (bool, error) {
		return executor.ExecutePureTask(pureCtx, attrs, taskInstance)
	})
	if err != nil {
		d.T.Fatalf("EachPureTask failed: %v", err)
	}
	if _, err := node.CloseTransaction(); err != nil {
		d.T.Fatalf("CloseTransaction after pure tasks failed: %v", err)
	}

	// Fire side-effect tasks now due (visibility <= fireTime), each exactly once.
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	for _, ct := range d.pendingSideEffectTasks() {
		if ct.GetVisibilityTime().After(fireTime) {
			continue
		}
		d.dispatchedSideEffects[ct] = struct{}{}
		execErr := node.ExecuteSideEffectTask(
			engineCtx,
			d.execKey,
			ct,
			func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil },
		)
		if execErr != nil {
			// A NotFound means the logical task was superseded/removed before
			// dispatch (the backend accumulates physical tasks append-only, so a
			// stale one can linger). Production drops these; so do we.
			var notFound *serviceerror.NotFound
			if errors.As(execErr, &notFound) {
				continue
			}
			d.T.Fatalf("ExecuteSideEffectTask failed: %v", execErr)
		}
	}
	if _, err := node.CloseTransaction(); err != nil {
		d.T.Fatalf("CloseTransaction after side-effect tasks failed: %v", err)
	}

	if d.AfterStep != nil {
		d.AfterStep(d)
	}
	return true
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
}

// Snapshot reads current scheduler state into a Snapshot.
func (d *Driver) Snapshot() Snapshot {
	d.T.Helper()
	snap := Snapshot{
		Now:            d.Now(),
		HasPendingTask: d.HasPendingTask(),
	}
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		snap.Closed = s.Closed
		snap.IsSentinel = s.IsSentinel()
		snap.Paused = s.Schedule.GetState().GetPaused()
		snap.BackfillerCount = len(s.Backfillers)
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
			if inv := s.Invoker.Get(ctx); inv != nil && inv.LastProcessedTime != nil {
				snap.InvokerLPT = inv.LastProcessedTime.AsTime()
			}
		}
	})
	return snap
}
