package scheduler_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	namespace   = "ns"
	namespaceID = "ns-id"
	scheduleID  = "sched-id"

	defaultInterval      = 1 * time.Minute
	defaultCatchupWindow = 5 * time.Minute
)

// newLegacySpecBuilder builds a legacy SpecBuilder with the given warn/max compute-limit bounds.
// A value of 0 means "use the default" (GetNextTime treats a non-positive bound as its default).
func newLegacySpecBuilder(warnIter, maxIter int) *legacyscheduler.SpecBuilder {
	return legacyscheduler.NewSpecBuilder(func() int { return warnIter }, func() int { return maxIter })
}

// defaultSchedule returns a protobuf definition for a schedule matching this
// package's other testing defaults.
func defaultSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(defaultInterval),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "scheduled-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			CatchupWindow: durationpb.New(defaultCatchupWindow),
		},
		State: &schedulepb.ScheduleState{
			Paused:           false,
			LimitedActions:   false,
			RemainingActions: 0,
		},
	}
}

func defaultConfig() *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			return scheduler.DefaultTweakables
		},
		ServiceCallTimeout: func() time.Duration {
			return 5 * time.Second
		},
		EncodeInternalTokenWithEnvelope: func(string) bool {
			return true
		},
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(1 * time.Second)
		},
	}
}

func newTestLibrary(logger log.Logger, specProcessor scheduler.SpecProcessor) *scheduler.Library {
	config := defaultConfig()
	specBuilder := newLegacySpecBuilder(0, 0)
	invokerOpts := scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	}
	return scheduler.NewLibrary(
		config,
		nil,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
			SpecBuilder:    specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOpts),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		}),
	)
}

// testEnv holds a scheduler execution on a CHASM test engine, with direct
// access to its node, for tests that mutate the scheduler in an open
// transaction and commit it with CloseTransaction.
type testEnv struct {
	t             *testing.T
	Ctrl          *gomock.Controller
	Registry      *chasm.Registry
	Node          *chasm.Node
	NodeBackend   *chasm.MockNodeBackend
	TimeSource    *clock.EventTimeSource
	Scheduler     *scheduler.Scheduler
	SpecProcessor scheduler.SpecProcessor
	Logger        log.Logger

	// engine owns the execution backing Node; rootRef identifies it.
	engine  *chasmtest.Engine
	rootRef chasm.ComponentRef

	// allowStuckReason, when non-empty, suppresses the stuckness invariant
	// asserted by CloseTransaction. See AllowStuck.
	allowStuckReason string
}

// testEnvConfig holds configuration options for testEnv.
type testEnvConfig struct {
	specProcessor scheduler.SpecProcessor
	schedule      *schedulepb.Schedule
}

// testEnvOption is a functional option for configuring testEnv.
type testEnvOption func(*testEnvConfig)

// withSpecProcessor configures testEnv with a custom SpecProcessor.
// By default, testEnv uses a real SpecProcessor. Use this option only
// when you need to mock specific SpecProcessor behavior (e.g., simulating failures).
func withSpecProcessor(sp scheduler.SpecProcessor) testEnvOption {
	return func(c *testEnvConfig) {
		c.specProcessor = sp
	}
}

// withSchedule overrides defaultSchedule(), for tests that need a spec other
// than the package default 1-minute interval (e.g. an exhausted spec, which is
// what drives the Generator into its idle branch).
func withSchedule(schedule *schedulepb.Schedule) testEnvOption {
	return func(c *testEnvConfig) {
		c.schedule = schedule
	}
}

// expiredSchedule returns a schedule whose spec has already ended, so the
// Generator finds no next wakeup and takes its idle branch. This is the shape
// of a real schedule that has run to the end of its subscription window.
func expiredSchedule(now time.Time) *schedulepb.Schedule {
	schedule := defaultSchedule()
	schedule.Spec.StartTime = timestamppb.New(now.Add(-2 * time.Hour))
	schedule.Spec.EndTime = timestamppb.New(now.Add(-1 * time.Hour))
	return schedule
}

// newRealSpecProcessor creates a real SpecProcessor for tests.
func newRealSpecProcessor(ctrl *gomock.Controller, logger log.Logger) scheduler.SpecProcessor {
	mockMetrics := metrics.NewMockHandler(ctrl)
	mockMetrics.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	mockMetrics.EXPECT().WithTags(gomock.Any()).Return(mockMetrics).AnyTimes()
	mockMetrics.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	return scheduler.NewSpecProcessor(
		defaultConfig(),
		mockMetrics,
		logger,
		newLegacySpecBuilder(0, 0),
	)
}

// engineTestConfig holds configuration options for newTestEngineContext.
type engineTestConfig struct {
	specProcessor scheduler.SpecProcessor
	timeSource    *clock.EventTimeSource
	engineOpts    []chasmtest.EngineOption
}

// engineTestOption is a functional option for configuring newTestEngineContext.
type engineTestOption func(*engineTestConfig)

// withEngineSpecProcessor configures newTestEngineContext with a custom
// SpecProcessor, instead of the default real one.
func withEngineSpecProcessor(sp scheduler.SpecProcessor) engineTestOption {
	return func(c *engineTestConfig) {
		c.specProcessor = sp
	}
}

// withEngineTimeSource configures the CHASM test engine with a controllable
// time source, for tests that need to advance time explicitly.
func withEngineTimeSource(ts *clock.EventTimeSource) engineTestOption {
	return func(c *engineTestConfig) {
		c.timeSource = ts
		c.engineOpts = append(c.engineOpts, chasmtest.WithTimeSource(ts))
	}
}

func withEngineMetricsHandler(handler metrics.Handler) engineTestOption {
	return func(c *engineTestConfig) {
		c.engineOpts = append(c.engineOpts, chasmtest.WithMetricsHandler(handler))
	}
}

func withEngineNamespaceName(name string) engineTestOption {
	return func(c *engineTestConfig) {
		c.engineOpts = append(c.engineOpts, chasmtest.WithNamespaceName(namespaceID, name))
	}
}

func newEngineTestConfig(opts ...engineTestOption) *engineTestConfig {
	config := &engineTestConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// newTestEngineContext builds a CHASM registry with the core and scheduler
// libraries registered, wraps it in a chasmtest.Engine, and returns the
// engine along with an engine-bound context ready for chasm.StartExecution /
// ReadComponent / etc.
func newTestEngineContext(t *testing.T, logger log.Logger, opts ...engineTestOption) (*chasmtest.Engine, context.Context) {
	return newTestEngineContextFromConfig(t, logger, newEngineTestConfig(opts...))
}

func newTestEngineContextFromConfig(
	t *testing.T,
	logger log.Logger,
	config *engineTestConfig,
) (*chasmtest.Engine, context.Context) {
	specProcessor := config.specProcessor
	if specProcessor == nil {
		specProcessor = newRealSpecProcessor(gomock.NewController(t), logger)
	}

	registry := newTestRegistry(t, logger, specProcessor)

	config.engineOpts = append(config.engineOpts, chasmtest.WithInvariantCheck(
		func(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
			requireValidSchedulerState(t, registry, node, root)
		},
	))
	engine := chasmtest.NewEngine(t, registry, config.engineOpts...)
	return engine, chasm.NewEngineContext(context.Background(), engine)
}

// schedulerTestEngine bundles a CHASM test engine, a created Scheduler root
// component, and convenience accessors, for tests that drive a Scheduler
// through its actual task handlers and the engine's update and read paths,
// rather than testEnv's open transaction on the execution's node.
type schedulerTestEngine struct {
	engine     *chasmtest.Engine
	engineCtx  context.Context
	rootRef    chasm.ComponentRef
	logger     log.Logger
	timeSource *clock.EventTimeSource
}

// newSchedulerTestEngine builds a schedulerTestEngine and creates a schedule
// on it via the real CreateSchedule handler path. If no time source is
// supplied via withEngineTimeSource, a controllable one is created and
// wired in automatically.
func newSchedulerTestEngine(
	t *testing.T,
	schedule *schedulepb.Schedule,
	opts ...engineTestOption,
) *schedulerTestEngine {
	t.Helper()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	config := newEngineTestConfig(opts...)
	if config.timeSource == nil {
		config.timeSource = clock.NewEventTimeSource()
		config.timeSource.Update(time.Now())
		config.engineOpts = append(config.engineOpts, chasmtest.WithTimeSource(config.timeSource))
	}
	engine, engineCtx := newTestEngineContextFromConfig(t, logger, config)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})
	_, err := scheduler.NewTestHandler(logger).CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   schedule,
			RequestId:  "create-request",
		},
	})
	require.NoError(t, err)
	return &schedulerTestEngine{
		engine:     engine,
		engineCtx:  engineCtx,
		rootRef:    rootRef,
		logger:     logger,
		timeSource: config.timeSource,
	}
}

// updateScheduler runs update against the Scheduler root component through
// the engine's UpdateComponent path.
func (e *schedulerTestEngine) updateScheduler(
	update func(*scheduler.Scheduler, chasm.MutableContext) error,
) error {
	_, _, err := chasm.UpdateComponent(
		e.engineCtx,
		e.rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, update(s, ctx)
		},
		struct{}{},
	)
	return err
}

// readScheduler runs read against the Scheduler root component through the
// engine's read-only ReadComponent path.
func (e *schedulerTestEngine) readScheduler(
	read func(*scheduler.Scheduler, chasm.Context) error,
) error {
	_, err := chasm.ReadComponent(
		e.engineCtx,
		e.rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, read(s, ctx)
		},
		struct{}{},
	)
	return err
}

// newTestEnv creates a new test environment with the given options.
func newTestEnv(t *testing.T, opts ...testEnvOption) *testEnv {
	config := &testEnvConfig{}
	for _, opt := range opts {
		opt(config)
	}

	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	// Configure spec processor: use custom if provided, otherwise use real.
	var specProcessor scheduler.SpecProcessor
	if config.specProcessor != nil {
		specProcessor = config.specProcessor
	} else {
		specProcessor = newRealSpecProcessor(ctrl, logger)
	}

	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)

	schedule := config.schedule
	if schedule == nil {
		schedule = defaultSchedule()
	}

	env := startTestEnv(t, logger, specProcessor, timeSource, func(ctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)
		if err != nil {
			return nil, err
		}
		// Advance Generator's high water mark to 'now'.
		sched.Generator.Get(ctx).LastProcessedTime = timestamppb.New(now)
		return sched, nil
	})
	env.Ctrl = ctrl

	return env
}

// startTestEnv builds a testEnv around a CHASM test engine and starts an
// execution on it whose root component is built by startFn.
func startTestEnv(
	t *testing.T,
	logger log.Logger,
	specProcessor scheduler.SpecProcessor,
	timeSource *clock.EventTimeSource,
	startFn func(chasm.MutableContext) (*scheduler.Scheduler, error),
) *testEnv {
	env := &testEnv{
		t:             t,
		TimeSource:    timeSource,
		SpecProcessor: specProcessor,
		Logger:        logger,
	}
	env.Registry = newTestRegistry(t, logger, specProcessor)
	env.engine = chasmtest.NewEngine(t, env.Registry,
		chasmtest.WithTimeSource(timeSource),
		chasmtest.WithNamespaceName(namespaceID, namespace),
		chasmtest.WithInvariantCheck(env.checkInvariants),
	)
	env.rootRef = startTestExecution(t, env.engine, func(ctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		sched, err := startFn(ctx)
		env.Scheduler = sched
		return sched, err
	})

	var err error
	if env.Node, err = env.engine.NodeForRef(env.rootRef); err != nil {
		t.Fatalf("failed to get execution node: %v", err)
	}
	if env.NodeBackend, err = env.engine.BackendForRef(env.rootRef); err != nil {
		t.Fatalf("failed to get execution backend: %v", err)
	}
	return env
}

// MutableContext returns a new mutable CHASM context.
func (e *testEnv) MutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), e.Node)
}

// ReadContext returns a new read-only CHASM context.
func (e *testEnv) ReadContext() chasm.Context {
	return chasm.NewContext(context.Background(), e.Node)
}

// CloseTransaction commits the open CHASM transaction through the engine,
// whose invariant check asserts the stuckness invariant: a scheduler that is not in a terminal state must carry
// at least one live logical task, or nothing will ever wake it again.
//
// This runs on every test in the package that closes through testEnv, so the
// existing suite doubles as a stuckness detector at no extra cost. Tests that
// deliberately construct a state with no pending work must opt out explicitly
// via AllowStuck.
//
// The invariant is checked only on a successful close, since Node.Snapshot
// requires a clean tree.
func (e *testEnv) CloseTransaction() error {
	e.t.Helper()
	return e.engine.CloseTransaction(context.Background(), e.rootRef)
}

// checkInvariants is the engine invariant check behind CloseTransaction.
func (e *testEnv) checkInvariants(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
	t.Helper()
	if e.allowStuckReason == "" {
		requireValidSchedulerState(t, e.Registry, node, root)
	}
}

// AllowStuck opts this test out of the stuckness invariant asserted by
// CloseTransaction, for tests that deliberately drive the scheduler into a
// state with no pending work.
//
// reason is mandatory: an unexplained opt-out is indistinguishable from a
// silently tolerated bug, and the set of tests that need one is itself a
// finding worth reviewing.
func (e *testEnv) AllowStuck(reason string) {
	e.t.Helper()
	if reason == "" {
		e.t.Fatal("AllowStuck requires a reason explaining why this test tolerates a stuck scheduler")
	}
	e.allowStuckReason = reason
}

// HasTask returns true if the given task type was added with the given visibilityTime.
func (e *testEnv) HasTask(task any, visibilityTime time.Time) bool {
	taskType := reflect.TypeOf(task)
	for _, categoryTasks := range e.NodeBackend.TasksByCategory {
		for _, t := range categoryTasks {
			if reflect.TypeOf(t) == taskType &&
				t.GetVisibilityTime().Equal(visibilityTime) {
				return true
			}
		}
	}
	return false
}

// HasTaskInCategory is like HasTask but scoped to a single queue category, to
// distinguish tasks that share a physical type but land in different queues.
func (e *testEnv) HasTaskInCategory(task any, category tasks.Category, visibilityTime time.Time) bool {
	taskType := reflect.TypeOf(task)
	for _, t := range e.NodeBackend.TasksByCategory[category] {
		if reflect.TypeOf(t) == taskType &&
			t.GetVisibilityTime().Equal(visibilityTime) {
			return true
		}
	}
	return false
}

// EngineContext returns a context bound to the test engine, for side-effect
// task handlers that read and update components through the engine.
func (e *testEnv) EngineContext() context.Context {
	return chasm.NewEngineContext(context.Background(), e.engine)
}

// ComponentRef returns the engine ref for component, as a task handler
// would receive it.
func (e *testEnv) ComponentRef(ctx chasm.Context, component chasm.Component) chasm.ComponentRef {
	e.t.Helper()
	serialized, err := ctx.Ref(component)
	require.NoError(e.t, err)
	ref, err := chasm.DeserializeComponentRef(serialized)
	require.NoError(e.t, err)
	return ref
}

// newTestRegistry builds a CHASM registry with the core and scheduler
// libraries registered.
func newTestRegistry(t *testing.T, logger log.Logger, specProcessor scheduler.SpecProcessor) *chasm.Registry {
	registry := chasm.NewRegistry(logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	if err := registry.Register(newTestLibrary(logger, specProcessor)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}
	return registry
}

// startTestExecution starts a scheduler execution on engine, with a root
// component built by startFn, and returns a ref to it.
func startTestExecution(
	t *testing.T,
	engine *chasmtest.Engine,
	startFn func(chasm.MutableContext) (*scheduler.Scheduler, error),
) chasm.ComponentRef {
	ref := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})
	_, err := engine.StartExecution(context.Background(), ref, func(ctx chasm.MutableContext) (chasm.RootComponent, error) {
		return startFn(ctx)
	})
	if err != nil {
		t.Fatalf("failed to start scheduler execution: %v", err)
	}
	return ref
}

// setupExecutionForTest starts an execution whose root component is built by
// startFn, and returns its root, a fresh mutable context on it, and the
// testEnv that owns it.
func setupExecutionForTest(
	t *testing.T,
	specProcessor scheduler.SpecProcessor,
	startFn func(chasm.MutableContext) (*scheduler.Scheduler, error),
) (*scheduler.Scheduler, chasm.MutableContext, *testEnv) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	env := startTestEnv(t, logger, specProcessor, timeSource, startFn)
	return env.Scheduler, env.MutableContext(), env
}

// newFixedSpecProcessor returns a mock SpecProcessor that always reports the
// next wakeup an hour from now.
func newFixedSpecProcessor(t *testing.T) scheduler.SpecProcessor {
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(&scheduler.ProcessedTimeRange{
		NextWakeupTime: time.Now().Add(time.Hour),
		LastActionTime: time.Now(),
	}, nil).AnyTimes()
	specProcessor.EXPECT().NextTime(gomock.Any(), gomock.Any()).Return(legacyscheduler.GetNextTimeResult{
		Next:    time.Now().Add(time.Hour),
		Nominal: time.Now().Add(time.Hour),
	}, nil).AnyTimes()
	return specProcessor
}

func setupSchedulerForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *testEnv) {
	return setupExecutionForTest(t, newFixedSpecProcessor(t), func(ctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	})
}

// setupMigratedSchedulerForTest is setupSchedulerForTest for a scheduler
// created from V1 migration state. inspect runs on the new scheduler inside
// the start transaction, before closing it runs any immediate tasks.
func setupMigratedSchedulerForTest(
	t *testing.T,
	req *schedulerpb.CreateFromMigrationStateRequest,
	inspect func(*scheduler.Scheduler, chasm.MutableContext),
) (*scheduler.Scheduler, chasm.MutableContext, *testEnv) {
	return setupExecutionForTest(t, newFixedSpecProcessor(t), func(ctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
		require.NoError(t, err)
		inspect(sched, ctx)
		return sched, nil
	})
}

func setupSentinelForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *testEnv) {
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)

	return setupExecutionForTest(t, specProcessor, func(ctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.NewSentinel(ctx, namespace, namespaceID, scheduleID), nil
	})
}
