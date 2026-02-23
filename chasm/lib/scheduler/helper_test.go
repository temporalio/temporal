package scheduler_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
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
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(1 * time.Second)
		},
	}
}

func newTestLibrary(logger log.Logger, specProcessor scheduler.SpecProcessor) *scheduler.Library {
	config := defaultConfig()
	specBuilder := legacyscheduler.NewSpecBuilder()
	invokerOpts := scheduler.InvokerTaskExecutorOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	}
	return scheduler.NewLibrary(
		nil,
		scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
			Config: config,
		}),
		scheduler.NewGeneratorTaskExecutor(scheduler.GeneratorTaskExecutorOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
			SpecBuilder:    specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskExecutor(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskExecutor(invokerOpts),
		scheduler.NewBackfillerTaskExecutor(scheduler.BackfillerTaskExecutorOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
		}),
	)
}

// testEnv holds all components needed for scheduler tests.
type testEnv struct {
	t             *testing.T // only used within these setup helpers
	Ctrl          *gomock.Controller
	Registry      *chasm.Registry
	Node          *chasm.Node
	NodeBackend   *chasm.MockNodeBackend
	TimeSource    *clock.EventTimeSource
	Scheduler     *scheduler.Scheduler
	SpecProcessor scheduler.SpecProcessor
	MockEngine    *chasm.MockEngine
	Logger        log.Logger
}

// testEnvConfig holds configuration options for testEnv.
type testEnvConfig struct {
	specProcessor  scheduler.SpecProcessor
	withMockEngine bool
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

// withMockEngine configures testEnv to include a mock CHASM engine for side-effect tasks.
func withMockEngine() testEnvOption {
	return func(c *testEnvConfig) {
		c.withMockEngine = true
	}
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
		legacyscheduler.NewSpecBuilder(),
	)
}

// newTestEnv creates a new test environment with the given options.
func newTestEnv(t *testing.T, opts ...testEnvOption) *testEnv {
	config := &testEnvConfig{}
	for _, opt := range opts {
		opt(config)
	}

	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	nodePathEncoder := chasm.DefaultPathEncoder

	// Configure spec processor: use custom if provided, otherwise use real.
	var specProcessor scheduler.SpecProcessor
	if config.specProcessor != nil {
		specProcessor = config.specProcessor
	} else {
		specProcessor = newRealSpecProcessor(ctrl, logger)
	}

	registry := chasm.NewRegistry(logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	if err := registry.Register(newTestLibrary(logger, specProcessor)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)

	tv := testvars.New(t)
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, nodePathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), node)
	sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}
	if err = node.SetRootComponent(sched); err != nil {
		t.Fatalf("failed to set root component: %v", err)
	}

	// Advance Generator's high water mark to 'now'.
	generator := sched.Generator.Get(ctx)
	generator.LastProcessedTime = timestamppb.New(now)

	_, err = node.CloseTransaction()
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	env := &testEnv{
		t:             t,
		Ctrl:          ctrl,
		Registry:      registry,
		Node:          node,
		NodeBackend:   nodeBackend,
		TimeSource:    timeSource,
		Scheduler:     sched,
		SpecProcessor: specProcessor,
		Logger:        logger,
	}

	if config.withMockEngine {
		env.MockEngine = chasm.NewMockEngine(ctrl)
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

// CloseTransaction closes the current CHASM transaction.
func (e *testEnv) CloseTransaction() error {
	_, err := e.Node.CloseTransaction()
	return err
}

// HasTask returns true if the given task type was added with the given visibilityTime.
func (e *testEnv) HasTask(task any, visibilityTime time.Time) bool {
	taskType := reflect.TypeOf(task)
	for _, tasks := range e.NodeBackend.TasksByCategory {
		for _, t := range tasks {
			if reflect.TypeOf(t) == taskType &&
				t.GetVisibilityTime().Equal(visibilityTime) {
				return true
			}
		}
	}
	return false
}

// EngineContext returns a context with a mock engine. Requires withMockEngine().
func (e *testEnv) EngineContext() context.Context {
	if e.MockEngine == nil {
		e.t.Fatal("EngineContext requires withMockEngine() option")
	}
	return chasm.NewEngineContext(context.Background(), e.MockEngine)
}

// ExpectReadComponent sets up mock expectations for reading a component.
func (e *testEnv) ExpectReadComponent(ctx chasm.Context, returnedComponent chasm.Component) {
	if e.MockEngine == nil {
		e.t.Fatal("ExpectReadComponent requires withMockEngine() option")
	}
	e.MockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component, *chasm.Registry) error, _ ...chasm.TransitionOption) error {
			return readFn(ctx, returnedComponent, e.Registry)
		}).Times(1)
}

// ExpectUpdateComponent sets up mock expectations for updating a component.
func (e *testEnv) ExpectUpdateComponent(ctx chasm.MutableContext, componentToUpdate chasm.Component) {
	if e.MockEngine == nil {
		e.t.Fatal("ExpectUpdateComponent requires withMockEngine() option")
	}
	e.MockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component, *chasm.Registry) error, _ ...chasm.TransitionOption) ([]byte, error) {
			err := updateFn(ctx, componentToUpdate, e.Registry)
			return nil, err
		}).Times(1)
}

type testInfra struct {
	node        *chasm.Node
	nodeBackend *chasm.MockNodeBackend
	logger      log.Logger
}

// setupTestInfra creates the common test infrastructure for scheduler tests.
func setupTestInfra(t *testing.T, specProcessor scheduler.SpecProcessor) *testInfra {
	nodeBackend := &chasm.MockNodeBackend{}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	nodePathEncoder := chasm.DefaultPathEncoder

	registry := chasm.NewRegistry(logger)
	err := registry.Register(&chasm.CoreLibrary{})
	if err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	err = registry.Register(newTestLibrary(logger, specProcessor))
	if err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	tv := testvars.New(t)
	nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }
	nodeBackend.HandleGetWorkflowKey = tv.Any().WorkflowKey
	nodeBackend.HandleIsWorkflow = func() bool { return false }
	nodeBackend.HandleCurrentVersionedTransition = func() *persistencespb.VersionedTransition {
		return &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          1,
		}
	}

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, nodePathEncoder, logger, metrics.NoopMetricsHandler)
	return &testInfra{
		node:        node,
		nodeBackend: nodeBackend,
		logger:      logger,
	}
}

func setupSchedulerForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *chasm.Node) {
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

	infra := setupTestInfra(t, specProcessor)
	ctx := chasm.NewMutableContext(context.Background(), infra.node)
	sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}
	err = infra.node.SetRootComponent(sched)
	if err != nil {
		t.Fatalf("failed to set root component: %v", err)
	}
	_, err = infra.node.CloseTransaction()
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	ctx = chasm.NewMutableContext(context.Background(), infra.node)
	return sched, ctx, infra.node
}

func setupSentinelForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *chasm.Node) {
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)

	infra := setupTestInfra(t, specProcessor)
	ctx := chasm.NewMutableContext(context.Background(), infra.node)
	sentinel := scheduler.NewSentinel(ctx, namespace, namespaceID, scheduleID)
	err := infra.node.SetRootComponent(sentinel)
	if err != nil {
		t.Fatalf("failed to set root component: %v", err)
	}
	_, err = infra.node.CloseTransaction()
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	ctx = chasm.NewMutableContext(context.Background(), infra.node)
	return sentinel, ctx, infra.node
}
