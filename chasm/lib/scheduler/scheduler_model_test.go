package scheduler_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

const modelDrainLimit = 1000

var modelStartTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

type (
	modelEnvConfig struct {
		startTime        time.Time
		interval         time.Duration
		phase            time.Duration
		specStart        time.Time
		specEnd          time.Time
		overlapPolicy    enumspb.ScheduleOverlapPolicy
		catchupWindow    time.Duration
		paused           bool
		notes            string
		limitedActions   bool
		remainingAction  int64
		pauseOnFailure   bool
		maxBufferSize    int
		generatorReserve int
		maxActions       int
		idleTime         time.Duration
	}

	modelStartOutcome struct {
		err                       error
		commitBeforeResponseError bool
		reconcileCommittedStart   bool
	}

	modelWorkflowService struct {
		workflowservice.WorkflowServiceClient

		mu             sync.Mutex
		starts         map[string]*workflowservice.StartWorkflowExecutionRequest
		startCalls     []*workflowservice.StartWorkflowExecutionRequest
		startOutcomes  []modelStartOutcome
		attachOutcomes []modelStartOutcome
	}

	modelDescribeOutcome struct {
		response *historyservice.DescribeWorkflowExecutionResponse
		err      error
	}

	modelHistoryService struct {
		historyservice.HistoryServiceClient

		mu                 sync.Mutex
		cancels            []*historyservice.RequestCancelWorkflowExecutionRequest
		terminates         []*historyservice.TerminateWorkflowExecutionRequest
		describes          []*historyservice.DescribeWorkflowExecutionRequest
		migrationStarts    []*historyservice.StartWorkflowExecutionRequest
		cancelOutcomes     []error
		terminateOutcomes  []error
		describeOutcomes   []modelDescribeOutcome
		migrationOutcomes  []error
		migrationSuccesses int
	}

	modelTaskSnapshot struct {
		typeName       string
		category       string
		visibilityTime time.Time
	}

	modelBufferedStart struct {
		requestID     string
		workflowID    string
		runID         string
		nominalTime   time.Time
		actualTime    time.Time
		desiredTime   time.Time
		backoffTime   time.Time
		attempt       int64
		manual        bool
		overlapPolicy enumspb.ScheduleOverlapPolicy
		completed     enumspb.WorkflowExecutionStatus
		hasCallback   bool
	}

	modelBackfiller struct {
		attempt           int64
		lastProcessedTime time.Time
	}

	modelInternalSnapshot struct {
		closed             bool
		sentinel           bool
		migrationPending   bool
		preMigrationPaused bool
		preMigrationNotes  string
		conflictToken      int64
		createTime         time.Time
		lastProcessedTime  time.Time
		idleCloseTime      time.Time
		lastSuccess        []byte
		lastFailure        string
		backfillers        int
		backfillerStates   []modelBackfiller
		buffered           []modelBufferedStart
	}

	modelWorkflowSnapshot struct {
		starts     map[string]*workflowservice.StartWorkflowExecutionRequest
		startCalls []*workflowservice.StartWorkflowExecutionRequest
	}

	modelHistorySnapshot struct {
		cancels            []*historyservice.RequestCancelWorkflowExecutionRequest
		terminates         []*historyservice.TerminateWorkflowExecutionRequest
		describes          []*historyservice.DescribeWorkflowExecutionRequest
		migrationStarts    []*historyservice.StartWorkflowExecutionRequest
		migrationSuccesses int
	}

	modelRetryPolicy struct{}

	schedulerModelEnv struct {
		handler     schedulerpb.SchedulerServiceServer
		engine      *chasmtest.Engine
		engineCtx   context.Context
		ref         chasm.ComponentRef
		timeSource  *clock.EventTimeSource
		workflows   *modelWorkflowService
		history     *modelHistoryService
		config      modelEnvConfig
		scheduleID  string
		completionN int
	}
)

func (modelRetryPolicy) ComputeNextDelay(_ time.Duration, attempts int, _ error) time.Duration {
	delay := time.Second
	for attempt := 1; attempt < attempts && delay < time.Minute; attempt++ {
		delay *= 2
	}
	return min(delay, time.Minute)
}

func (s *modelWorkflowService) StartWorkflowExecution(
	_ context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	request = proto.CloneOf(request)
	s.startCalls = append(s.startCalls, request)
	outcomes := &s.startOutcomes
	if request.GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
		outcomes = &s.attachOutcomes
	}
	if len(*outcomes) > 0 {
		outcome := (*outcomes)[0]
		*outcomes = (*outcomes)[1:]
		if outcome.commitBeforeResponseError {
			if _, exists := s.starts[request.RequestId]; !exists {
				s.starts[request.RequestId] = request
			}
		}
		if outcome.reconcileCommittedStart {
			return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
				"workflow already started", request.RequestId, modelRunID(request.RequestId),
			)
		}
		if outcome.err != nil {
			return nil, outcome.err
		}
	}

	if _, exists := s.starts[request.RequestId]; !exists {
		s.starts[request.RequestId] = request
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: modelRunID(request.RequestId)}, nil
}

func (s *modelWorkflowService) pushStartError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startOutcomes = append(s.startOutcomes, modelStartOutcome{err: err})
}

func (s *modelWorkflowService) pushCommittedResponseLost(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startOutcomes = append(s.startOutcomes, modelStartOutcome{
		err: err, commitBeforeResponseError: true,
	})
}

func (s *modelWorkflowService) pushReconcileCommittedStart() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startOutcomes = append(s.startOutcomes, modelStartOutcome{reconcileCommittedStart: true})
}

func (s *modelWorkflowService) pushAttachError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attachOutcomes = append(s.attachOutcomes, modelStartOutcome{err: err})
}

func (s *modelHistoryService) pushDescribeOutcome(
	response *historyservice.DescribeWorkflowExecutionResponse,
	err error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.describeOutcomes = append(s.describeOutcomes, modelDescribeOutcome{response: response, err: err})
}

func (s *modelHistoryService) pushMigrationError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.migrationOutcomes = append(s.migrationOutcomes, err)
}

func (s *modelWorkflowService) snapshot() modelWorkflowSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := modelWorkflowSnapshot{
		starts:     make(map[string]*workflowservice.StartWorkflowExecutionRequest, len(s.starts)),
		startCalls: make([]*workflowservice.StartWorkflowExecutionRequest, len(s.startCalls)),
	}
	for requestID, request := range s.starts {
		result.starts[requestID] = proto.CloneOf(request)
	}
	for i, request := range s.startCalls {
		result.startCalls[i] = proto.CloneOf(request)
	}
	return result
}

func (s *modelHistoryService) RequestCancelWorkflowExecution(
	_ context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancels = append(s.cancels, proto.CloneOf(request))
	if len(s.cancelOutcomes) > 0 {
		err := s.cancelOutcomes[0]
		s.cancelOutcomes = s.cancelOutcomes[1:]
		return &historyservice.RequestCancelWorkflowExecutionResponse{}, err
	}
	return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
}

func (s *modelHistoryService) TerminateWorkflowExecution(
	_ context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminates = append(s.terminates, proto.CloneOf(request))
	if len(s.terminateOutcomes) > 0 {
		err := s.terminateOutcomes[0]
		s.terminateOutcomes = s.terminateOutcomes[1:]
		return &historyservice.TerminateWorkflowExecutionResponse{}, err
	}
	return &historyservice.TerminateWorkflowExecutionResponse{}, nil
}

func (s *modelHistoryService) DescribeWorkflowExecution(
	_ context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.describes = append(s.describes, proto.CloneOf(request))
	if len(s.describeOutcomes) == 0 {
		return &historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: request.GetRequest().GetExecution(),
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		}, nil
	}
	outcome := s.describeOutcomes[0]
	s.describeOutcomes = s.describeOutcomes[1:]
	return proto.CloneOf(outcome.response), outcome.err
}

func (s *modelHistoryService) StartWorkflowExecution(
	_ context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.migrationStarts = append(s.migrationStarts, proto.CloneOf(request))
	if len(s.migrationOutcomes) > 0 {
		err := s.migrationOutcomes[0]
		s.migrationOutcomes = s.migrationOutcomes[1:]
		if err != nil {
			return nil, err
		}
	}
	s.migrationSuccesses++
	return &historyservice.StartWorkflowExecutionResponse{RunId: "migration-run"}, nil
}

func (s *modelHistoryService) snapshot() modelHistorySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return modelHistorySnapshot{
		cancels:            cloneProtoSlice(s.cancels),
		terminates:         cloneProtoSlice(s.terminates),
		describes:          cloneProtoSlice(s.describes),
		migrationStarts:    cloneProtoSlice(s.migrationStarts),
		migrationSuccesses: s.migrationSuccesses,
	}
}

func cloneProtoSlice[T proto.Message](input []T) []T {
	result := make([]T, len(input))
	for i, value := range input {
		result[i] = proto.Clone(value).(T)
	}
	return result
}

func defaultModelEnvConfig() modelEnvConfig {
	return modelEnvConfig{
		startTime:        modelStartTime,
		interval:         defaultInterval,
		overlapPolicy:    enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		catchupWindow:    24 * time.Hour,
		maxBufferSize:    scheduler.DefaultTweakables.MaxBufferSize,
		generatorReserve: scheduler.DefaultTweakables.GeneratorBufferReserveSize,
		maxActions:       100,
		idleTime:         365 * 24 * time.Hour,
	}
}

func newSchedulerModelEnv(t *rapid.T, config modelEnvConfig) *schedulerModelEnv {
	t.Helper()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(config.startTime)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	workflows := &modelWorkflowService{starts: make(map[string]*workflowservice.StartWorkflowExecutionRequest)}
	history := &modelHistoryService{}
	schedulerConfig := modelSchedulerConfig(config)
	specBuilder := legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	specProcessor := scheduler.NewSpecProcessor(schedulerConfig, metrics.NoopMetricsHandler, logger, specBuilder)
	handler := scheduler.NewTestHandler(logger)
	invokerOpts := scheduler.InvokerTaskHandlerOptions{
		Config:         schedulerConfig,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		FrontendClient: workflows,
		HistoryClient:  history,
	}
	library := scheduler.NewLibrary(
		schedulerConfig,
		handler,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config: schedulerConfig, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config: schedulerConfig, HistoryClient: history, FrontendClient: workflows,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config: schedulerConfig, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
			SpecProcessor: specProcessor, SpecBuilder: specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOpts),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config: schedulerConfig, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
			SpecProcessor: specProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config: schedulerConfig, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
			HistoryClient: history, SaMapperProvider: searchattribute.NewTestMapperProvider(nil),
		}),
	)
	registry := chasm.NewRegistry(logger)
	mustNoError(t, registry.Register(&chasm.CoreLibrary{}))
	mustNoError(t, registry.Register(library))

	namespaceEntry := namespacepkg.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: namespace}, nil, "active",
	)
	engine := chasmtest.NewEngine(
		t,
		registry,
		chasmtest.WithTimeSource(timeSource),
		chasmtest.WithNodeBackendDecorator(func(backend *chasm.MockNodeBackend) {
			backend.HandleGetNamespaceEntry = func() *namespacepkg.Namespace { return namespaceEntry }
		}),
	)
	engineCtx := chasm.NewEngineContext(t.Context(), engine)
	env := &schedulerModelEnv{
		handler: handler, engine: engine, engineCtx: engineCtx,
		ref: chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
			NamespaceID: namespaceID, BusinessID: scheduleID,
		}),
		timeSource: timeSource, workflows: workflows, history: history, config: config,
		scheduleID: scheduleID,
	}

	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID, RequestId: "create-request",
			Schedule: modelSchedule(config),
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				"created-memo": {Data: []byte("created")},
			}},
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": {Data: []byte("created")},
			}},
		},
	})
	mustNoError(t, err)
	env.drain(t)
	return env
}

func modelSchedulerConfig(config modelEnvConfig) *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(string) scheduler.Tweakables {
			tweakables := scheduler.DefaultTweakables
			tweakables.IdleTime = config.idleTime
			tweakables.MaxBufferSize = config.maxBufferSize
			tweakables.GeneratorBufferReserveSize = config.generatorReserve
			tweakables.MaxActionsPerExecution = config.maxActions
			return tweakables
		},
		ServiceCallTimeout: func() time.Duration { return 5 * time.Second },
		RetryPolicy: func() backoff.RetryPolicy {
			return modelRetryPolicy{}
		},
		EncodeInternalTokenWithEnvelope: func(string) bool { return true },
	}
}

func modelSchedule(config modelEnvConfig) *schedulepb.Schedule {
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
		Interval: durationpb.New(config.interval), Phase: durationpb.New(config.phase),
	}}}
	if !config.specStart.IsZero() {
		spec.StartTime = timestamppb.New(config.specStart)
	}
	if !config.specEnd.IsZero() {
		spec.EndTime = timestamppb.New(config.specEnd)
	}
	return &schedulepb.Schedule{
		Spec: spec,
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId: "scheduled-wf", WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				TaskQueue: &taskqueuepb.TaskQueue{Name: "scheduler-model-queue"},
			},
		}},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: config.overlapPolicy, CatchupWindow: durationpb.New(config.catchupWindow),
			PauseOnFailure: config.pauseOnFailure,
		},
		State: &schedulepb.ScheduleState{
			Paused: config.paused, Notes: config.notes,
			LimitedActions: config.limitedActions, RemainingActions: config.remainingAction,
		},
	}
}

func (e *schedulerModelEnv) drain(t *rapid.T) int {
	t.Helper()
	for drained := range modelDrainLimit {
		runnable := e.runnableTasks(t)
		if len(runnable) == 0 {
			return drained
		}
		selected := rapid.IntRange(0, len(runnable)-1).Draw(t, "runnable task")
		_, err := e.engine.ExecuteTask(e.engineCtx, e.ref, runnable[selected])
		mustNoError(t, err)
	}
	runnable := e.runnableTasks(t)
	if len(runnable) != 0 {
		t.Fatalf("task drain limit %d reached with %d runnable tasks remaining", modelDrainLimit, len(runnable))
	}
	return modelDrainLimit
}

func (e *schedulerModelEnv) drainNewStarts(t *rapid.T) int {
	t.Helper()
	before := len(e.workflows.snapshot().starts)
	e.drain(t)
	return len(e.workflows.snapshot().starts) - before
}

func (e *schedulerModelEnv) executeOne(t *rapid.T) (tasks.Task, chasmtest.TaskExecutionResult) {
	t.Helper()
	task := e.selectRunnableTask(t)
	result, err := e.engine.ExecuteTask(e.engineCtx, e.ref, task)
	mustNoError(t, err)
	return task, result
}

func (e *schedulerModelEnv) executeOneError(t *rapid.T, expected error) tasks.Task {
	t.Helper()
	task := e.selectRunnableTask(t)
	_, err := e.engine.ExecuteTask(e.engineCtx, e.ref, task)
	if !errors.Is(err, expected) {
		t.Fatalf("task error: got %v, want %v", err, expected)
	}
	stillRunnable := e.runnableTasks(t)
	if !slices.Contains(stillRunnable, task) {
		t.Fatal("failed task was not retained for retry")
	}
	return task
}

func (e *schedulerModelEnv) runnableTasks(t *rapid.T) []tasks.Task {
	t.Helper()
	runnable, err := e.engine.RunnableTasks(e.ref)
	mustNoError(t, err)
	return runnable
}

func (e *schedulerModelEnv) selectRunnableTask(t *rapid.T) tasks.Task {
	t.Helper()
	runnable := e.runnableTasks(t)
	if len(runnable) == 0 {
		t.Fatal("expected one runnable task")
	}
	return runnable[rapid.IntRange(0, len(runnable)-1).Draw(t, "runnable task")]
}

func (e *schedulerModelEnv) advanceToNextTask(t *rapid.T) time.Time {
	t.Helper()
	now := e.timeSource.Now()
	for _, task := range e.tasks(t) {
		if task.visibilityTime.After(now) {
			e.timeSource.Update(task.visibilityTime)
			return task.visibilityTime
		}
	}
	t.Skip("no future task")
	return time.Time{}
}

func (e *schedulerModelEnv) reload(t *rapid.T) {
	t.Helper()
	beforeInternal := e.internal(t)
	beforeTasks := e.tasks(t)
	beforeWorkflow := e.workflows.snapshot()
	beforeHistory := e.history.snapshot()
	var beforeDescription *workflowservice.DescribeScheduleResponse
	if !beforeInternal.closed && !beforeInternal.sentinel {
		beforeDescription = proto.CloneOf(e.describe(t))
	}

	mustNoError(t, e.engine.ReloadExecution(e.engineCtx, e.ref))

	afterInternal := e.internal(t)
	if !reflect.DeepEqual(beforeInternal, afterInternal) {
		t.Fatalf("reload changed internal state: before=%+v after=%+v", beforeInternal, afterInternal)
	}
	if !slices.Equal(beforeTasks, e.tasks(t)) {
		t.Fatal("reload changed physical tasks")
	}
	if !modelWorkflowSnapshotsEqual(beforeWorkflow, e.workflows.snapshot()) ||
		!modelHistorySnapshotsEqual(beforeHistory, e.history.snapshot()) {
		t.Fatal("reload made a service call")
	}
	if beforeDescription != nil && !proto.Equal(beforeDescription, e.describe(t)) {
		t.Fatal("reload changed DescribeSchedule state")
	}
}

func modelWorkflowSnapshotsEqual(a, b modelWorkflowSnapshot) bool {
	if len(a.starts) != len(b.starts) || len(a.startCalls) != len(b.startCalls) {
		return false
	}
	for requestID, request := range a.starts {
		if !proto.Equal(request, b.starts[requestID]) {
			return false
		}
	}
	return protoSlicesEqual(a.startCalls, b.startCalls)
}

func modelHistorySnapshotsEqual(a, b modelHistorySnapshot) bool {
	return a.migrationSuccesses == b.migrationSuccesses &&
		protoSlicesEqual(a.cancels, b.cancels) &&
		protoSlicesEqual(a.terminates, b.terminates) &&
		protoSlicesEqual(a.describes, b.describes) &&
		protoSlicesEqual(a.migrationStarts, b.migrationStarts)
}

func protoSlicesEqual[T proto.Message](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func (e *schedulerModelEnv) redeliver(t *rapid.T, task tasks.Task) chasmtest.TaskExecutionResult {
	t.Helper()
	result, err := e.engine.ExecuteTask(e.engineCtx, e.ref, task)
	mustNoError(t, err)
	return result
}

func (e *schedulerModelEnv) requireNoRunnableTasks(t *rapid.T) {
	t.Helper()
	runnable, err := e.engine.RunnableTasks(e.ref)
	mustNoError(t, err)
	if len(runnable) != 0 {
		t.Fatalf("found %d runnable tasks after drain; next is %T", len(runnable), runnable[0])
	}
}

func (e *schedulerModelEnv) tasks(t *rapid.T) []modelTaskSnapshot {
	t.Helper()
	queued, err := e.engine.Tasks(e.ref)
	mustNoError(t, err)
	var result []modelTaskSnapshot
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			result = append(result, modelTaskSnapshot{
				typeName: fmt.Sprintf("%T", task), category: category.Name(),
				visibilityTime: task.GetVisibilityTime(),
			})
		}
	}
	slices.SortFunc(result, func(a, b modelTaskSnapshot) int {
		if order := a.visibilityTime.Compare(b.visibilityTime); order != 0 {
			return order
		}
		if a.category != b.category {
			return compareString(a.category, b.category)
		}
		return compareString(a.typeName, b.typeName)
	})
	return result
}

func (e *schedulerModelEnv) describe(t *rapid.T) *workflowservice.DescribeScheduleResponse {
	t.Helper()
	response, err := e.handler.DescribeSchedule(e.engineCtx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: e.scheduleID},
	})
	mustNoError(t, err)
	return response.GetFrontendResponse()
}

func (e *schedulerModelEnv) listMatching(
	t *rapid.T,
	start time.Time,
	end time.Time,
) ([]time.Time, error) {
	t.Helper()
	response, err := e.handler.ListScheduleMatchingTimes(e.engineCtx, &schedulerpb.ListScheduleMatchingTimesRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
			Namespace: namespace, ScheduleId: e.scheduleID,
			StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
		},
	})
	if err != nil {
		return nil, err
	}
	result := make([]time.Time, len(response.GetFrontendResponse().GetStartTime()))
	for i, value := range response.GetFrontendResponse().GetStartTime() {
		result[i] = value.AsTime()
	}
	return result, nil
}

func (e *schedulerModelEnv) internal(t *rapid.T) modelInternalSnapshot {
	t.Helper()
	result, err := chasm.ReadComponent(
		e.engineCtx,
		e.ref,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (modelInternalSnapshot, error) {
			snapshot := modelInternalSnapshot{
				closed: s.Closed, sentinel: s.Sentinel, migrationPending: s.WorkflowMigration != nil,
				conflictToken: s.ConflictToken, createTime: s.GetInfo().GetCreateTime().AsTime(),
				backfillers: len(s.Backfillers),
			}
			if s.GetIdleCloseTime() != nil {
				snapshot.idleCloseTime = s.GetIdleCloseTime().AsTime()
			}
			if s.WorkflowMigration != nil {
				snapshot.preMigrationPaused = s.WorkflowMigration.GetPreMigrationPaused()
				snapshot.preMigrationNotes = s.WorkflowMigration.GetPreMigrationNotes()
			}
			if s.Sentinel {
				return snapshot, nil
			}
			lastCompletion := s.LastCompletionResult.Get(ctx)
			snapshot.lastSuccess = slices.Clone(lastCompletion.GetSuccess().GetData())
			snapshot.lastFailure = lastCompletion.GetFailure().GetMessage()
			for _, field := range s.Backfillers {
				backfiller := field.Get(ctx)
				snapshot.backfillerStates = append(snapshot.backfillerStates, modelBackfiller{
					attempt: backfiller.GetAttempt(), lastProcessedTime: backfiller.GetLastProcessedTime().AsTime(),
				})
			}
			slices.SortFunc(snapshot.backfillerStates, func(a, b modelBackfiller) int {
				if a.attempt < b.attempt {
					return -1
				}
				if a.attempt > b.attempt {
					return 1
				}
				return a.lastProcessedTime.Compare(b.lastProcessedTime)
			})
			invoker := s.Invoker.Get(ctx)
			snapshot.lastProcessedTime = invoker.GetLastProcessedTime().AsTime()
			for _, start := range invoker.GetBufferedStarts() {
				status := enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
				if start.GetCompleted() != nil {
					status = start.GetCompleted().GetStatus()
				}
				snapshot.buffered = append(snapshot.buffered, modelBufferedStart{
					requestID: start.GetRequestId(), workflowID: start.GetWorkflowId(), runID: start.GetRunId(),
					nominalTime: start.GetNominalTime().AsTime(), actualTime: start.GetActualTime().AsTime(),
					desiredTime: start.GetDesiredTime().AsTime(), backoffTime: start.GetBackoffTime().AsTime(),
					attempt: start.GetAttempt(), manual: start.GetManual(), overlapPolicy: start.GetOverlapPolicy(),
					completed: status, hasCallback: start.GetHasCallback(),
				})
			}
			slices.SortFunc(snapshot.buffered, func(a, b modelBufferedStart) int {
				return compareString(a.requestID, b.requestID)
			})
			return snapshot, nil
		},
		struct{}{},
	)
	mustNoError(t, err)
	return result
}

func (e *schedulerModelEnv) complete(
	t *rapid.T,
	requestID string,
	status enumspb.WorkflowExecutionStatus,
	payload []byte,
) {
	t.Helper()
	start := e.workflows.snapshot().starts[requestID]
	if start == nil || len(start.GetCompletionCallbacks()) != 1 {
		t.Fatalf("request %q has no recorded completion callback", requestID)
	}
	callback := start.GetCompletionCallbacks()[0].GetNexus()
	if callback == nil || len(callback.Header) != 1 {
		t.Fatalf("request %q has invalid completion callback", requestID)
	}
	var token string
	for _, value := range callback.Header {
		token = value
	}
	componentRef, callbackRequestID, err := chasm.UnpackNexusCallbackToken(token)
	mustNoError(t, err)
	if callbackRequestID != requestID {
		t.Fatalf("callback request ID: got %q, want %q", callbackRequestID, requestID)
	}

	e.completionN++
	closeTime := e.timeSource.Now().Add(time.Duration(e.completionN) * time.Nanosecond)
	completion := &persistencespb.ChasmNexusCompletion{
		RequestId: requestID, CloseTime: timestamppb.New(closeTime),
	}
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		completion.Outcome = &persistencespb.ChasmNexusCompletion_Success{
			Success: &commonpb.Payload{Data: slices.Clone(payload)},
		}
	} else {
		failure := &failurepb.Failure{Message: string(payload)}
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED {
			failure.FailureInfo = &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
			}
		}
		completion.Outcome = &persistencespb.ChasmNexusCompletion_Failure{Failure: failure}
	}
	_, _, err = chasm.UpdateComponent(
		e.engineCtx,
		componentRef,
		func(
			component chasm.NexusCompletionHandler,
			ctx chasm.MutableContext,
			completion *persistencespb.ChasmNexusCompletion,
		) (chasm.NoValue, error) {
			return nil, component.HandleNexusCompletion(ctx, completion)
		},
		completion,
	)
	mustNoError(t, err)
}

func (e *schedulerModelEnv) runningRequestIDs(t *rapid.T) []string {
	t.Helper()
	var result []string
	for _, start := range e.internal(t).buffered {
		if start.runID != "" && start.completed == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
			result = append(result, start.requestID)
		}
	}
	slices.Sort(result)
	return result
}

func (e *schedulerModelEnv) checkRequestIDs(t *rapid.T) {
	t.Helper()
	snapshot := e.workflows.snapshot()
	for requestID, request := range snapshot.starts {
		if requestID == "" || request.GetRequestId() != requestID {
			t.Fatalf("invalid request ID entry %q", requestID)
		}
		if request.GetWorkflowId() == "" {
			t.Fatalf("request %q has empty workflow ID", requestID)
		}
		if len(request.GetCompletionCallbacks()) != 1 {
			t.Fatalf("request %q has %d callbacks", requestID, len(request.GetCompletionCallbacks()))
		}
	}
}

func expectedIntervalTimes(
	interval time.Duration,
	phase time.Duration,
	specStart time.Time,
	specEnd time.Time,
	after time.Time,
	through time.Time,
) []time.Time {
	if !specStart.IsZero() && after.Before(specStart.Add(-time.Nanosecond)) {
		after = specStart.Add(-time.Nanosecond)
	}
	anchor := time.Unix(0, 0).UTC().Add(phase)
	delta := after.Sub(anchor)
	steps := delta / interval
	if delta >= 0 {
		steps++
	}
	next := anchor.Add(steps * interval)
	for !next.After(after) {
		next = next.Add(interval)
	}
	var result []time.Time
	for !next.After(through) {
		if (!specStart.IsZero() && next.Before(specStart)) || (!specEnd.IsZero() && next.After(specEnd)) {
			next = next.Add(interval)
			continue
		}
		result = append(result, next)
		next = next.Add(interval)
	}
	return result
}

func conflictTokenValue(token []byte) int64 {
	if len(token) != 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(token))
}

func modelRunID(requestID string) string {
	return fmt.Sprintf("run-%s", requestID)
}

func compareString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func mustNoError(t *rapid.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
