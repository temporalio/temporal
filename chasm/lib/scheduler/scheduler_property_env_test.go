package scheduler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var schedulerPropertyStartTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

const (
	schedulerPropertyMaxBufferSize = 16
	startWorkflowMethod            = "StartWorkflowExecution"
	describeWorkflowMethod         = "DescribeWorkflowExecution"
	cancelWorkflowMethod           = "RequestCancelWorkflowExecution"
	terminateWorkflowMethod        = "TerminateWorkflowExecution"
	migrateWorkflowMethod          = "MigrateStartWorkflowExecution"
)

type schedulerPropertyEnv struct {
	handler    schedulerpb.SchedulerServiceServer
	engine     *chasmtest.Engine
	engineCtx  context.Context
	ref        chasm.ComponentRef
	timeSource *clock.EventTimeSource
	services   schedulerServiceScripts
	delivered  []chasmtest.DeliveryRef
}

type schedulerServiceScripts struct {
	Start     rpctest.RPCContract
	Describe  rpctest.RPCContract
	Cancel    rpctest.RPCContract
	Terminate rpctest.RPCContract
	Migrate   rpctest.RPCContract

	startCalls []schedulerStartCall
}

type schedulerStartCall struct {
	Request  *workflowservice.StartWorkflowExecutionRequest
	Response *workflowservice.StartWorkflowExecutionResponse
	Err      error
}

type schedulerPropertyTestingT interface {
	require.TestingT
	Helper()
}

type schedulerPropertyOwner interface {
	testlogger.TestingT
	Context() context.Context
}

func newSchedulerPropertyEnv(t schedulerPropertyOwner, initiallyPaused bool) *schedulerPropertyEnv {
	return newSchedulerPropertyEnvWithPolicy(t, initiallyPaused, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
}

func newSchedulerPropertyEnvWithPolicy(t schedulerPropertyOwner, initiallyPaused bool, overlapPolicy enumspb.ScheduleOverlapPolicy) *schedulerPropertyEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	config := schedulerPropertyConfig()
	specBuilder := newLegacySpecBuilder(0, 0)
	specProcessor := scheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler, logger, specBuilder)
	frontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)
	handler := scheduler.NewTestHandler(logger)
	env := &schedulerPropertyEnv{handler: handler}

	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(env.startWorkflow).AnyTimes()
	history.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(env.describeWorkflow).AnyTimes()
	history.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(env.cancelWorkflow).AnyTimes()
	history.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(env.terminateWorkflow).AnyTimes()
	history.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(env.migrateWorkflow).AnyTimes()

	invokerOptions := scheduler.InvokerTaskHandlerOptions{
		Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, SpecProcessor: specProcessor,
		HistoryClient: history, FrontendClient: frontend,
	}
	library := scheduler.NewLibrary(
		config,
		handler,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{Config: config, HistoryClient: history, FrontendClient: frontend}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, SpecProcessor: specProcessor, SpecBuilder: specBuilder}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOptions),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOptions),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, SpecProcessor: specProcessor}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, HistoryClient: history, SaMapperProvider: searchattribute.NewTestMapperProvider(nil)}),
	)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(library))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(schedulerPropertyStartTime)
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
	schedule := proto.CloneOf(defaultSchedule())
	schedule.State.Paused = initiallyPaused
	schedule.Policies.OverlapPolicy = overlapPolicy
	schedule.Action.GetStartWorkflow().TaskQueue = &taskqueuepb.TaskQueue{Name: "property-task-queue"}
	schedule.Action.GetStartWorkflow().Input = &commonpb.Payloads{}
	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.CreateScheduleRequest{
		Namespace: namespace, ScheduleId: scheduleID, RequestId: "property-create", Schedule: schedule,
	}})
	require.NoError(t, err)

	env.engine = engine
	env.engineCtx = engineCtx
	env.ref = chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID}}
	env.timeSource = timeSource
	return env
}

func schedulerPropertyConfig() *scheduler.Config {
	tweakables := scheduler.DefaultTweakables
	tweakables.MaxBufferSize = schedulerPropertyMaxBufferSize
	tweakables.GeneratorBufferReserveSize = 0
	tweakables.MaxActionsPerExecution = 2
	tweakables.IdleTime = 10 * time.Minute
	config := defaultConfig()
	config.Tweakables = func(string) scheduler.Tweakables { return tweakables }
	return config
}

func (e *schedulerPropertyEnv) startWorkflow(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	var response *workflowservice.StartWorkflowExecutionResponse
	var err error
	if e.services.Start.Pending() > 0 {
		value, invokeErr := e.services.Start.Invoke(ctx, startWorkflowMethod, request)
		err = invokeErr
		if value != nil {
			response = value.(*workflowservice.StartWorkflowExecutionResponse)
		}
	} else {
		response = &workflowservice.StartWorkflowExecutionResponse{RunId: "run-" + request.GetRequestId()}
	}
	e.services.startCalls = append(e.services.startCalls, schedulerStartCall{Request: proto.CloneOf(request), Response: proto.CloneOf(response), Err: err})
	return response, err
}

func (e *schedulerPropertyEnv) describeWorkflow(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	if e.services.Describe.Pending() > 0 {
		value, err := e.services.Describe.Invoke(ctx, describeWorkflowMethod, request)
		if value == nil {
			return nil, err
		}
		return value.(*historyservice.DescribeWorkflowExecutionResponse), err
	}
	return &historyservice.DescribeWorkflowExecutionResponse{}, nil
}

func (e *schedulerPropertyEnv) cancelWorkflow(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	if e.services.Cancel.Pending() > 0 {
		value, err := e.services.Cancel.Invoke(ctx, cancelWorkflowMethod, request)
		if value == nil {
			return nil, err
		}
		return value.(*historyservice.RequestCancelWorkflowExecutionResponse), err
	}
	return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
}

func (e *schedulerPropertyEnv) terminateWorkflow(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	if e.services.Terminate.Pending() > 0 {
		value, err := e.services.Terminate.Invoke(ctx, terminateWorkflowMethod, request)
		if value == nil {
			return nil, err
		}
		return value.(*historyservice.TerminateWorkflowExecutionResponse), err
	}
	return &historyservice.TerminateWorkflowExecutionResponse{}, nil
}

func (e *schedulerPropertyEnv) migrateWorkflow(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
	if e.services.Migrate.Pending() > 0 {
		value, err := e.services.Migrate.Invoke(ctx, migrateWorkflowMethod, request)
		if value == nil {
			return nil, err
		}
		return value.(*historyservice.StartWorkflowExecutionResponse), err
	}
	return &historyservice.StartWorkflowExecutionResponse{RunId: "migration-" + request.GetStartRequest().GetRequestId()}, nil
}

func (e *schedulerPropertyEnv) describe(t schedulerPropertyTestingT) *workflowservice.DescribeScheduleResponse {
	t.Helper()
	response, err := e.handler.DescribeSchedule(e.engineCtx, &schedulerpb.DescribeScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: scheduleID}})
	require.NoError(t, err)
	return response.GetFrontendResponse()
}

func (e *schedulerPropertyEnv) setPaused(t schedulerPropertyTestingT, paused bool) {
	t.Helper()
	patch := &schedulepb.SchedulePatch{Pause: "property pause"}
	if !paused {
		patch = &schedulepb.SchedulePatch{Unpause: "property unpause"}
	}
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.PatchScheduleRequest{Namespace: namespace, ScheduleId: scheduleID, Patch: patch}})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) trigger(t schedulerPropertyTestingT) {
	t.Helper()
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.PatchScheduleRequest{Namespace: namespace, ScheduleId: scheduleID, Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}}})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) backfill(t schedulerPropertyTestingT, start, end time.Time, overlapPolicy enumspb.ScheduleOverlapPolicy) {
	t.Helper()
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.PatchScheduleRequest{
		Namespace: namespace, ScheduleId: scheduleID, Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
			StartTime: timestamppb.New(start), EndTime: timestamppb.New(end), OverlapPolicy: overlapPolicy,
		}}},
	}})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) delete(t schedulerPropertyTestingT) {
	t.Helper()
	_, err := e.handler.DeleteSchedule(e.engineCtx, &schedulerpb.DeleteScheduleRequest{NamespaceId: namespaceID, FrontendRequest: &workflowservice.DeleteScheduleRequest{Namespace: namespace, ScheduleId: scheduleID}})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) complete(t schedulerPropertyTestingT, requestID string) {
	t.Helper()
	_, err := e.engine.UpdateComponent(e.engineCtx, e.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		return component.(*scheduler.Scheduler).HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{RequestId: requestID, Outcome: &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}}, CloseTime: timestamppb.New(e.timeSource.Now())})
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) drain(t schedulerPropertyTestingT, limit int) {
	t.Helper()
	for range limit {
		runnable, err := e.engine.RunnableDeliveries(e.ref)
		require.NoError(t, err)
		if len(runnable) == 0 {
			return
		}
		receipt, err := e.engine.Deliver(e.engineCtx, e.ref, runnable[0])
		require.NoError(t, err, "deliver %s", describeDelivery(runnable[0]))
		e.delivered = append(e.delivered, receipt.Ref)
	}
	require.FailNow(t, "task drain limit reached", "execution=%+v", e.ref.ExecutionKey)
}

func (e *schedulerPropertyEnv) reload(t schedulerPropertyTestingT) {
	t.Helper()
	before := e.describe(t)
	require.NoError(t, e.engine.ReloadExecution(e.engineCtx, e.ref))
	require.True(t, proto.Equal(before, e.describe(t)))
}

func describeDelivery(delivery chasmtest.DeliveryRef) string {
	return fmt.Sprintf("%s/%d at %s", delivery.ID, delivery.CategoryID, delivery.VisibilityTime.Format(time.RFC3339Nano))
}

func (e *schedulerPropertyEnv) assertScripts(t schedulerPropertyTestingT) {
	t.Helper()
	require.NoError(t, e.services.Start.AssertSatisfied())
	require.NoError(t, e.services.Describe.AssertSatisfied())
	require.NoError(t, e.services.Cancel.AssertSatisfied())
	require.NoError(t, e.services.Terminate.AssertSatisfied())
	require.NoError(t, e.services.Migrate.AssertSatisfied())
}

func TestSchedulerPropertyEnvironmentUsesDeliveryRefs(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, 20)
	env.trigger(t)
	env.drain(t, 20)
	require.NotEmpty(t, env.delivered)
	redelivery, err := env.engine.Redeliver(env.engineCtx, env.ref, env.delivered[0])
	require.NoError(t, err)
	require.True(t, redelivery.Result.Dropped)
	env.reload(t)
}
