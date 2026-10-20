package wci

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	testNamespaceID = "ns-id"
	testInstanceID  = "temporal-sys-worker-controller-instance:dep:build"
)

type fakeCompute struct {
	mu      sync.Mutex
	calls   []string
	sizes   []int32
	callErr error
}

func (f *fakeCompute) record(call string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
	return f.callErr
}

func (f *fakeCompute) ValidateSpec(context.Context, compute.RequestContext, *iface.WorkerControllerInstanceSpec) error {
	return f.record("validate")
}

func (f *fakeCompute) InvokeWorker(context.Context, compute.RequestContext, *iface.ComputeProviderSpec) error {
	return f.record("invoke")
}

func (f *fakeCompute) UpdateWorkerSetSize(_ context.Context, _ compute.RequestContext, _ *iface.ComputeProviderSpec, size int32) error {
	f.mu.Lock()
	f.sizes = append(f.sizes, size)
	f.mu.Unlock()
	return f.record("resize")
}

type fakeFrontend struct {
	workflowservice.WorkflowServiceClient
	resp *workflowservice.DescribeWorkerDeploymentVersionResponse
}

func (f *fakeFrontend) DescribeWorkerDeploymentVersion(context.Context, *workflowservice.DescribeWorkerDeploymentVersionRequest, ...grpc.CallOption) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	if f.resp == nil {
		return &workflowservice.DescribeWorkerDeploymentVersionResponse{}, nil
	}
	return f.resp, nil
}

type fakeHistory struct {
	historyservice.HistoryServiceClient

	startErr error
	starts   []*historyservice.StartWorkflowExecutionRequest
}

func (f *fakeHistory) StartWorkflowExecution(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
	f.starts = append(f.starts, req)
	return &historyservice.StartWorkflowExecutionResponse{}, f.startErr
}

func (*fakeHistory) SignalWorkflowExecution(context.Context, *historyservice.SignalWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.SignalWorkflowExecutionResponse, error) {
	return &historyservice.SignalWorkflowExecutionResponse{}, nil
}

type e2e struct {
	ctx      context.Context
	engine   *chasmtest.Engine
	handler  *handler
	compute  *fakeCompute
	frontend *fakeFrontend
	lib      *Library
	history  *fakeHistory
	metrics  *metricstest.CaptureHandler
}

func newE2E(t *testing.T, script fakeAlgo) *e2e {
	t.Helper()
	*fake = script

	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("ns"), nil).AnyTimes()

	computeClient := &fakeCompute{}
	frontend := &fakeFrontend{}
	h := newHandler(HandlerOptions{
		ComputeClient:     computeClient,
		FrontendClient:    frontend,
		NamespaceRegistry: registry,
		DynamicConfig:     dynamicconfig.NewNoopCollection(),
		Logger:            log.NewNoopLogger(),
	})
	metricsHandler := metricstest.NewCaptureHandler()
	snapshotOpts := SnapshotTaskHandlerOptions{FrontendClient: frontend, Logger: log.NewNoopLogger(), MetricsHandler: metricsHandler}
	history := &fakeHistory{}
	computeOpts := ComputeTaskHandlerOptions{ComputeClient: computeClient, HistoryClient: history, Logger: log.NewNoopLogger()}
	lib := NewLibrary(h,
		NewFetchSnapshotTaskHandler(snapshotOpts), NewDeferredDecisionTaskHandler(snapshotOpts),
		NewResizeTaskHandler(computeOpts), NewInvokeTaskHandler(computeOpts),
		NewValidateSpecTaskHandler(computeOpts), NewSignalVersionWorkflowTaskHandler(computeOpts),
		NewMigrateToWorkflowTaskHandler(MigrateToWorkflowTaskHandlerOptions{HistoryClient: history, DynamicConfig: dynamicconfig.NewNoopCollection(), MetricsHandler: metricsHandler}))

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmRegistry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, chasmRegistry.Register(lib))
	engine := chasmtest.NewEngine(t, chasmRegistry, chasmtest.WithMetricsHandler(metricsHandler))
	return &e2e{ctx: chasm.NewEngineContext(context.Background(), engine), engine: engine, handler: h, compute: computeClient, frontend: frontend, lib: lib, history: history, metrics: metricsHandler}
}

func groupUpsert(provider string) map[string]*wcipb.ScalingGroupSpecUpdate {
	return map[string]*wcipb.ScalingGroupSpecUpdate{"g": {Spec: &wcipb.ScalingGroupSpec{
		TaskTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Compute:   &wcipb.ComputeProviderSpec{ProviderType: provider},
	}}}
}

func (e *e2e) update(upserts map[string]*wcipb.ScalingGroupSpecUpdate, remove ...string) (*wcipb.UpdateWorkerControllerInstanceResponse, error) {
	return e.handler.UpdateWorkerControllerInstance(e.ctx, &wcipb.UpdateWorkerControllerInstanceRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID, DeploymentName: "dep", BuildId: "build",
		Identity: "me", UpsertScalingGroups: upserts, RemoveScalingGroups: remove,
	})
}

func (e *e2e) describe() (*wcipb.DescribeWorkerControllerInstanceResponse, error) {
	return e.handler.DescribeWorkerControllerInstance(e.ctx, &wcipb.DescribeWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
}

func (e *e2e) recordTaskAdd() error {
	_, err := e.handler.RecordTaskAdd(e.ctx, &wcipb.RecordTaskAddRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID,
		Event: &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	})
	return err
}

// live returns the engine's root and group "g".
func (e *e2e) live(t *testing.T) (*WorkerControllerInstance, *ScalingGroup) {
	t.Helper()
	type out struct {
		w *WorkerControllerInstance
		g *ScalingGroup
	}
	o, err := chasm.ReadComponent(e.ctx, refFor(testNamespaceID, testInstanceID), func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (out, error) {
		g, _ := w.ScalingGroups["g"].TryGet(cctx)
		return out{w: w, g: g}, nil
	}, nil)
	require.NoError(t, err)
	return o.w, o.g
}

func TestE2E_CreateRegistersAndDescribes(t *testing.T) {
	e := newE2E(t, fakeAlgo{regActions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(1)}}})

	resp, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.Contains(t, resp.GetScalingGroupSpecs(), "g")
	require.Equal(t, []string{"validate", "resize"}, e.compute.calls, "validated, then sized up to register task queues")

	desc, err := e.describe()
	require.NoError(t, err)
	require.Equal(t, "dep", desc.GetDeploymentName())
	require.Equal(t, wcipb.VALIDATION_RESULT_SUCCESS, desc.GetValidationStatus().GetResult())

	_, g := e.live(t)
	require.EqualValues(t, 1, g.AppliedSize, "the registration size is recorded on the group")
}

func TestE2E_RegistrationSkippedWhenQueuesRegistered(t *testing.T) {
	e := newE2E(t, fakeAlgo{regActions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeInvokeWorker}}})
	e.frontend.resp = &workflowservice.DescribeWorkerDeploymentVersionResponse{
		VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{{Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW}},
	}
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.Equal(t, []string{"validate"}, e.compute.calls)
}

func TestE2E_InvalidUpdateWritesNothing(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	e.compute.callErr = errors.New("bad provider config")
	_, err := e.update(groupUpsert("test-invoke"))
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)

	_, err = e.describe()
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
}

func TestE2E_UnsupportedProviderStaysWorkflow(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("aws-lambda"))
	require.ErrorIs(t, err, ErrProviderNotSupportedOnCHASM)
}

func TestE2E_TaskAddThenResizeEndToEnd(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)

	fake.actions = []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(4)}}
	require.NoError(t, e.recordTaskAdd())
	_, g := e.live(t)
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_SCALING, g.Status)

	dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, g, chasm.SideEffectTaskHandler[*ScalingGroup, *wcipb.ResizeTask](e.lib.ResizeTaskHandler), chasm.TaskAttributes{Destination: "test-invoke"}, &wcipb.ResizeTask{Attempt: g.Attempt})
	require.NoError(t, err)
	require.False(t, dropped)

	_, g = e.live(t)
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_IDLE, g.Status)
	require.EqualValues(t, 4, g.AppliedSize)
	require.Equal(t, []int32{4}, e.compute.sizes)
}

func TestE2E_EmitsWorkflowMetricsTaggedCHASM(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	capture := e.metrics.StartCapture()
	defer e.metrics.StopCapture(capture)

	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	fake.actions = []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(4)}, {Action: scalingalgorithm.ActionTypeInvokeWorker}}
	require.NoError(t, e.recordTaskAdd())
	_, g := e.live(t)
	_, err = chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, g, chasm.SideEffectTaskHandler[*ScalingGroup, *wcipb.ResizeTask](e.lib.ResizeTaskHandler), chasm.TaskAttributes{Destination: "test-invoke"}, &wcipb.ResizeTask{Attempt: g.Attempt})
	require.NoError(t, err)
	require.NoError(t, e.migrateToWorkflow())
	e.runMigrateTask(t)

	snap := capture.Snapshot()
	for _, name := range []string{
		wcimetrics.InstanceCreated.Name(), wcimetrics.Signals.Name(), wcimetrics.ScaleUpCount.Name(),
		wcimetrics.Operations.Name(), wcimetrics.TargetWorkerCount.Name(), MigrateToWorkflowCount.Name(),
	} {
		require.NotEmpty(t, snap[name], name)
		for _, r := range snap[name] {
			require.Equal(t, backendCHASM, r.Tags[wcimetrics.BackendTagName], name)
		}
	}
	require.Equal(t, "test-invoke", snap[wcimetrics.ScaleUpCount.Name()][0].Tags[wcimetrics.ComputeProviderTag])
	require.Equal(t, "dep", snap[wcimetrics.ScaleUpCount.Name()][0].Tags[wcimetrics.WorkerDeploymentNameTag])
	require.InDelta(t, 4.0, snap[wcimetrics.TargetWorkerCount.Name()][0].Value, 0)
	resize := snap[wcimetrics.Operations.Name()][0]
	require.Equal(t, wcimetrics.OperationTypeUpdateWorkerSetSize, resize.Tags[wcimetrics.OperationTagName])
	require.Equal(t, string(wcimetrics.ErrorTypeNone), resize.Tags[wcimetrics.ErrorTypeTagName])
	require.Equal(t, migrationOutcomeSuccess, snap[MigrateToWorkflowCount.Name()][0].Tags[wcimetrics.MigrationOutcomeTagName])
}

func TestE2E_FetchSnapshotRunsPoll(t *testing.T) {
	next := 2 * minPollInterval
	e := newE2E(t, fakeAlgo{nextPoll: &next})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	e.frontend.resp = &workflowservice.DescribeWorkerDeploymentVersionResponse{
		VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
			{Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Stats: &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 5}},
		},
	}

	w, _ := e.live(t)
	before := w.NextPollTime.AsTime()
	dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.FetchSnapshotTask](e.lib.FetchSnapshotTaskHandler), chasm.TaskAttributes{}, &wcipb.FetchSnapshotTask{PollTime: w.NextPollTime})
	require.NoError(t, err)
	require.False(t, dropped)

	require.EqualValues(t, 5, fake.seen.Workflow.LastBacklogCount)
	w, _ = e.live(t)
	require.NotEqual(t, before, w.NextPollTime.AsTime(), "the poll re-armed the timer")
}

func TestE2E_DescribeReturnsTaskQueueObservations(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	e.frontend.resp = &workflowservice.DescribeWorkerDeploymentVersionResponse{
		VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
			{Name: "orders", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Stats: &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 5}},
		},
	}
	w, _ := e.live(t)
	_, err = chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.FetchSnapshotTask](e.lib.FetchSnapshotTaskHandler), chasm.TaskAttributes{}, &wcipb.FetchSnapshotTask{PollTime: w.NextPollTime})
	require.NoError(t, err)
	require.NoError(t, e.recordTaskAdd())

	resp, err := e.describe()
	require.NoError(t, err)
	o := resp.GetTaskQueueObservations()["g"]
	require.Len(t, o.GetTaskQueues(), 1)
	require.Equal(t, "orders", o.GetTaskQueues()[0].GetName())
	require.EqualValues(t, 5, o.GetTaskQueues()[0].GetBacklogCount())
	require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW, o.GetLastTaskAdd().GetTaskQueueType())
	require.NotNil(t, o.GetLastTaskAddTime())
}

func TestE2E_DeleteThenTaskAddIsClosed(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)

	_, err = e.handler.DeleteWorkerControllerInstance(e.ctx, &wcipb.DeleteWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
	require.NoError(t, err)

	fake.actions = []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(4)}}
	require.ErrorIs(t, e.recordTaskAdd(), ErrClosed)
	require.Zero(t, fake.taskAddCalls, "a deleted instance never runs the algorithm")
	_, err = e.describe()
	require.ErrorIs(t, err, ErrClosed)
}

func TestE2E_TaskAddOnMissingIsNotFound(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, e.recordTaskAdd(), &notFound)
}

func TestE2E_ZeroGroupsDeletes(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	_, err = e.update(nil, "g")
	require.NoError(t, err)
	_, err = e.describe()
	require.ErrorIs(t, err, ErrClosed)
}

func TestE2E_ConflictTokenMismatch(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	_, err = e.handler.UpdateWorkerControllerInstance(e.ctx, &wcipb.UpdateWorkerControllerInstanceRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID, ConflictToken: []byte("stale"),
		UpsertScalingGroups: groupUpsert("test-invoke"),
	})
	var precondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &precondition)
}

func TestE2E_CreateFromMigrationReplaysOnceAndIsIdempotent(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	req := &wcipb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID,
		State: &wcipb.WorkerControllerInstanceState{
			DeploymentName: "dep", BuildId: "build",
			ScalingGroupSpecs: groupUpsertSpecs("test-invoke"),
		},
		ScalingGroups:   map[string]*wcipb.ScalingGroupState{"g": NewScalingGroup("g").ScalingGroupState},
		PendingTaskAdds: []*wcipb.TaskAddEvent{{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}, {TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}},
	}

	_, err := e.handler.CreateFromMigrationState(e.ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, fake.taskAddCalls, "queued task-adds replayed exactly once")

	_, err = e.handler.CreateFromMigrationState(e.ctx, req)
	require.NoError(t, err, "already exists counts as success")
	require.Equal(t, 2, fake.taskAddCalls)
}

func groupUpsertSpecs(provider string) map[string]*wcipb.ScalingGroupSpec {
	return map[string]*wcipb.ScalingGroupSpec{"g": groupUpsert(provider)["g"].GetSpec()}
}

func (e *e2e) migrateToWorkflow() error {
	_, err := e.handler.MigrateToWorkflow(e.ctx, &wcipb.MigrateToWorkflowRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
	return err
}

func (e *e2e) runMigrateTask(t *testing.T) {
	t.Helper()
	w, _ := e.live(t)
	dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.MigrateToWorkflowTask](e.lib.MigrateToWorkflowTaskHandler), chasm.TaskAttributes{}, &wcipb.MigrateToWorkflowTask{})
	require.NoError(t, err)
	require.False(t, dropped)
}

func TestE2E_MigrateToWorkflow(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	w, _ := e.live(t)
	nextPoll := w.NextPollTime.AsTime()

	require.NoError(t, e.migrateToWorkflow())
	require.NoError(t, e.migrateToWorkflow(), "repeating while migrating is a no-op")
	require.ErrorIs(t, e.recordTaskAdd(), ErrMigrationPending, "mutations wait for the hand-off")

	e.runMigrateTask(t)

	require.Len(t, e.history.starts, 1)
	start := e.history.starts[0].GetStartRequest()
	require.Equal(t, testInstanceID, start.GetWorkflowId(), "the workflow ID is the CHASM business ID")
	require.Equal(t, iface.WorkerControllerInstanceWorkflowType, start.GetWorkflowType().GetName())
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, start.GetWorkflowIdConflictPolicy())

	var args iface.WorkerControllerInstanceWorkflowArgs
	require.NoError(t, sdk.PreferProtoDataConverter.FromPayloads(start.GetInput(), &args))
	require.Equal(t, "dep", args.DeploymentName)
	require.Contains(t, args.State.Spec.ScalingGroupSpecs, "g")
	require.Equal(t, nextPoll, args.State.NextPollTime.AsTime(), "the poll cadence carries over")

	w, _ = e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status)
	require.ErrorIs(t, e.recordTaskAdd(), ErrClosed, "the router falls back to the workflow")
	_, err = e.describe()
	require.ErrorIs(t, err, ErrClosed)
}

func TestE2E_MigrateToWorkflowAlreadyStarted(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	e.history.startErr = serviceerror.NewWorkflowExecutionAlreadyStarted("started", "req", "run")

	require.NoError(t, e.migrateToWorkflow())
	e.runMigrateTask(t)

	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status, "already started counts as success")
}

func TestE2E_MigrateToWorkflowOnMissing(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, e.migrateToWorkflow(), &notFound)
}
