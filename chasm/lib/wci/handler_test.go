package wci

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testNamespaceID = "ns-id"
	testInstanceID  = "temporal-sys-worker-controller-instance:dep:build"
)

type fakeCompute struct {
	mu         sync.Mutex
	calls      []string
	sizes      []int32
	requestIDs []string
	callErr    error
	// Provider types the WCI process serves, on top of the in-process defaults.
	remote []string
	// onValidate runs inside ValidateSpec, i.e. while an update holds no lock.
	onValidate func()
}

func (f *fakeCompute) Supports(namespaceName, providerType string) bool {
	return slices.Contains(InProcessComputeProviders.Get(dynamicconfig.NewNoopCollection())(namespaceName), providerType) ||
		slices.Contains(f.remote, providerType)
}

func (f *fakeCompute) record(call string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
	return f.callErr
}

func (f *fakeCompute) ValidateSpec(context.Context, compute.RequestContext, *iface.WorkerControllerInstanceSpec) error {
	if f.onValidate != nil {
		f.onValidate()
	}
	return f.record("validate")
}

func (f *fakeCompute) InvokeWorker(_ context.Context, _ compute.RequestContext, _ *iface.ComputeProviderSpec, requestID string) error {
	f.mu.Lock()
	f.requestIDs = append(f.requestIDs, requestID)
	f.mu.Unlock()
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
	// Activation outcome: nil accepts; updateErr is returned as the call error.
	rejectActivation *failurepb.Failure
	updateErr        error
	updates          []*updatepb.Request
}

func (f *fakeHistory) UpdateWorkflowExecution(_ context.Context, req *historyservice.UpdateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	f.updates = append(f.updates, req.GetRequest().GetRequest())
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	outcome := &updatepb.Outcome{Value: &updatepb.Outcome_Success{}}
	if f.rejectActivation != nil && req.GetRequest().GetRequest().GetInput().GetName() == iface.UpdateActivateMigration {
		outcome = &updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: f.rejectActivation}}
	}
	return &historyservice.UpdateWorkflowExecutionResponse{Response: &workflowservice.UpdateWorkflowExecutionResponse{Outcome: outcome}}, nil
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

func TestE2E_RemoteProviderRunsOnCHASM(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	e.compute.remote = []string{"aws-lambda"}

	_, err := e.update(groupUpsert("aws-lambda"))
	require.NoError(t, err, "served by the WCI process, so it no longer stays a workflow")
	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_RUNNING, w.Status)
	require.Contains(t, e.compute.calls, "validate", "the provider config is validated through the compute service")
	require.NoError(t, e.recordTaskAdd(), "task-adds reach the CHASM instance")
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

func TestE2E_InvokeTaskPassesItsRequestID(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	fake.actions = []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeInvokeWorker}}
	require.NoError(t, e.recordTaskAdd())
	_, g := e.live(t)
	// Copied: finishing the invoke resets the live group's in-flight ID.
	requestID, attempt := g.InflightInvokeId, g.InflightInvokeAttempt
	require.NotZero(t, requestID)

	dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, g, chasm.SideEffectTaskHandler[*ScalingGroup, *wcipb.InvokeTask](e.lib.InvokeTaskHandler), chasm.TaskAttributes{Destination: "test-invoke"}, &wcipb.InvokeTask{RequestId: requestID, Attempt: attempt})
	require.NoError(t, err)
	require.False(t, dropped)
	require.Equal(t, []string{strconv.FormatInt(requestID, 10)}, e.compute.requestIDs, "the provider can de-duplicate retries on it")
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

func TestE2E_UpdateAfterDeleteRecreates(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	first, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	_, err = e.handler.DeleteWorkerControllerInstance(e.ctx, &wcipb.DeleteWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
	require.NoError(t, err)

	_, err = e.updateWithToken(first.GetConflictToken(), groupUpsert("test-invoke"))
	require.ErrorIs(t, err, errConflictTokenMismatch, "a token from the deleted instance never matches")

	second, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.NotEqual(t, first.GetConflictToken(), second.GetConflictToken())
	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_RUNNING, w.Status)
	require.Contains(t, w.ScalingGroupSpecs, "g")
}

func TestE2E_UpdateRacingCloseIsRetryable(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)

	// The instance closes between the update's read and its apply.
	e.compute.onValidate = func() {
		e.compute.onValidate = nil
		_, err := e.handler.DeleteWorkerControllerInstance(e.ctx, &wcipb.DeleteWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
		require.NoError(t, err)
	}
	_, err = e.update(groupUpsert("test-invoke"))
	require.ErrorIs(t, err, errClosedDuringUpdate)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable, "retryable, so the router re-probes")
	_, err = e.describe()
	require.ErrorIs(t, err, ErrClosed, "no new execution replaced the closed one")

	_, err = e.update(groupUpsert("test-invoke"))
	require.NoError(t, err, "the retry re-creates")
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

func (e *e2e) updateWithToken(token []byte, upserts map[string]*wcipb.ScalingGroupSpecUpdate, remove ...string) (*wcipb.UpdateWorkerControllerInstanceResponse, error) {
	return e.handler.UpdateWorkerControllerInstance(e.ctx, &wcipb.UpdateWorkerControllerInstanceRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID, DeploymentName: "dep", BuildId: "build",
		Identity: "me", ConflictToken: token, UpsertScalingGroups: upserts, RemoveScalingGroups: remove,
	})
}

func TestE2E_ConflictTokenRotates(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	created, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.NotEmpty(t, created.GetConflictToken(), "minted at create")

	// An update without a token still rotates it, so earlier holders go stale.
	untokened, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.NotEqual(t, created.GetConflictToken(), untokened.GetConflictToken())
	_, err = e.updateWithToken(created.GetConflictToken(), groupUpsert("test-invoke"))
	require.ErrorIs(t, err, errConflictTokenMismatch)

	tokened, err := e.updateWithToken(untokened.GetConflictToken(), groupUpsert("test-invoke"))
	require.NoError(t, err)
	desc, err := e.describe()
	require.NoError(t, err)
	require.Equal(t, tokened.GetConflictToken(), desc.GetConflictToken())

	// Second holder of the same token loses.
	_, err = e.updateWithToken(untokened.GetConflictToken(), groupUpsert("test-invoke"))
	require.ErrorIs(t, err, errConflictTokenMismatch)
}

func TestE2E_ConflictTokenOnImplicitDelete(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	created, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	_, err = e.updateWithToken([]byte("stale"), nil, "g")
	require.ErrorIs(t, err, errConflictTokenMismatch, "implicit delete checks the token too")
	deleted, err := e.updateWithToken(created.GetConflictToken(), nil, "g")
	require.NoError(t, err)
	require.NotEqual(t, created.GetConflictToken(), deleted.GetConflictToken())
}

func TestE2E_ConflictTokenForMissingInstance(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.updateWithToken([]byte("any"), groupUpsert("test-invoke"))
	require.ErrorIs(t, err, errConflictTokenMismatch)
}

// The pre-read check runs without the lock; this is the re-check that serialises same-token updates.
func TestCheckConflictToken(t *testing.T) {
	w := &WorkerControllerInstance{WorkerControllerInstanceState: &wcipb.WorkerControllerInstanceState{ConflictToken: []byte("cur")}}
	require.NoError(t, w.checkConflictToken(nil))
	require.NoError(t, w.checkConflictToken([]byte("cur")))
	require.ErrorIs(t, w.checkConflictToken([]byte("old")), errConflictTokenMismatch)
}

const testToken = "run-1"

func (e *e2e) prepare(token string, expireIn time.Duration) error {
	_, err := e.handler.CreateFromMigrationState(e.ctx, &wcipb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID,
		State: &wcipb.WorkerControllerInstanceState{
			DeploymentName: "dep", BuildId: "build",
			ScalingGroupSpecs: groupUpsertSpecs("test-invoke"),
		},
		ScalingGroups:   map[string]*wcipb.ScalingGroupState{"g": NewScalingGroup("g").ScalingGroupState},
		PendingTaskAdds: []*wcipb.TaskAddEvent{{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}},
		MigrationToken:  token,
		ExpireTime:      timestamppb.New(time.Now().Add(expireIn)),
	})
	return err
}

func (e *e2e) activate(token string, taskAdds int) error {
	events := make([]*wcipb.TaskAddEvent, taskAdds)
	for i := range events {
		events[i] = &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}
	}
	_, err := e.handler.ActivateMigration(e.ctx, &wcipb.ActivateMigrationRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID, MigrationToken: token, PendingTaskAdds: events,
	})
	return err
}

func (e *e2e) abort(token string) error {
	_, err := e.handler.AbortMigration(e.ctx, &wcipb.AbortMigrationRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID, MigrationToken: token})
	return err
}

func TestE2E_PreparedTargetIsInertAndInvisible(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	require.NoError(t, e.prepare(testToken, time.Minute))
	require.NoError(t, e.prepare(testToken, time.Minute), "a repeated prepare is a no-op")
	require.Zero(t, fake.taskAddCalls, "nothing replays at prepare")

	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_PENDING_ACTIVATION, w.Status)
	var notFound *serviceerror.NotFound
	_, err := e.describe()
	require.ErrorAs(t, err, &notFound, "the router keeps using the frozen workflow")
	require.ErrorAs(t, e.recordTaskAdd(), &notFound)

	var exists *serviceerror.AlreadyExists
	require.ErrorAs(t, e.prepare("run-2", time.Minute), &exists, "another migration cannot take over")
}

func TestE2E_ActivateCommitsAndReplaysOnce(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	require.NoError(t, e.prepare(testToken, time.Minute))
	require.ErrorIs(t, e.activate("run-2", 0), ErrMigrationNotPending, "the token must match")

	require.NoError(t, e.activate(testToken, 2))
	require.Equal(t, 2, fake.taskAddCalls, "queued task-adds replay in the activating transaction")
	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_RUNNING, w.Status)
	require.NotNil(t, w.NextPollTime, "the cadence starts at activation")

	require.NoError(t, e.activate(testToken, 2), "a retried activation is a no-op")
	require.Equal(t, 2, fake.taskAddCalls)
	require.ErrorIs(t, e.abort(testToken), ErrMigrationNotPending, "an activated target cannot be aborted")
}

func TestE2E_AbortAndExpiryCloseThePendingTarget(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	require.NoError(t, e.prepare(testToken, time.Minute))
	require.NoError(t, e.abort(testToken))
	require.NoError(t, e.abort(testToken), "a repeated abort is a no-op")
	require.ErrorIs(t, e.activate(testToken, 0), ErrMigrationNotPending)
	require.NoError(t, e.prepare("run-2", time.Minute), "an aborted target does not block the next migration")

	w, _ := e.live(t)
	dropped, err := chasmtest.ExecutePureTask(e.ctx, e.engine, w, &PendingExpiryTaskHandler{}, chasm.TaskAttributes{}, &wcipb.PendingExpiryTask{ExpireTime: w.PendingExpireTime})
	require.NoError(t, err)
	require.False(t, dropped)
	w, _ = e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATION_ABORTED, w.Status, "an unactivated target expires")
	require.ErrorIs(t, e.activate("run-2", 0), ErrMigrationNotPending)
}

// The first activation commits but its response is lost; the retry must not read as an abort,
// or the source workflow unfreezes and revives a WCI that has moved on.
func TestE2E_RetriedActivationAfterTargetMovesOn(t *testing.T) {
	tests := []struct {
		name   string
		moveOn func(*testing.T, *e2e)
		status wcipb.WciStatus
	}{
		{name: "deleted", status: wcipb.WCI_STATUS_DELETED, moveOn: func(t *testing.T, e *e2e) {
			_, err := e.handler.DeleteWorkerControllerInstance(e.ctx, &wcipb.DeleteWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
			require.NoError(t, err)
		}},
		{name: "implicitly_deleted", status: wcipb.WCI_STATUS_DELETED, moveOn: func(t *testing.T, e *e2e) {
			_, err := e.update(nil, "g")
			require.NoError(t, err)
		}},
		{name: "migrating_back", status: wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, moveOn: func(t *testing.T, e *e2e) {
			require.NoError(t, e.migrateToWorkflow())
		}},
		{name: "migrated_back", status: wcipb.WCI_STATUS_MIGRATED, moveOn: func(t *testing.T, e *e2e) {
			require.NoError(t, e.migrateToWorkflow())
			e.runMigrateTask(t)
		}},
		// An earlier run's timed-out prepare lands after the delete and replaces the deleted execution.
		{name: "deleted_then_late_prepare", status: wcipb.WCI_STATUS_PENDING_ACTIVATION, moveOn: func(t *testing.T, e *e2e) {
			_, err := e.handler.DeleteWorkerControllerInstance(e.ctx, &wcipb.DeleteWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
			require.NoError(t, err)
			require.NoError(t, e.prepare("run-0", time.Minute))
		}},
		{name: "deleted_then_recreated", status: wcipb.WCI_STATUS_RUNNING, moveOn: func(t *testing.T, e *e2e) {
			_, err := e.handler.DeleteWorkerControllerInstance(e.ctx, &wcipb.DeleteWorkerControllerInstanceRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
			require.NoError(t, err)
			_, err = e.update(groupUpsert("test-invoke"))
			require.NoError(t, err)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newE2E(t, fakeAlgo{})
			require.NoError(t, e.prepare(testToken, time.Minute))
			require.NoError(t, e.activate(testToken, 1))
			tc.moveOn(t, e)
			w, _ := e.live(t)
			require.Equal(t, tc.status, w.Status)

			require.NoError(t, e.activate(testToken, 1))
			require.Equal(t, 1, fake.taskAddCalls, "the retry replays nothing")
			require.ErrorIs(t, e.activate("run-2", 0), ErrMigrationNotPending)
		})
	}
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

	require.NotNil(t, args.PendingMigration, "the workflow starts pending")
	require.Equal(t, args.PendingMigration.Token, start.GetRequestId(), "the token de-duplicates a retried start")
	require.Len(t, e.history.updates, 1)
	require.Equal(t, iface.UpdateActivateMigration, e.history.updates[0].GetInput().GetName())

	w, _ = e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status)
	require.ErrorIs(t, e.recordTaskAdd(), ErrClosed, "the router falls back to the workflow")
	_, err = e.describe()
	require.ErrorIs(t, err, ErrClosed)
}

func TestE2E_MigrateToWorkflowAborts(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*fakeHistory)
	}{
		{name: "another_run_owns_the_id", setup: func(h *fakeHistory) {
			h.startErr = serviceerror.NewWorkflowExecutionAlreadyStarted("started", "req", "run")
			h.rejectActivation = &failurepb.Failure{Message: "unknown update"}
		}},
		{name: "activation_rejected", setup: func(h *fakeHistory) { h.rejectActivation = &failurepb.Failure{Message: "token mismatch"} }},
		{name: "workflow_expired", setup: func(h *fakeHistory) { h.updateErr = serviceerror.NewNotFound("workflow execution already completed") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newE2E(t, fakeAlgo{})
			_, err := e.update(groupUpsert("test-invoke"))
			require.NoError(t, err)
			tc.setup(e.history)

			require.NoError(t, e.migrateToWorkflow())
			e.runMigrateTask(t)

			w, _ := e.live(t)
			require.Equal(t, wcipb.WCI_STATUS_RUNNING, w.Status, "CHASM keeps the WCI")
			require.Empty(t, w.MigrationToken)
			require.NoError(t, e.recordTaskAdd(), "scaling resumes")
		})
	}
}

// The activate response was lost and the activated run continued as new before the task retried:
// the retried start finds another run, which still accepts the activation.
func TestE2E_MigrateToWorkflowActivatedRunContinuedAsNew(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	e.history.startErr = serviceerror.NewWorkflowExecutionAlreadyStarted("started", "req", "run")

	require.NoError(t, e.migrateToWorkflow())
	e.runMigrateTask(t)

	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status, "CHASM must not resume next to the workflow")
	require.Len(t, e.history.updates, 1)
	require.Equal(t, iface.UpdateActivateMigration, e.history.updates[0].GetInput().GetName())
}

func TestE2E_MigrateToWorkflowRetriesAmbiguousActivation(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	e.history.updateErr = serviceerror.NewUnavailable("shard moved")

	require.NoError(t, e.migrateToWorkflow())
	w, _ := e.live(t)
	_, err = chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.MigrateToWorkflowTask](e.lib.MigrateToWorkflowTaskHandler), chasm.TaskAttributes{}, &wcipb.MigrateToWorkflowTask{})
	require.Error(t, err, "the task retries")
	w, _ = e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, w.Status, "still frozen")

	e.history.updateErr = nil
	e.runMigrateTask(t)
	w, _ = e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status)
	require.Equal(t, e.history.starts[0].GetStartRequest().GetRequestId(), e.history.starts[1].GetStartRequest().GetRequestId(), "the retried start is a duplicate")
}

func TestE2E_MigrateToWorkflowOnMissing(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, e.migrateToWorkflow(), &notFound)
}
