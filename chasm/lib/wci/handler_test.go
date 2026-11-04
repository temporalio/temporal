package wci

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
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
	wciconfig "go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	historytasks "go.temporal.io/server/service/history/tasks"
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
	// Group IDs the last ValidateSpec saw.
	validated []string
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

func (f *fakeCompute) ValidateSpec(_ context.Context, _ compute.RequestContext, spec *iface.WorkerControllerInstanceSpec) error {
	f.mu.Lock()
	f.validated = slices.Sorted(maps.Keys(spec.ScalingGroupSpecs))
	f.mu.Unlock()
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
	// onDescribe runs while the fetch is outside the lock, before its write-back.
	onDescribe func()
}

func (f *fakeFrontend) DescribeWorkerDeploymentVersion(context.Context, *workflowservice.DescribeWorkerDeploymentVersionRequest, ...grpc.CallOption) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	if f.onDescribe != nil {
		f.onDescribe()
	}
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
	// onUpdate runs while the migrate task waits on the update, outside the lock.
	onUpdate func()
}

func (f *fakeHistory) UpdateWorkflowExecution(_ context.Context, req *historyservice.UpdateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	f.updates = append(f.updates, req.GetRequest().GetRequest())
	if f.onUpdate != nil {
		f.onUpdate()
	}
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
	clock    *clock.EventTimeSource
	registry *chasm.Registry
}

func newE2E(t *testing.T, script fakeAlgo) *e2e {
	t.Helper()
	return newE2EInRegion(t, script, "")
}

// newE2EInRegion wires the handlers with the history's host region.
func newE2EInRegion(t *testing.T, script fakeAlgo, region string) *e2e {
	t.Helper()
	*fake = script
	staticConfig := &wciconfig.StaticConfig{RegionID: region}

	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("ns"), nil).AnyTimes()

	computeClient := &fakeCompute{}
	frontend := &fakeFrontend{}
	h := newHandler(HandlerOptions{
		ComputeClient:     computeClient,
		FrontendClient:    frontend,
		NamespaceRegistry: registry,
		Logger:            log.NewNoopLogger(),
		StaticConfig:      staticConfig,
	})
	metricsHandler := metricstest.NewCaptureHandler()
	snapshotOpts := SnapshotTaskHandlerOptions{FrontendClient: frontend, Logger: log.NewNoopLogger(), MetricsHandler: metricsHandler, StaticConfig: staticConfig}
	history := &fakeHistory{}
	computeOpts := ComputeTaskHandlerOptions{ComputeClient: computeClient, HistoryClient: history, Logger: log.NewNoopLogger(), StaticConfig: staticConfig}
	lib := NewLibrary(h,
		NewFetchSnapshotTaskHandler(snapshotOpts), NewDeferredDecisionTaskHandler(snapshotOpts),
		NewResizeTaskHandler(computeOpts), NewInvokeTaskHandler(computeOpts),
		NewValidateSpecTaskHandler(computeOpts), NewSignalVersionWorkflowTaskHandler(computeOpts),
		NewMigrateToWorkflowTaskHandler(MigrateToWorkflowTaskHandlerOptions{HistoryClient: history, DynamicConfig: dynamicconfig.NewNoopCollection(), MetricsHandler: metricsHandler, StaticConfig: staticConfig}),
		NewForwardTaskAddsTaskHandler(MigrateToWorkflowTaskHandlerOptions{HistoryClient: history}))

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmRegistry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, chasmRegistry.Register(lib))
	ts := clock.NewEventTimeSource()
	ts.Update(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	engine := chasmtest.NewEngine(t, chasmRegistry, chasmtest.WithMetricsHandler(metricsHandler), chasmtest.WithTimeSource(ts))
	return &e2e{ctx: chasm.NewEngineContext(context.Background(), engine), engine: engine, handler: h, compute: computeClient, frontend: frontend, lib: lib, history: history, metrics: metricsHandler, clock: ts, registry: chasmRegistry}
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

func TestE2E_RegionScopedSpecsStayWorkflows(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	regional := groupUpsert("test-invoke")
	regional["g"].Spec.RegionIds = []string{"us-east-1"}

	_, err := e.update(regional)
	require.ErrorIs(t, err, ErrRegionsNotSupportedOnCHASM)
	require.Empty(t, e.compute.calls, "rejected before any provider call")
	require.ErrorIs(t, e.prepareSpecs(testToken, time.Minute, map[string]*wcipb.ScalingGroupSpec{"g": regional["g"].GetSpec()}), ErrRegionsNotSupportedOnCHASM, "the handover is declined")

	var notFound *serviceerror.NotFound
	_, err = e.describe()
	require.ErrorAs(t, err, &notFound, "nothing was created")
}

func TestE2E_CreateFromMigrationStateValidatesSpec(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	specs := groupUpsertSpecs("test-invoke")
	specs["h"] = specs["g"]
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, e.prepareSpecs(testToken, time.Minute, specs), &invalid, "two groups claim the same task type")
}

func TestE2E_SharedWorkerSetRejected(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	e.compute.remote = []string{"gcp-cloud-run"}
	pool, err := sdk.PreferProtoDataConverter.ToPayload(map[string]string{"project": "p", "region": "r", "worker_pool": "pool"})
	require.NoError(t, err)
	specs := map[string]*wcipb.ScalingGroupSpec{}
	for id, taskType := range map[string]enumspb.TaskQueueType{"g": enumspb.TASK_QUEUE_TYPE_WORKFLOW, "h": enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
		specs[id] = &wcipb.ScalingGroupSpec{
			TaskTypes: []enumspb.TaskQueueType{taskType},
			Compute:   &wcipb.ComputeProviderSpec{ProviderType: "gcp-cloud-run", Config: pool},
		}
	}
	upserts := map[string]*wcipb.ScalingGroupSpecUpdate{}
	for id, spec := range specs {
		upserts[id] = &wcipb.ScalingGroupSpecUpdate{Spec: spec}
	}

	var invalid *serviceerror.InvalidArgument
	_, err = e.update(upserts)
	require.ErrorAs(t, err, &invalid)
	require.ErrorContains(t, err, "both resize gcp-cloud-run worker set p/r/pool")
	require.Empty(t, e.compute.calls, "rejected before any provider call")
	require.ErrorAs(t, e.prepareSpecs(testToken, time.Minute, specs), &invalid, "the handover is declined")
}

func TestE2E_UpdateValidatesOnlyChangedCompute(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.Equal(t, []string{"g"}, e.compute.validated)

	added := map[string]*wcipb.ScalingGroupSpecUpdate{"h": {Spec: &wcipb.ScalingGroupSpec{
		TaskTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		Compute:   &wcipb.ComputeProviderSpec{ProviderType: "test-invoke"},
	}}}
	_, err = e.update(added)
	require.NoError(t, err)
	require.Equal(t, []string{"h"}, e.compute.validated, "g's compute is unchanged")

	_, err = e.handler.ValidateWorkerControllerInstanceSpec(e.ctx, &wcipb.ValidateWorkerControllerInstanceSpecRequest{NamespaceId: testNamespaceID, InstanceId: testInstanceID})
	require.NoError(t, err)
	require.Equal(t, []string{"g", "h"}, e.compute.validated, "a request without changes validates every group")
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

// A regional group's retried invokes and resizes keep their region's breaker key.
func TestE2E_RegionalDestinationsSurviveRetries(t *testing.T) {
	const east = "aws-us-east-1"
	want := "test-invoke/" + east
	transient := compute.NewProviderError(compute.FailureUnavailable, errors.New("down"))
	setup := func(t *testing.T, action scalingalgorithm.ScalingAction) (*e2e, *ScalingGroup) {
		e := newE2EInRegion(t, fakeAlgo{}, east)
		_, err := e.update(groupUpsert("test-invoke"))
		require.NoError(t, err)
		// The handler rejects regional specs until 8d.2f; scope the group directly.
		_, _, err = chasm.UpdateComponent(e.ctx, refFor(testNamespaceID, testInstanceID), func(w *WorkerControllerInstance, _ chasm.MutableContext, _ any) (struct{}, error) {
			w.ScalingGroupSpecs["g"].RegionIds = []string{east}
			return struct{}{}, nil
		}, nil)
		require.NoError(t, err)
		fake.actions = []scalingalgorithm.ScalingAction{action}
		require.NoError(t, e.recordTaskAdd())
		e.compute.callErr = transient
		_, g := e.live(t)
		return e, g
	}

	t.Run("invoke", func(t *testing.T) {
		e, g := setup(t, scalingalgorithm.ScalingAction{Action: scalingalgorithm.ActionTypeInvokeWorker})
		_, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, g, chasm.SideEffectTaskHandler[*ScalingGroup, *wcipb.InvokeTask](e.lib.InvokeTaskHandler), chasm.TaskAttributes{}, &wcipb.InvokeTask{RequestId: g.InflightInvokeId, Attempt: g.InflightInvokeAttempt})
		require.NoError(t, err)
		require.Equal(t, []string{want, want}, e.destinations(t, "invoke"), "first attempt and its retry")
	})
	t.Run("resize_after_backoff", func(t *testing.T) {
		e, g := setup(t, scalingalgorithm.ScalingAction{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(4)})
		_, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, g, chasm.SideEffectTaskHandler[*ScalingGroup, *wcipb.ResizeTask](e.lib.ResizeTaskHandler), chasm.TaskAttributes{}, &wcipb.ResizeTask{Attempt: g.Attempt})
		require.NoError(t, err)
		_, g = e.live(t)
		require.Equal(t, wcipb.SCALING_GROUP_STATUS_BACKING_OFF, g.Status)
		_, err = chasmtest.ExecutePureTask(e.ctx, e.engine, g, chasm.PureTaskHandler[*ScalingGroup, *wcipb.ScalingGroupBackoffTask](&ScalingGroupBackoffTaskHandler{}), chasm.TaskAttributes{ScheduledTime: g.NextAttemptTime.AsTime()}, &wcipb.ScalingGroupBackoffTask{Attempt: g.Attempt})
		require.NoError(t, err)
		require.Equal(t, []string{want, want}, e.destinations(t, "resize"), "first attempt and the one after backoff")
	})
}

// A resize for a group that serves nothing in the host region gets the workflow's two attempts, e.g. one
// carried over a failover whose old region is down; a group serving here keeps retrying.
func TestE2E_ResizeOutsideHostRegionGivesUp(t *testing.T) {
	const east, west = "aws-us-east-1", "aws-us-west-2"
	for _, tc := range []struct {
		name     string
		failover bool
		want     wcipb.ScalingGroupStatus
	}{
		{name: "carried_over", failover: true, want: wcipb.SCALING_GROUP_STATUS_IDLE},
		{name: "serves_here", want: wcipb.SCALING_GROUP_STATUS_BACKING_OFF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newE2EInRegion(t, fakeAlgo{}, east)
			_, err := e.update(groupUpsert("test-invoke"))
			require.NoError(t, err)
			_, _, err = chasm.UpdateComponent(e.ctx, refFor(testNamespaceID, testInstanceID), func(w *WorkerControllerInstance, _ chasm.MutableContext, _ any) (struct{}, error) {
				w.ScalingGroupSpecs["g"].RegionIds = []string{east}
				return struct{}{}, nil
			}, nil)
			require.NoError(t, err)
			fake.actions = []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(4)}}
			require.NoError(t, e.recordTaskAdd())
			if tc.failover {
				e.moveTo(west)
			}
			e.compute.callErr = compute.NewProviderError(compute.FailureUnavailable, errors.New("down"))

			runResize := func() {
				_, g := e.live(t)
				_, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, g, chasm.SideEffectTaskHandler[*ScalingGroup, *wcipb.ResizeTask](e.lib.ResizeTaskHandler), chasm.TaskAttributes{}, &wcipb.ResizeTask{Attempt: g.Attempt})
				require.NoError(t, err)
			}
			runResize()
			_, g := e.live(t)
			require.Equal(t, wcipb.SCALING_GROUP_STATUS_BACKING_OFF, g.Status, "the first failure backs off either way")
			_, err = chasmtest.ExecutePureTask(e.ctx, e.engine, g, chasm.PureTaskHandler[*ScalingGroup, *wcipb.ScalingGroupBackoffTask](&ScalingGroupBackoffTaskHandler{}), chasm.TaskAttributes{ScheduledTime: g.NextAttemptTime.AsTime()}, &wcipb.ScalingGroupBackoffTask{Attempt: g.Attempt})
			require.NoError(t, err)
			runResize()

			w, g := e.live(t)
			require.Equal(t, tc.want, g.Status)
			require.NotEqual(t, wcipb.VALIDATION_RESULT_FAILED, w.GetValidationStatus().GetResult(), "a region that may be down isn't the customer's error")
			require.Len(t, e.compute.sizes, 2)
		})
	}
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

	require.False(t, e.fetchSnapshot(t, e.firePollTimer(t)))

	require.EqualValues(t, 5, fake.seen.Workflow.LastBacklogCount)
	w, _ := e.live(t)
	require.Equal(t, e.clock.Now().Add(next), w.NextPollTime.AsTime(), "the poll re-armed the timer")
}

func (e *e2e) firePollTimer(t *testing.T) *timestamppb.Timestamp {
	t.Helper()
	w, _ := e.live(t)
	pollTime := w.NextPollTime
	// The queue fires a timer at or after its scheduled time.
	e.clock.Update(pollTime.AsTime())
	dropped, err := chasmtest.ExecutePureTask(e.ctx, e.engine, w, chasm.PureTaskHandler[*WorkerControllerInstance, *wcipb.MetricsPollTimerTask](&MetricsPollTimerTaskHandler{}), chasm.TaskAttributes{ScheduledTime: pollTime.AsTime()}, &wcipb.MetricsPollTimerTask{PollTime: pollTime})
	require.NoError(t, err, "the poll timer invalidates itself")
	require.False(t, dropped)
	return pollTime
}

func (e *e2e) fetchSnapshot(t *testing.T, pollTime *timestamppb.Timestamp) bool {
	t.Helper()
	w, _ := e.live(t)
	dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.FetchSnapshotTask](e.lib.FetchSnapshotTaskHandler), chasm.TaskAttributes{}, &wcipb.FetchSnapshotTask{PollTime: pollTime})
	require.NoError(t, err)
	return dropped
}

func TestE2E_PollTimerArmsFallbackUntilFetchWritesBack(t *testing.T) {
	next := 2 * minPollInterval
	e := newE2E(t, fakeAlgo{nextPoll: &next})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)

	pollTime := e.firePollTimer(t)
	w, _ := e.live(t)
	require.Equal(t, pollTime.AsTime(), w.InFlightPollTime.AsTime())
	require.Equal(t, e.clock.Now().Add(maxPollInterval), w.NextPollTime.AsTime(), "a fallback poll covers a lost fetch")

	require.False(t, e.fetchSnapshot(t, pollTime))
	w, _ = e.live(t)
	require.Nil(t, w.InFlightPollTime)
	require.Equal(t, e.clock.Now().Add(next), w.NextPollTime.AsTime(), "the fetch re-arms at the algorithm's interval")
}

func TestE2E_SupersededFetchDoesNotApply(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)

	t.Run("at_validation", func(t *testing.T) {
		first := e.firePollTimer(t)
		second := e.firePollTimer(t)
		require.True(t, e.fetchSnapshot(t, first), "the fallback's poll replaced it")
		require.False(t, e.fetchSnapshot(t, second))
	})
	t.Run("at_write_back", func(t *testing.T) {
		first := e.firePollTimer(t)
		var second *timestamppb.Timestamp
		e.frontend.onDescribe = func() {
			e.frontend.onDescribe = nil
			second = e.firePollTimer(t)
		}
		require.False(t, e.fetchSnapshot(t, first))
		w, _ := e.live(t)
		require.Equal(t, second.AsTime(), w.InFlightPollTime.AsTime(), "the stale write-back left the newer poll in flight")
		require.Equal(t, e.clock.Now().Add(maxPollInterval), w.NextPollTime.AsTime())
	})
}

// moveTo points every handler at a new host region, as after a failover.
func (e *e2e) moveTo(region string) {
	e.handler.hostRegion = region
	e.lib.FetchSnapshotTaskHandler.hostRegion = region
	e.lib.DeferredDecisionTaskHandler.hostRegion = region
	e.lib.MigrateToWorkflowTaskHandler.hostRegion = region
	e.lib.ResizeTaskHandler.hostRegion = region
}

func TestE2E_RecordsHostRegion(t *testing.T) {
	const east, west = "aws-us-east-1", "aws-us-west-2"
	tests := []struct {
		name string
		call func(*testing.T, *e2e)
	}{
		{name: "update", call: func(t *testing.T, e *e2e) {
			_, err := e.update(groupUpsert("test-invoke"))
			require.NoError(t, err)
		}},
		{name: "task_add", call: func(t *testing.T, e *e2e) { require.NoError(t, e.recordTaskAdd()) }},
		{name: "poll", call: func(t *testing.T, e *e2e) { require.False(t, e.fetchSnapshot(t, e.firePollTimer(t))) }},
		{name: "deferred_decision", call: func(t *testing.T, e *e2e) {
			w, _ := e.live(t)
			task := &wcipb.DeferredDecisionTask{ScalingGroupId: "g", TaskAdd: &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}}
			dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.DeferredDecisionTask](e.lib.DeferredDecisionTaskHandler), chasm.TaskAttributes{}, task)
			require.NoError(t, err)
			require.False(t, dropped)
		}},
		{name: "move_back_aborted", call: func(t *testing.T, e *e2e) {
			require.NoError(t, e.migrateToWorkflow())
			e.history.rejectActivation = &failurepb.Failure{Message: "token mismatch"}
			e.runMigrateTask(t)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newE2EInRegion(t, fakeAlgo{}, east)
			_, err := e.update(groupUpsert("test-invoke"))
			require.NoError(t, err)
			w, _ := e.live(t)
			require.Equal(t, east, w.HostRegion, "recorded at create")

			e.moveTo(west)
			tc.call(t, e)
			w, _ = e.live(t)
			require.Equal(t, west, w.HostRegion)
		})
	}
}

func TestE2E_ActivationRecordsHostRegion(t *testing.T) {
	e := newE2EInRegion(t, fakeAlgo{}, "aws-us-west-2")
	require.NoError(t, e.prepare(testToken, time.Minute))
	require.NoError(t, e.activate(testToken, 0))
	w, _ := e.live(t)
	require.Equal(t, "aws-us-west-2", w.HostRegion)

	e.moveTo("aws-us-east-1")
	require.NoError(t, e.activateSeq(testToken, 1, 1), "a forward after activation")
	w, _ = e.live(t)
	require.Equal(t, "aws-us-east-1", w.HostRegion)
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
	require.False(t, e.fetchSnapshot(t, e.firePollTimer(t)))
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

// A poll overdue past the margin (its timer discarded on a standby before failover) is re-armed by the next task-add or update.
func TestE2E_OverduePollReArmed(t *testing.T) {
	tests := []struct {
		name string
		call func(*e2e) error
	}{
		{name: "task_add", call: func(e *e2e) error { return e.recordTaskAdd() }},
		{name: "update", call: func(e *e2e) error { _, err := e.update(groupUpsert("test-invoke")); return err }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newE2E(t, fakeAlgo{})
			_, err := e.update(groupUpsert("test-invoke"))
			require.NoError(t, err)
			w, _ := e.live(t)
			nextPoll := w.NextPollTime.AsTime()

			e.clock.Update(nextPoll.Add(maxPollInterval))
			require.NoError(t, tc.call(e))
			w, _ = e.live(t)
			require.Equal(t, nextPoll, w.NextPollTime.AsTime(), "within the margin the timer may just be late")

			e.clock.Advance(time.Second)
			require.NoError(t, tc.call(e))
			w, _ = e.live(t)
			require.Equal(t, e.clock.Now(), w.NextPollTime.AsTime())
		})
	}
}

// Spec changes stamp LastUpdateTime (state and memo); other transactions don't.
func TestE2E_LastUpdateTime(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	created := e.clock.Now()
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	w, _ := e.live(t)
	require.Equal(t, created, w.LastUpdateTime.AsTime())

	e.clock.Advance(time.Minute)
	require.NoError(t, e.recordTaskAdd())
	w, _ = e.live(t)
	require.Equal(t, created, w.LastUpdateTime.AsTime(), "task-adds aren't spec changes")

	e.clock.Advance(time.Minute)
	updated := e.clock.Now()
	_, err = e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	w, _ = e.live(t)
	require.Equal(t, updated, w.LastUpdateTime.AsTime())
	require.Equal(t, updated, w.Memo(nil).(*wcipb.WorkerControllerInstanceMemo).GetLastUpdateTime().AsTime())
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
	return e.prepareSpecs(token, expireIn, groupUpsertSpecs("test-invoke"))
}

func (e *e2e) prepareSpecs(token string, expireIn time.Duration, specs map[string]*wcipb.ScalingGroupSpec) error {
	_, err := e.handler.CreateFromMigrationState(e.ctx, &wcipb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID,
		State: &wcipb.WorkerControllerInstanceState{
			DeploymentName: "dep", BuildId: "build",
			ScalingGroupSpecs: specs,
		},
		ScalingGroups:   map[string]*wcipb.ScalingGroupState{"g": NewScalingGroup("g").ScalingGroupState},
		PendingTaskAdds: []*wcipb.TaskAddEvent{{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}},
		MigrationToken:  token,
		ExpireTime:      timestamppb.New(time.Now().Add(expireIn)),
	})
	return err
}

func (e *e2e) activate(token string, taskAdds int) error {
	return e.activateSeq(token, 0, taskAdds)
}

// activateSeq sends taskAdds queued task-adds numbered from first; 0 leaves them unnumbered.
func (e *e2e) activateSeq(token string, first int64, taskAdds int) error {
	events := make([]*wcipb.TaskAddEvent, taskAdds)
	for i := range events {
		events[i] = &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}
	}
	_, err := e.handler.ActivateMigration(e.ctx, &wcipb.ActivateMigrationRequest{
		NamespaceId: testNamespaceID, InstanceId: testInstanceID, MigrationToken: token, PendingTaskAdds: events,
		FirstTaskAddSeq: first,
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
	require.Equal(t, e.clock.Now(), w.NextPollTime.AsTime(), "polls at once to catch task-adds lost while frozen")

	require.NoError(t, e.activate(testToken, 2), "a retried activation is a no-op")
	require.Equal(t, 2, fake.taskAddCalls)
	require.ErrorIs(t, e.abort(testToken), ErrMigrationNotPending, "an activated target cannot be aborted")
}

func TestE2E_ActivationAppliesEachTaskAddOnce(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	require.NoError(t, e.prepare(testToken, time.Minute))

	require.NoError(t, e.activateSeq(testToken, 1, 2))
	require.Equal(t, 2, fake.taskAddCalls)
	require.NoError(t, e.activateSeq(testToken, 1, 3), "a retry carries the grown queue")
	require.Equal(t, 3, fake.taskAddCalls, "only the new entry applies")
	require.NoError(t, e.activateSeq(testToken, 1, 3))
	require.Equal(t, 3, fake.taskAddCalls, "a repeated retry applies nothing")
	require.NoError(t, e.activateSeq(testToken, 3, 2), "a forward after activation")
	require.Equal(t, 4, fake.taskAddCalls)

	w, _ := e.live(t)
	require.EqualValues(t, 4, w.AppliedTaskAddSeq)
}

// The workflow's forward can land after CHASM started migrating back; its new entries join the queue for the workflow.
func TestE2E_ForwardWhileMigratingBackIsQueued(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	require.NoError(t, e.prepare(testToken, time.Minute))
	require.NoError(t, e.activateSeq(testToken, 1, 1))
	require.NoError(t, e.migrateToWorkflow())

	require.NoError(t, e.activateSeq(testToken, 1, 3))
	require.NoError(t, e.activateSeq(testToken, 1, 3), "a repeated forward queues nothing more")
	require.Equal(t, 1, fake.taskAddCalls, "nothing applies on CHASM while it hands over")
	w, _ := e.live(t)
	require.Len(t, w.MigrationTaskAdds, 2)
	require.EqualValues(t, 3, w.AppliedTaskAddSeq)

	e.runMigrateTask(t)
	require.Len(t, migrationTokenRequest(t, e.history.updates[0]).TaskAdds, 2, "sent to the workflow with the activation")
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
		// Task-adds applied on CHASM after the numbered retry; the first activation applied 1.
		applied int
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
		{name: "migration_back_aborted", status: wcipb.WCI_STATUS_RUNNING, applied: 2, moveOn: func(t *testing.T, e *e2e) {
			require.NoError(t, e.migrateToWorkflow())
			e.history.rejectActivation = &failurepb.Failure{Message: "token mismatch"}
			e.runMigrateTask(t)
		}},
		// The applied mark is carried with the token, so only the new entry applies.
		{name: "deleted_then_recreated", status: wcipb.WCI_STATUS_RUNNING, applied: 2, moveOn: func(t *testing.T, e *e2e) {
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
			require.NoError(t, e.activateSeq(testToken, 1, 1))
			tc.moveOn(t, e)
			w, _ := e.live(t)
			require.Equal(t, tc.status, w.Status)

			require.NoError(t, e.activate(testToken, 1))
			require.NoError(t, e.activateSeq(testToken, 1, 2))
			require.Equal(t, max(1, tc.applied), fake.taskAddCalls, "a running instance takes only the new entry; one that moved on takes none")
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

	require.NoError(t, e.migrateToWorkflow())
	require.NoError(t, e.migrateToWorkflow(), "repeating while migrating is a no-op")
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
	require.Equal(t, e.clock.Now(), args.State.NextPollTime.AsTime(), "the workflow polls at once after activation")

	require.NotNil(t, args.PendingMigration, "the workflow starts pending")
	require.Equal(t, args.PendingMigration.Token, start.GetRequestId(), "the token de-duplicates a retried start")
	require.Len(t, e.history.updates, 1)
	require.Equal(t, iface.UpdateActivateMigration, e.history.updates[0].GetInput().GetName())

	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status)
	require.Zero(t, e.scheduled(t, "forwardTaskAdds"), "nothing queued after the activation's read")
	require.ErrorIs(t, e.recordTaskAdd(), ErrClosed, "the router falls back to the workflow")
	_, err = e.describe()
	require.ErrorIs(t, err, ErrClosed)
}

func migrationTokenRequest(t *testing.T, u *updatepb.Request) *iface.MigrationTokenRequest {
	t.Helper()
	var req iface.MigrationTokenRequest
	require.NoError(t, sdk.PreferProtoDataConverter.FromPayloads(u.GetInput().GetArgs(), &req))
	return &req
}

// scheduled counts the physical side-effect tasks of the named type generated so far.
func (e *e2e) scheduled(t *testing.T, name string) int {
	t.Helper()
	byCategory, err := e.engine.Tasks(refFor(testNamespaceID, testInstanceID))
	require.NoError(t, err)
	n := 0
	for _, list := range byCategory {
		for _, task := range list {
			if ct, ok := task.(*historytasks.ChasmTask); ok {
				if fqn, ok := e.registry.TaskFqnByID(ct.Info.GetTypeId()); ok && strings.HasSuffix(fqn, "."+name) {
					n++
				}
			}
		}
	}
	return n
}

// destinations lists the outbound destinations of the named task type generated so far, in order.
func (e *e2e) destinations(t *testing.T, name string) []string {
	t.Helper()
	byCategory, err := e.engine.Tasks(refFor(testNamespaceID, testInstanceID))
	require.NoError(t, err)
	var out []string
	for _, list := range byCategory {
		for _, task := range list {
			if ct, ok := task.(*historytasks.ChasmTask); ok {
				if fqn, ok := e.registry.TaskFqnByID(ct.Info.GetTypeId()); ok && strings.HasSuffix(fqn, "."+name) {
					out = append(out, ct.Destination)
				}
			}
		}
	}
	return out
}

func (e *e2e) runForwardTask(t *testing.T) bool {
	t.Helper()
	w, _ := e.live(t)
	dropped, err := chasmtest.ExecuteSideEffectTask(e.ctx, e.engine, w, chasm.SideEffectTaskHandler[*WorkerControllerInstance, *wcipb.ForwardTaskAddsTask](e.lib.ForwardTaskAddsTaskHandler), chasm.TaskAttributes{}, &wcipb.ForwardTaskAddsTask{})
	require.NoError(t, err)
	return dropped
}

// Task-adds reaching CHASM while it migrates are queued, sent with the activation, and the tail after it forwarded.
func TestE2E_MigrateToWorkflowQueuesTaskAdds(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.NoError(t, e.migrateToWorkflow())
	frozen, _ := e.live(t)
	token := frozen.MigrationToken
	require.NoError(t, e.recordTaskAdd(), "queued, not rejected")
	require.NoError(t, e.recordTaskAdd())
	e.history.onUpdate = func() {
		e.history.onUpdate = nil
		require.NoError(t, e.recordTaskAdd(), "arrives while the activation is in flight")
	}

	e.runMigrateTask(t)
	require.Zero(t, fake.taskAddCalls, "the workflow applies them, not CHASM")
	require.Len(t, e.history.updates, 1)
	activate := e.history.updates[0]
	require.Equal(t, iface.UpdateActivateMigration+":"+token+":2", activate.GetMeta().GetUpdateId())
	req := migrationTokenRequest(t, activate)
	require.Len(t, req.TaskAdds, 2)
	require.EqualValues(t, 1, req.FirstTaskAddSeq)

	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_MIGRATED, w.Status)
	require.ErrorIs(t, e.recordTaskAdd(), ErrClosed, "once closed, task-adds fall through to the workflow")
	require.Equal(t, 1, e.scheduled(t, "forwardTaskAdds"), "the tail queued during activation is forwarded")

	require.False(t, e.runForwardTask(t))
	require.Len(t, e.history.updates, 2)
	forward := migrationTokenRequest(t, e.history.updates[1])
	require.Len(t, forward.TaskAdds, 3, "the whole queue; the workflow skips what it has")
	require.EqualValues(t, 1, forward.FirstTaskAddSeq)
	require.Equal(t, iface.UpdateActivateMigration+":"+token+":3", e.history.updates[1].GetMeta().GetUpdateId())
}

func TestE2E_MigrateToWorkflowSendsQueueWithActivation(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.NoError(t, e.migrateToWorkflow())
	require.NoError(t, e.recordTaskAdd())

	e.runMigrateTask(t)
	require.Len(t, migrationTokenRequest(t, e.history.updates[0]).TaskAdds, 1)
	require.Zero(t, e.scheduled(t, "forwardTaskAdds"), "the activation carried the whole queue")
}

func TestE2E_MigrateToWorkflowAbortReplaysQueuedTaskAdds(t *testing.T) {
	e := newE2E(t, fakeAlgo{})
	_, err := e.update(groupUpsert("test-invoke"))
	require.NoError(t, err)
	require.NoError(t, e.migrateToWorkflow())
	require.NoError(t, e.recordTaskAdd())
	require.NoError(t, e.recordTaskAdd())
	e.history.rejectActivation = &failurepb.Failure{Message: "token mismatch"}

	e.runMigrateTask(t)
	w, _ := e.live(t)
	require.Equal(t, wcipb.WCI_STATUS_RUNNING, w.Status)
	require.Equal(t, 2, fake.taskAddCalls, "CHASM processes what it queued")
	require.Empty(t, w.MigrationTaskAdds)
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
			require.Equal(t, e.clock.Now(), w.NextPollTime.AsTime(), "polls at once to catch task-adds rejected while migrating")
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
