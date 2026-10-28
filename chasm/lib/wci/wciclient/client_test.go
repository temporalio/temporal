package wciclient

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/wci"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type fakeService struct {
	wcipb.WorkerControllerInstanceServiceClient

	err       error
	describe  *wcipb.DescribeWorkerControllerInstanceResponse
	update    *wcipb.UpdateWorkerControllerInstanceRequest
	taskAdd   *wcipb.RecordTaskAddRequest
	migration *wcipb.CreateFromMigrationStateRequest
	activate  *wcipb.ActivateMigrationRequest
	abort     *wcipb.AbortMigrationRequest
}

func (f *fakeService) ActivateMigration(_ context.Context, req *wcipb.ActivateMigrationRequest, _ ...grpc.CallOption) (*wcipb.ActivateMigrationResponse, error) {
	f.activate = req
	return &wcipb.ActivateMigrationResponse{}, f.err
}

func (f *fakeService) AbortMigration(_ context.Context, req *wcipb.AbortMigrationRequest, _ ...grpc.CallOption) (*wcipb.AbortMigrationResponse, error) {
	f.abort = req
	return &wcipb.AbortMigrationResponse{}, f.err
}

func (f *fakeService) DescribeWorkerControllerInstance(context.Context, *wcipb.DescribeWorkerControllerInstanceRequest, ...grpc.CallOption) (*wcipb.DescribeWorkerControllerInstanceResponse, error) {
	return f.describe, f.err
}

func (f *fakeService) UpdateWorkerControllerInstance(_ context.Context, req *wcipb.UpdateWorkerControllerInstanceRequest, _ ...grpc.CallOption) (*wcipb.UpdateWorkerControllerInstanceResponse, error) {
	f.update = req
	return &wcipb.UpdateWorkerControllerInstanceResponse{ScalingGroupSpecs: map[string]*wcipb.ScalingGroupSpec{"g": {Compute: &wcipb.ComputeProviderSpec{ProviderType: "test-invoke"}}}}, f.err
}

func (f *fakeService) RecordTaskAdd(_ context.Context, req *wcipb.RecordTaskAddRequest, _ ...grpc.CallOption) (*wcipb.RecordTaskAddResponse, error) {
	f.taskAdd = req
	return &wcipb.RecordTaskAddResponse{}, f.err
}

func (f *fakeService) CreateFromMigrationState(_ context.Context, req *wcipb.CreateFromMigrationStateRequest, _ ...grpc.CallOption) (*wcipb.CreateFromMigrationStateResponse, error) {
	f.migration = req
	return &wcipb.CreateFromMigrationStateResponse{}, f.err
}

var (
	testVersion = &deploymentpb.WorkerDeploymentVersion{DeploymentName: "dep", BuildId: "build"}
	testNS      = namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns"}, nil, "active")
)

// Errors arrive as fresh serviceerrors after gRPC, so mapping must not rely on identity.
func overTheWire(err error) error {
	return serviceerror.FromStatus(serviceerror.ToStatus(err))
}

func TestErrorMapping(t *testing.T) {
	require.ErrorIs(t, mapError(overTheWire(wci.ErrClosed)), wciclient.ErrInstanceClosed)
	require.ErrorIs(t, mapError(overTheWire(wci.ErrProviderNotSupportedOnCHASM)), wciclient.ErrProviderNotSupportedOnCHASM)

	var notFound *serviceerror.NotFound
	require.ErrorAs(t, mapError(overTheWire(serviceerror.NewNotFound("missing"))), &notFound)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, mapError(overTheWire(wci.ErrMigrationPending)), &unavailable, "retryable, not a fall-through")
	require.ErrorIs(t, mapError(overTheWire(wci.ErrMigrationPending)), wciclient.ErrMigrationPending)
}

func TestExists(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "running", want: true},
		{name: "missing", err: serviceerror.NewNotFound("missing")},
		{name: "closed", err: wci.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeService{describe: &wcipb.DescribeWorkerControllerInstanceResponse{}}
			if tc.err != nil {
				svc.err = overTheWire(tc.err)
			}
			got, err := New(Params{Service: svc}).WorkerControllerInstanceExists(t.Context(), testNS, testVersion)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDescribeConverts(t *testing.T) {
	svc := &fakeService{describe: &wcipb.DescribeWorkerControllerInstanceResponse{
		DeploymentName:    "dep",
		BuildId:           "build",
		ConflictToken:     []byte("token"),
		ScalingGroupSpecs: map[string]*wcipb.ScalingGroupSpec{"g": {Compute: &wcipb.ComputeProviderSpec{ProviderType: "test-invoke"}}},
		ValidationStatus:  &wcipb.ValidationStatus{Result: wcipb.VALIDATION_RESULT_SUCCESS},
	}}
	resp, token, err := New(Params{Service: svc}).DescribeWorkerControllerInstance(t.Context(), testNS, testVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("token"), token)
	require.Equal(t, iface.ComputeProviderType("test-invoke"), resp.Spec.ScalingGroupSpecs["g"].Compute.ProviderType)
	require.Equal(t, iface.ValidationResultSuccess, resp.ValidationStatus.Status)
}

func TestUpdateAndSignalAddressTheWorkflowID(t *testing.T) {
	svc := &fakeService{}
	c := New(Params{Service: svc})

	resp, err := c.UpdateWorkerControllerInstance(t.Context(), testNS, testVersion, []byte("tok"), "me",
		map[string]iface.ScalingGroupSpecUpdate{"g": {Spec: iface.ScalingGroupSpec{Compute: iface.ComputeProviderSpec{ProviderType: "test-invoke"}}}}, nil)
	require.NoError(t, err)
	require.Contains(t, resp.Spec.ScalingGroupSpecs, "g")
	require.Equal(t, wciclient.GenerateWorkerControllerInstanceWorkflowID(testVersion), svc.update.GetInstanceId())
	require.Equal(t, "ns-id", svc.update.GetNamespaceId())
	require.Equal(t, "build", svc.update.GetBuildId())

	require.NoError(t, c.SignalTaskAddEvent(t.Context(), testNS, testVersion, &iface.SignalTaskAddRequest{TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY, NoSyncMatchSignalsSinceLast: 3}))
	require.Equal(t, svc.update.GetInstanceId(), svc.taskAdd.GetInstanceId())
	require.EqualValues(t, 3, svc.taskAdd.GetEvent().GetNoSyncMatchSignalsSinceLast())
}

func TestCreateFromWorkflowState(t *testing.T) {
	svc := &fakeService{}
	expire := time.Unix(1_000, 0).UTC()
	err := New(Params{Service: svc}).CreateFromWorkflowState(t.Context(), &iface.WorkerControllerInstanceWorkflowArgs{
		NamespaceId: "ns-id", DeploymentName: "dep", BuildId: "build",
		State: &iface.WorkerControllerInstanceLocalState{PendingTaskAddSignals: []*iface.SignalTaskAddRequest{{}}},
	}, "run-1", expire)
	require.NoError(t, err)
	require.Equal(t, wciclient.GenerateWorkerControllerInstanceWorkflowID(testVersion), svc.migration.GetInstanceId())
	require.Equal(t, "run-1", svc.migration.GetMigrationToken())
	require.Equal(t, expire, svc.migration.GetExpireTime().AsTime())

	svc.err = overTheWire(wci.ErrProviderNotSupportedOnCHASM)
	err = New(Params{Service: svc}).CreateFromWorkflowState(t.Context(), &iface.WorkerControllerInstanceWorkflowArgs{DeploymentName: "dep", BuildId: "build"}, "run-1", expire)
	require.ErrorIs(t, err, wciclient.ErrProviderNotSupportedOnCHASM, "the workflow stays a workflow")
}

func TestActivateAndAbortMigration(t *testing.T) {
	args := &iface.WorkerControllerInstanceWorkflowArgs{NamespaceId: "ns-id", DeploymentName: "dep", BuildId: "build"}
	svc := &fakeService{}
	c := New(Params{Service: svc})

	require.NoError(t, c.ActivateMigration(t.Context(), args, "run-1", []*iface.SignalTaskAddRequest{{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}, nil}))
	require.Equal(t, "run-1", svc.activate.GetMigrationToken())
	require.Equal(t, wciclient.GenerateWorkerControllerInstanceWorkflowID(testVersion), svc.activate.GetInstanceId())
	require.Len(t, svc.activate.GetPendingTaskAdds(), 1, "nil task-adds are dropped")

	require.NoError(t, c.AbortMigration(t.Context(), args, "run-1"))
	require.Equal(t, "run-1", svc.abort.GetMigrationToken())

	// Definitive: the target is gone, so the workflow keeps the WCI.
	for _, gone := range []error{wci.ErrMigrationNotPending, serviceerror.NewNotFound("missing")} {
		svc.err = overTheWire(gone)
		require.ErrorIs(t, c.ActivateMigration(t.Context(), args, "run-1", nil), wciclient.ErrMigrationAborted)
	}
	// Ambiguous: anything else.
	svc.err = overTheWire(serviceerror.NewUnavailable("shard moved"))
	err := c.ActivateMigration(t.Context(), args, "run-1", nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, wciclient.ErrMigrationAborted)
}

func TestListNeedsVisibility(t *testing.T) {
	_, _, err := New(Params{Service: &fakeService{}}).ListWorkerControllerInstances(t.Context(), testNS, 10, nil)
	var unimplemented *serviceerror.Unimplemented
	require.ErrorAs(t, err, &unimplemented)
}

// List filters on the server: only running instances come back, so pending targets and closed runs are not listed.
func TestListQueriesRunning(t *testing.T) {
	memo, err := payload.Encode(&wcipb.WorkerControllerInstanceMemo{DeploymentName: "dep", BuildId: "b1"})
	require.NoError(t, err)
	visibility := chasm.NewMockVisibilityManager(gomock.NewController(t))
	visibility.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ reflect.Type, req *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Equal(t, wci.RunningListQuery, req.Query)
			return &visibilityservice.ListChasmExecutionsResponse{
				Executions:    []*chasmspb.VisibilityExecutionInfo{{BusinessId: "id", ChasmMemo: memo}},
				NextPageToken: []byte("next"),
			}, nil
		})

	items, next, err := New(Params{Service: &fakeService{}, Visibility: visibility}).ListWorkerControllerInstances(t.Context(), testNS, 10, nil)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "dep", items[0].DeploymentName)
	require.Equal(t, []byte("next"), next)
}
