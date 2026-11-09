//go:build temporal_minimal && !temporal_with_worker_controller

package workerdeployment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	computepb "go.temporal.io/api/compute/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	wciiface "go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/sdk/temporal"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

type unavailableWorkerControllerClient struct {
	wciclient.Client
	exists    bool
	existsErr error
}

func (unavailableWorkerControllerClient) ValidateWorkerControllerInstanceSpec(context.Context, *namespace.Namespace, *deploymentpb.WorkerDeploymentVersion, string, map[string]wciiface.ScalingGroupSpecUpdate, []string) error {
	return serviceerror.NewFailedPrecondition("worker controller disabled")
}

func (c unavailableWorkerControllerClient) WorkerControllerInstanceExists(context.Context, *namespace.Namespace, *deploymentpb.WorkerDeploymentVersion) (bool, error) {
	return c.exists, c.existsErr
}

func (unavailableWorkerControllerClient) DescribeWorkerControllerInstance(context.Context, *namespace.Namespace, *deploymentpb.WorkerDeploymentVersion) (*wciiface.QueryDescribeWorkerControllerInstanceResponse, []byte, error) {
	return nil, nil, serviceerror.NewFailedPrecondition("worker controller has no worker")
}

func TestWorkerControllerExcludedComputeAdmission(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"create", "update", "validate"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			history := historyservicemock.NewMockHistoryServiceClient(ctrl)
			history.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("missing workflow")).AnyTimes()
			d := &ClientImpl{logger: log.NewNoopLogger(), historyClient: history, maxIDLengthLimit: func() int { return 1000 }, workerControllerInstanceClient: unavailableWorkerControllerClient{}}
			ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace"}, nil, "")
			version := &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}
			var err error
			switch operation {
			case "create":
				err = d.CreateWorkerDeploymentVersion(t.Context(), ns, "deployment", "build", "identity", "request", &computepb.ComputeConfig{})
			case "update":
				err = d.UpdateVersionComputeConfig(t.Context(), ns, version, nil, nil, "identity", "request")
			case "validate":
				err = d.ValidateComputeConfig(t.Context(), ns, version, nil, nil, "identity")
			default:
				t.Fatalf("unknown operation %q", operation)
			}
			require.ErrorContains(t, err, "temporal_with_worker_controller")
			var failedPrecondition *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecondition)
		})
	}
}

func TestWorkerControllerExcludedDescribeVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                    string
		hasCompute, hasInstance bool
		existsErr               error
	}{
		{name: "ordinary version"},
		{name: "persisted compute config", hasCompute: true},
		{name: "persisted controller instance", hasInstance: true},
		{name: "instance lookup failure", existsErr: serviceerror.NewFailedPrecondition("instance lookup unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			history := historyservicemock.NewMockHistoryServiceClient(ctrl)
			state := &deploymentspb.VersionLocalState{Version: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED}
			if tc.hasCompute {
				state.ComputeConfig = &computepb.ComputeConfigSummary{}
			}
			payloads, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.QueryDescribeVersionResponse{VersionState: state})
			require.NoError(t, err)
			history.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&historyservice.QueryWorkflowResponse{Response: &workflowservice.QueryWorkflowResponse{QueryResult: payloads}}, nil)
			d := &ClientImpl{logger: log.NewNoopLogger(), historyClient: history, maxIDLengthLimit: func() int { return 1000 }, workerControllerInstanceClient: unavailableWorkerControllerClient{exists: tc.hasInstance, existsErr: tc.existsErr}}
			ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace"}, nil, "")
			info, queues, err := d.DescribeVersion(t.Context(), ns, "deployment.build", false)
			if tc.existsErr != nil {
				require.ErrorIs(t, err, tc.existsErr)
				return
			}
			if tc.hasCompute || tc.hasInstance {
				require.ErrorContains(t, err, "temporal_with_worker_controller")
				var failedPrecondition *serviceerror.FailedPrecondition
				require.ErrorAs(t, err, &failedPrecondition)
				return
			}
			require.NoError(t, err)
			//nolint:staticcheck // SA1019
			protorequire.ProtoEqual(t, &deploymentpb.WorkerDeploymentVersionInfo{Version: "deployment.build", DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}, Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED}, info)
			require.Empty(t, queues)
		})
	}
}

func TestWorkerControllerExcludedCreateOrdinaryVersion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)
	history.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&historyservice.UpdateWorkflowExecutionResponse{Response: &workflowservice.UpdateWorkflowExecutionResponse{Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, Outcome: &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: &commonpb.Payloads{}}}}}, nil)
	d := &ClientImpl{logger: log.NewNoopLogger(), historyClient: history, maxIDLengthLimit: func() int { return 1000 }}
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace"}, nil, "")
	require.NoError(t, d.CreateWorkerDeploymentVersion(t.Context(), ns, "deployment", "build", "identity", "request", nil))
}

func (unavailableWorkerControllerClient) UpdateWorkerControllerInstance(context.Context, *namespace.Namespace, *deploymentpb.WorkerDeploymentVersion, []byte, string, map[string]wciiface.ScalingGroupSpecUpdate, []string) (*wciiface.UpdateWorkerControllerInstanceResponse, error) {
	return nil, serviceerror.NewFailedPrecondition("worker controller has no worker")
}

func (unavailableWorkerControllerClient) DeleteWorkerControllerInstance(context.Context, *namespace.Namespace, *deploymentpb.WorkerDeploymentVersion, string) error {
	return serviceerror.NewFailedPrecondition("worker controller has no worker")
}

func TestWorkerControllerExcludedActivities(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"update deployment", "update version", "describe status", "delete deployment instance", "delete version instance"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			deps := activityDeps{WorkerControllerInstanceClient: unavailableWorkerControllerClient{exists: true}}
			a := &Activities{activityDeps: deps}
			va := &VersionActivities{activityDeps: deps}
			input := &deploymentspb.UpdateWorkerControllerInstanceInput{Version: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}}
			var err error
			switch operation {
			case "update deployment":
				_, err = a.UpdateWorkerControllerInstanceFromDeployment(t.Context(), input)
			case "update version":
				_, err = va.UpdateWorkerControllerInstance(t.Context(), input)
			case "describe status":
				_, err = va.DescribeWorkerControllerInstanceStatus(t.Context(), &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"})
			case "delete deployment instance":
				err = a.DeleteWorkerControllerInstanceFromDeployment(t.Context(), &deploymentspb.DeleteWorkerControllerInstanceInput{Version: input.Version})
			case "delete version instance":
				err = va.DeleteWorkerControllerInstance(t.Context(), &deploymentspb.DeleteWorkerControllerInstanceInput{Version: input.Version})
			default:
				t.Fatalf("unknown operation %q", operation)
			}
			require.ErrorContains(t, err, "temporal_with_worker_controller")
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			require.True(t, appErr.NonRetryable())
			require.Equal(t, errFailedPrecondition, appErr.Type())
		})
	}
}

func TestWorkerControllerExcludedDeleteOrdinaryVersion(t *testing.T) {
	t.Parallel()

	deps := activityDeps{WorkerControllerInstanceClient: unavailableWorkerControllerClient{}}
	a := &Activities{activityDeps: deps}
	va := &VersionActivities{activityDeps: deps}
	input := &deploymentspb.DeleteWorkerControllerInstanceInput{Version: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}}
	require.NoError(t, a.DeleteWorkerControllerInstanceFromDeployment(t.Context(), input))
	require.NoError(t, va.DeleteWorkerControllerInstance(t.Context(), input))
}

func TestWorkerControllerExcludedDeleteLookupFailure(t *testing.T) {
	t.Parallel()

	lookupErr := serviceerror.NewUnavailable("instance lookup unavailable")
	deps := activityDeps{WorkerControllerInstanceClient: unavailableWorkerControllerClient{existsErr: lookupErr}}
	a := &Activities{activityDeps: deps}
	va := &VersionActivities{activityDeps: deps}
	input := &deploymentspb.DeleteWorkerControllerInstanceInput{Version: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}}
	require.ErrorIs(t, a.DeleteWorkerControllerInstanceFromDeployment(t.Context(), input), lookupErr)
	require.ErrorIs(t, va.DeleteWorkerControllerInstance(t.Context(), input), lookupErr)
}
