package tdbg

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/grpc"
)

const (
	testDeploymentNamespaceID = "ns-id"
	testDeploymentRunID       = "run-id"
)

type workerDeploymentTestClient struct {
	adminservice.AdminServiceClient
	describeMutableStateFn func(request *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error)
	getRawHistoryFn        func(request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error)
}

func (t *workerDeploymentTestClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return t
}

func (t *workerDeploymentTestClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("unimplemented")
}

func (t *workerDeploymentTestClient) DescribeMutableState(_ context.Context, request *adminservice.DescribeMutableStateRequest, _ ...grpc.CallOption) (*adminservice.DescribeMutableStateResponse, error) {
	return t.describeMutableStateFn(request)
}

func (t *workerDeploymentTestClient) GetWorkflowExecutionRawHistoryV2(_ context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request, _ ...grpc.CallOption) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	return t.getRawHistoryFn(request)
}

// newWorkerDeploymentTestClient returns a client whose deployment workflow was started with the given input.
func newWorkerDeploymentTestClient(t *testing.T, input *commonpb.Payload) *workerDeploymentTestClient {
	blob, err := serialization.NewSerializer().SerializeEvents([]*historypb.HistoryEvent{{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{input}},
			},
		},
	}})
	require.NoError(t, err)
	return &workerDeploymentTestClient{
		describeMutableStateFn: func(request *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error) {
			runID := request.GetExecution().GetRunId()
			if runID == "" {
				runID = testDeploymentRunID
			}
			return &adminservice.DescribeMutableStateResponse{
				DatabaseMutableState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{NamespaceId: testDeploymentNamespaceID},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:  runID,
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					},
				},
			}, nil
		},
		getRawHistoryFn: func(request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
			return &adminservice.GetWorkflowExecutionRawHistoryV2Response{HistoryBatches: []*commonpb.DataBlob{blob}}, nil
		},
	}
}

func runWorkerDeploymentCommand(client ClientFactory, args ...string) (string, error) {
	var stdout bytes.Buffer
	app := NewCliApp(func(params *Params) {
		params.ClientFactory = client
		params.Writer = &stdout
	})
	app.ExitErrHandler = func(context *cli.Context, err error) {}
	err := app.Run(append([]string{"tdbg", "worker-deployment", "describe"}, args...))
	return stdout.String(), err
}

// TestDescribeWorkerDeployment tests that the cli decodes the deployment state and counts its versions.
func TestDescribeWorkerDeployment(t *testing.T) {
	input, err := sdk.PreferProtoDataConverter.ToPayload(&deploymentspb.WorkerDeploymentWorkflowArgs{
		DeploymentName: "my-deployment",
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				"my-deployment.v1": {Version: "my-deployment.v1", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED},
				"my-deployment.v2": {Version: "my-deployment.v2", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
			},
		},
	})
	require.NoError(t, err)
	workflowID := worker_versioning.WorkerDeploymentWorkflowIDPrefix + worker_versioning.WorkerDeploymentVersionDelimiter + "my-deployment"

	// Missing --namespace and --name are enforced by cli/v2 (Required: true) before the action runs.
	_, err = runWorkerDeploymentCommand(newWorkerDeploymentTestClient(t, input), "--name", "my-deployment")
	require.ErrorContains(t, err, `Required flag "namespace" not set`)
	_, err = runWorkerDeploymentCommand(newWorkerDeploymentTestClient(t, input), "--namespace", "default")
	require.ErrorContains(t, err, `Required flag "name" not set`)

	// No --run-id: the current run is resolved via DescribeMutableState and its history is read.
	client := newWorkerDeploymentTestClient(t, input)
	var msRequest *adminservice.DescribeMutableStateRequest
	var historyRequest *adminservice.GetWorkflowExecutionRawHistoryV2Request
	describeMutableState, getRawHistory := client.describeMutableStateFn, client.getRawHistoryFn
	client.describeMutableStateFn = func(request *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error) {
		msRequest = request
		return describeMutableState(request)
	}
	client.getRawHistoryFn = func(request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
		historyRequest = request
		return getRawHistory(request)
	}
	stdout, err := runWorkerDeploymentCommand(client, "--namespace", "default", "--name", "my-deployment")
	require.NoError(t, err)
	require.Equal(t, workflowID, msRequest.GetExecution().GetWorkflowId())
	require.Empty(t, msRequest.GetExecution().GetRunId())
	require.Equal(t, testDeploymentNamespaceID, historyRequest.GetNamespaceId())
	require.Equal(t, testDeploymentRunID, historyRequest.GetExecution().GetRunId())
	require.Contains(t, stdout, "Run ID: run-id (Running)")
	require.Contains(t, stdout, "my-deployment.v1")
	require.Contains(t, stdout, "WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT")
	require.Contains(t, stdout, "Version count: 2\n")

	// --run-id is passed through to DescribeMutableState.
	_, err = runWorkerDeploymentCommand(client, "--namespace", "default", "--name", "my-deployment", "--run-id", "other-run")
	require.NoError(t, err)
	require.Equal(t, "other-run", msRequest.GetExecution().GetRunId())
	require.Equal(t, "other-run", historyRequest.GetExecution().GetRunId())

	// History service returns an error: CLI wraps and returns it.
	errorClient := newWorkerDeploymentTestClient(t, input)
	errorClient.describeMutableStateFn = func(request *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error) {
		return nil, errors.New("history unavailable")
	}
	_, err = runWorkerDeploymentCommand(errorClient, "--namespace", "default", "--name", "my-deployment")
	require.ErrorContains(t, err, "unable to describe Worker Deployment workflow")

	// Input that isn't a WorkerDeploymentWorkflowArgs fails to decode.
	badInput := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")},
		Data:     []byte{0xff, 0xff, 0xff},
	}
	_, err = runWorkerDeploymentCommand(newWorkerDeploymentTestClient(t, badInput), "--namespace", "default", "--name", "my-deployment")
	require.ErrorContains(t, err, "unable to decode Worker Deployment workflow input")
}
