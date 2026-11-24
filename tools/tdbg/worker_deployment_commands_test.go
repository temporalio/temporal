package tdbg

import (
	"bytes"
	"context"
	"encoding/json"
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
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
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
// DescribeMutableState resolves an empty run ID to testDeploymentRunID, like the server resolves the current run.
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
// Cases don't run in parallel because cli/v2 mutates shared package-level flags (e.g. the help flag) in
// App.Run, which races across concurrent apps.
func TestDescribeWorkerDeployment(t *testing.T) {
	deploymentArgs := &deploymentspb.WorkerDeploymentWorkflowArgs{
		DeploymentName: "my-deployment",
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				"my-deployment.v1": {Version: "my-deployment.v1", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED},
				"my-deployment.v2": {Version: "my-deployment.v2", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
			},
		},
	}
	validInput, err := sdk.PreferProtoDataConverter.ToPayload(deploymentArgs)
	require.NoError(t, err)
	undecodableInput := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")},
		Data:     []byte{0xff, 0xff, 0xff},
	}
	workflowID := worker_versioning.WorkerDeploymentWorkflowIDPrefix + worker_versioning.WorkerDeploymentVersionDelimiter + "my-deployment"

	tests := []struct {
		name                    string
		args                    []string
		input                   *commonpb.Payload
		describeMutableStateErr error
		wantErr                 string
		wantMSRunID             string
		wantRunID               string
	}{
		{
			// Enforced by cli/v2 (Required: true) before the action runs.
			name:    "missing namespace",
			args:    []string{"--name", "my-deployment"},
			input:   validInput,
			wantErr: `Required flag "namespace" not set`,
		},
		{
			// Enforced by cli/v2 (Required: true) before the action runs.
			name:    "missing name",
			args:    []string{"--namespace", "default"},
			input:   validInput,
			wantErr: `Required flag "name" not set`,
		},
		{
			name:        "current run",
			args:        []string{"--namespace", "default", "--name", "my-deployment"},
			input:       validInput,
			wantMSRunID: "",
			wantRunID:   testDeploymentRunID,
		},
		{
			name:        "specific run",
			args:        []string{"--namespace", "default", "--name", "my-deployment", "--run-id", "other-run"},
			input:       validInput,
			wantMSRunID: "other-run",
			wantRunID:   "other-run",
		},
		{
			name:                    "DescribeMutableState error",
			args:                    []string{"--namespace", "default", "--name", "my-deployment"},
			input:                   validInput,
			describeMutableStateErr: errors.New("history unavailable"),
			wantErr:                 "unable to describe Worker Deployment workflow: history unavailable",
		},
		{
			name:    "undecodable input",
			args:    []string{"--namespace", "default", "--name", "my-deployment"},
			input:   undecodableInput,
			wantErr: "unable to decode Worker Deployment workflow input",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newWorkerDeploymentTestClient(t, tc.input)
			var msRequest *adminservice.DescribeMutableStateRequest
			var historyRequest *adminservice.GetWorkflowExecutionRawHistoryV2Request
			describeMutableState, getRawHistory := client.describeMutableStateFn, client.getRawHistoryFn
			client.describeMutableStateFn = func(request *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error) {
				msRequest = request
				if tc.describeMutableStateErr != nil {
					return nil, tc.describeMutableStateErr
				}
				return describeMutableState(request)
			}
			client.getRawHistoryFn = func(request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
				historyRequest = request
				return getRawHistory(request)
			}

			stdout, err := runWorkerDeploymentCommand(client, tc.args...)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			require.Equal(t, workflowID, msRequest.GetExecution().GetWorkflowId())
			require.Equal(t, tc.wantMSRunID, msRequest.GetExecution().GetRunId())
			require.Equal(t, testDeploymentNamespaceID, historyRequest.GetNamespaceId())
			require.Equal(t, tc.wantRunID, historyRequest.GetExecution().GetRunId())

			var description workerDeploymentDescription
			require.NoError(t, json.Unmarshal([]byte(stdout), &description))
			require.Equal(t, tc.wantRunID, description.RunID)
			require.Equal(t, "Running", description.Status)
			require.Equal(t, 2, description.VersionCount)
			var state deploymentspb.WorkerDeploymentWorkflowArgs
			require.NoError(t, protojson.Unmarshal(description.State, &state))
			protorequire.ProtoEqual(t, deploymentArgs, &state)
		})
	}
}
