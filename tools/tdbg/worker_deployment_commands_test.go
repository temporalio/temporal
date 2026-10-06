package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testDeploymentNamespaceID = "ns-id"
	testDeploymentRunID       = "6f1c1f5e-1f6e-4a7b-9d55-0b3c2a1d4e5f"
)

type deploymentAdminClient struct {
	adminservice.AdminServiceClient
	status     enumspb.WorkflowExecutionStatus
	startTime  time.Time
	events     []*historypb.HistoryEvent
	noBatches  bool
	badBatch   bool
	msErr      error
	historyErr error

	msRequests      []*adminservice.DescribeMutableStateRequest
	historyRequests []*adminservice.GetWorkflowExecutionRawHistoryV2Request
}

func (c *deploymentAdminClient) DescribeMutableState(
	_ context.Context,
	req *adminservice.DescribeMutableStateRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	c.msRequests = append(c.msRequests, req)
	if c.msErr != nil {
		return nil, c.msErr
	}
	runID := req.GetExecution().GetRunId()
	if runID == "" {
		runID = testDeploymentRunID
	}
	return &adminservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:     runID,
				Status:    c.status,
				StartTime: timestamppb.New(c.startTime),
			},
		},
	}, nil
}

func (c *deploymentAdminClient) GetWorkflowExecutionRawHistoryV2(
	_ context.Context,
	req *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	_ ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	c.historyRequests = append(c.historyRequests, req)
	if c.historyErr != nil {
		return nil, c.historyErr
	}
	if c.noBatches {
		return &adminservice.GetWorkflowExecutionRawHistoryV2Response{}, nil
	}
	if c.badBatch {
		return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: []*commonpb.DataBlob{{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte{0xff, 0xff}}},
		}, nil
	}
	blob, err := serialization.NewSerializer().SerializeEvents(c.events)
	if err != nil {
		return nil, err
	}
	return &adminservice.GetWorkflowExecutionRawHistoryV2Response{HistoryBatches: []*commonpb.DataBlob{blob}}, nil
}

type deploymentWorkflowClient struct {
	workflowservice.WorkflowServiceClient
}

func (deploymentWorkflowClient) DescribeNamespace(
	context.Context,
	*workflowservice.DescribeNamespaceRequest,
	...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespacepb.NamespaceInfo{Id: testDeploymentNamespaceID},
	}, nil
}

type deploymentClientFactory struct {
	admin *deploymentAdminClient
}

func (f deploymentClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.admin
}

func (f deploymentClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return deploymentWorkflowClient{}
}

func runWorkerDeploymentDescribe(t *testing.T, admin *deploymentAdminClient, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = deploymentClientFactory{admin: admin}
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	err := app.Run(append([]string{"tdbg"}, args...))
	return stdout.String(), err
}

func startedEvent(input *commonpb.Payloads) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				Input: input,
			},
		},
	}
}

func encodeDeploymentArgs(t *testing.T, args *deploymentspb.WorkerDeploymentWorkflowArgs) *commonpb.Payloads {
	t.Helper()
	payload, err := sdk.PreferProtoDataConverter.ToPayload(args)
	require.NoError(t, err)
	return &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
}

func versionSummary(version string, status enumspb.WorkerDeploymentVersionStatus) *deploymentspb.WorkerDeploymentVersionSummary {
	return &deploymentspb.WorkerDeploymentVersionSummary{Version: version, Status: status}
}

func TestWorkerDeploymentDescribe_PrintsStateAndVersionCount(t *testing.T) {
	args := &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  "my-ns",
		DeploymentName: "my-deployment",
		State: &deploymentspb.WorkerDeploymentLocalState{
			// Keyed by the full version string, as the deployment workflow does.
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				"my-deployment.build-a": versionSummary("my-deployment.build-a", enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
				"my-deployment.build-b": versionSummary("my-deployment.build-b", enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
				"my-deployment.build-c": versionSummary("my-deployment.build-c", enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
			},
		},
	}
	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	admin := &deploymentAdminClient{
		status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		startTime: startTime,
		events:    []*historypb.HistoryEvent{startedEvent(encodeDeploymentArgs(t, args))},
	}

	stdout, err := runWorkerDeploymentDescribe(t, admin, "-n", "my-ns", "wd", "describe", "--name", "my-deployment", "--run-id", testDeploymentRunID)
	require.NoError(t, err)

	workflowID := workerdeployment.GenerateDeploymentWorkflowID("my-deployment")
	require.Len(t, admin.msRequests, 1)
	require.Equal(t, "my-ns", admin.msRequests[0].GetNamespace())
	require.Equal(t, workflowID, admin.msRequests[0].GetExecution().GetWorkflowId())
	require.Equal(t, testDeploymentRunID, admin.msRequests[0].GetExecution().GetRunId())
	require.Len(t, admin.historyRequests, 1)
	require.Equal(t, testDeploymentNamespaceID, admin.historyRequests[0].GetNamespaceId())
	require.Equal(t, workflowID, admin.historyRequests[0].GetExecution().GetWorkflowId())
	require.Equal(t, testDeploymentRunID, admin.historyRequests[0].GetExecution().GetRunId())

	header, rest, found := strings.Cut(stdout, "(changes made during the run are not included):\n")
	require.True(t, found)
	require.Contains(t, header, "Run ID:     "+testDeploymentRunID)
	require.Contains(t, header, "Start time: "+startTime.String())
	require.Contains(t, header, "Status:     Running")

	jsonOut, countOut, found := strings.Cut(rest, "Version count: ")
	require.True(t, found)
	var printed deploymentspb.WorkerDeploymentWorkflowArgs
	require.NoError(t, protojson.Unmarshal([]byte(jsonOut), &printed))
	protorequire.ProtoEqual(t, args, &printed)
	require.Equal(t, "3 (Current: 1, Drained: 2)\n", countOut)
}

func TestWorkerDeploymentDescribe_CurrentRunAndNoVersions(t *testing.T) {
	args := &deploymentspb.WorkerDeploymentWorkflowArgs{DeploymentName: "my-deployment"}
	admin := &deploymentAdminClient{
		status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		events: []*historypb.HistoryEvent{startedEvent(encodeDeploymentArgs(t, args))},
	}

	stdout, err := runWorkerDeploymentDescribe(t, admin, "wd", "describe", "--name", "my-deployment")
	require.NoError(t, err)

	require.Len(t, admin.msRequests, 1)
	require.Empty(t, admin.msRequests[0].GetExecution().GetRunId())
	// The run resolved by DescribeMutableState is the one whose history is read.
	require.Len(t, admin.historyRequests, 1)
	require.Equal(t, testDeploymentRunID, admin.historyRequests[0].GetExecution().GetRunId())
	require.Contains(t, stdout, "Run ID:     "+testDeploymentRunID)
	require.Contains(t, stdout, "Status:     Completed")
	require.True(t, strings.HasSuffix(stdout, "Version count: 0\n"))
}

func TestWorkerDeploymentDescribe_Errors(t *testing.T) {
	tests := []struct {
		name      string
		admin     *deploymentAdminClient
		args      []string
		errSubstr string
	}{
		{
			name:      "missing name",
			admin:     &deploymentAdminClient{},
			args:      []string{"wd", "describe"},
			errSubstr: `Required flag "name" not set`,
		},
		{
			name:      "describe mutable state error",
			admin:     &deploymentAdminClient{msErr: errors.New("ms boom")},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "unable to describe worker deployment workflow",
		},
		{
			name:      "history error",
			admin:     &deploymentAdminClient{historyErr: errors.New("history boom")},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "history boom",
		},
		{
			name:      "no history batches",
			admin:     &deploymentAdminClient{noBatches: true},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "no history events",
		},
		{
			name:      "undeserializable history",
			admin:     &deploymentAdminClient{badBatch: true},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "unable to deserialize history",
		},
		{
			name: "first event not started",
			admin: &deploymentAdminClient{events: []*historypb.HistoryEvent{{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
					WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
				},
			}}},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "expected WorkflowExecutionStarted",
		},
		{
			name:      "no input",
			admin:     &deploymentAdminClient{events: []*historypb.HistoryEvent{startedEvent(nil)}},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "has no input",
		},
		{
			name: "undecodable input",
			admin: &deploymentAdminClient{events: []*historypb.HistoryEvent{startedEvent(&commonpb.Payloads{
				Payloads: []*commonpb.Payload{{
					Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")},
					Data:     []byte{0xff, 0xff, 0xff},
				}},
			})}},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "unable to decode input",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runWorkerDeploymentDescribe(t, tc.admin, tc.args...)
			require.ErrorContains(t, err, tc.errSubstr)
		})
	}
}
