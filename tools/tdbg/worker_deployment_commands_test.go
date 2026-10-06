package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type historyWorkflowClient struct {
	workflowservice.WorkflowServiceClient
	events []*historypb.HistoryEvent
	err    error

	requests []*workflowservice.GetWorkflowExecutionHistoryRequest
}

func (c *historyWorkflowClient) GetWorkflowExecutionHistory(
	_ context.Context,
	req *workflowservice.GetWorkflowExecutionHistoryRequest,
	_ ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &historypb.History{Events: c.events},
	}, nil
}

func runWorkerDeploymentDescribe(t *testing.T, wf workflowservice.WorkflowServiceClient, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	factory := migrateClientFactory{admin: &migrateAdminClient{}, workflow: wf}
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = factory
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	err := app.Run(append([]string{"tdbg"}, args...))
	return stdout.String(), err
}

func startedEvent(t *testing.T, input *commonpb.Payloads) *historypb.HistoryEvent {
	t.Helper()
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

func TestWorkerDeploymentDescribe_PrintsStateAndVersionCount(t *testing.T) {
	args := &deploymentspb.WorkerDeploymentWorkflowArgs{
		NamespaceName:  "my-ns",
		DeploymentName: "my-deployment",
		State: &deploymentspb.WorkerDeploymentLocalState{
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
				"build-a": {Version: "my-deployment.build-a", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED},
				"build-b": {Version: "my-deployment.build-b", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED},
				"build-c": {Version: "my-deployment.build-c", Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
			},
		},
	}
	wf := &historyWorkflowClient{events: []*historypb.HistoryEvent{startedEvent(t, encodeDeploymentArgs(t, args))}}

	stdout, err := runWorkerDeploymentDescribe(t, wf, "-n", "my-ns", "wd", "describe", "--name", "my-deployment", "--run-id", "run-1")
	require.NoError(t, err)

	require.Len(t, wf.requests, 1)
	req := wf.requests[0]
	require.Equal(t, "my-ns", req.GetNamespace())
	require.Equal(t, workerdeployment.GenerateDeploymentWorkflowID("my-deployment"), req.GetExecution().GetWorkflowId())
	require.Equal(t, "run-1", req.GetExecution().GetRunId())

	jsonOut, countOut, found := strings.Cut(stdout, "Version count: ")
	require.True(t, found)
	var printed deploymentspb.WorkerDeploymentWorkflowArgs
	require.NoError(t, protojson.Unmarshal([]byte(jsonOut), &printed))
	require.True(t, proto.Equal(args, &printed))
	require.Equal(t, "3 (Current: 1, Drained: 2)\n", countOut)
}

func TestWorkerDeploymentDescribe_NoVersions(t *testing.T) {
	args := &deploymentspb.WorkerDeploymentWorkflowArgs{DeploymentName: "my-deployment"}
	wf := &historyWorkflowClient{events: []*historypb.HistoryEvent{startedEvent(t, encodeDeploymentArgs(t, args))}}

	stdout, err := runWorkerDeploymentDescribe(t, wf, "wd", "describe", "--name", "my-deployment")
	require.NoError(t, err)
	require.Empty(t, wf.requests[0].GetExecution().GetRunId())
	require.Contains(t, stdout, "Version count: 0\n")
}

func TestWorkerDeploymentDescribe_Errors(t *testing.T) {
	tests := []struct {
		name      string
		wf        *historyWorkflowClient
		args      []string
		errSubstr string
	}{
		{
			name:      "missing name",
			wf:        &historyWorkflowClient{},
			args:      []string{"wd", "describe"},
			errSubstr: "name",
		},
		{
			name:      "history error",
			wf:        &historyWorkflowClient{err: errors.New("boom")},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "boom",
		},
		{
			name:      "empty history",
			wf:        &historyWorkflowClient{},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "no history events",
		},
		{
			name: "first event not started",
			wf: &historyWorkflowClient{events: []*historypb.HistoryEvent{{
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
			wf:        &historyWorkflowClient{events: []*historypb.HistoryEvent{startedEvent(t, nil)}},
			args:      []string{"wd", "describe", "--name", "d"},
			errSubstr: "has no input",
		},
		{
			name: "undecodable input",
			wf: &historyWorkflowClient{events: []*historypb.HistoryEvent{startedEvent(t, &commonpb.Payloads{
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
			_, err := runWorkerDeploymentDescribe(t, tc.wf, tc.args...)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errSubstr)
		})
	}
}
