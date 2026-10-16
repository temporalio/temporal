package tdbg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/all"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nexusScanAdminClient struct {
	adminservice.AdminServiceClient

	mu        sync.Mutex
	requests  []*adminservice.DescribeMutableStateRequest
	responses map[string]*adminservice.DescribeMutableStateResponse
}

func (c *nexusScanAdminClient) DescribeMutableState(
	_ context.Context,
	req *adminservice.DescribeMutableStateRequest,
	_ ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	return c.responses[req.GetExecution().GetWorkflowId()], nil
}

type nexusScanWorkflowClient struct {
	workflowservice.WorkflowServiceClient

	listRequest     *workflowservice.ListWorkflowExecutionsRequest
	historyRequest  *workflowservice.GetWorkflowExecutionHistoryReverseRequest
	historyCalls    atomic.Int32
	listResponse    *workflowservice.ListWorkflowExecutionsResponse
	historyResponse *workflowservice.GetWorkflowExecutionHistoryReverseResponse
}

func (c *nexusScanWorkflowClient) ListWorkflowExecutions(
	_ context.Context,
	req *workflowservice.ListWorkflowExecutionsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	c.listRequest = req
	return c.listResponse, nil
}

func (c *nexusScanWorkflowClient) GetWorkflowExecutionHistoryReverse(
	_ context.Context,
	req *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
	_ ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	c.historyCalls.Add(1)
	c.historyRequest = req
	return c.historyResponse, nil
}

func TestAdminScanNexusChasm(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Add(-time.Hour)
	admin := &nexusScanAdminClient{responses: map[string]*adminservice.DescribeMutableStateResponse{
		"chasm-workflow": {
			DatabaseMutableState: nexusScanMutableStateWithOperation(t, now),
		},
		"hsm-workflow": {
			DatabaseMutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					NamespaceId: "namespace-id",
					WorkflowId:  "hsm-workflow",
				},
				ExecutionState: &persistencespb.WorkflowExecutionState{
					RunId:  "hsm-run",
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
		},
	}}
	workflowClient := &nexusScanWorkflowClient{
		listResponse: &workflowservice.ListWorkflowExecutionsResponse{
			Executions: []*workflowpb.WorkflowExecutionInfo{
				{
					Execution: &commonpb.WorkflowExecution{WorkflowId: "chasm-workflow", RunId: "chasm-run"},
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
				{
					Execution: &commonpb.WorkflowExecution{WorkflowId: "hsm-workflow", RunId: "hsm-run"},
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
		},
		historyResponse: &workflowservice.GetWorkflowExecutionHistoryReverseResponse{
			History: &historypb.History{Events: []*historypb.HistoryEvent{
				workflowTaskCompletedHistoryEvent(9, now.Add(3*time.Minute)),
				{
					EventId:   7,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
					EventTime: timestamppb.New(now.Add(2 * time.Minute)),
					Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
						NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
							ScheduledEventId: 5,
							OperationToken:   "completion-token-secret",
						},
					},
				},
				{
					EventId:   5,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					EventTime: timestamppb.New(now),
					Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
						NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
							Input: &commonpb.Payload{Data: []byte("payload-secret")},
						},
					},
				},
			}},
		},
	}

	var stdout, stderr bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = migrateClientFactory{admin: admin, workflow: workflowClient}
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	err := app.Run([]string{
		"tdbg",
		"--namespace", "test-namespace",
		"workflow", "scan-nexus-chasm",
		"--min-pending-age", "10m",
		"--rps", "10000",
		"--list-rps", "10000",
	})
	require.NoError(t, err)
	require.NotNil(t, workflowClient.listRequest)
	require.Equal(t, "test-namespace", workflowClient.listRequest.GetNamespace())
	require.Contains(t, workflowClient.listRequest.GetQuery(), "ExecutionStatus = 'Running'")
	require.Contains(t, workflowClient.listRequest.GetQuery(), "StartTime <=")
	require.Equal(t, int32(1), workflowClient.historyCalls.Load(), "history is read only for the CHASM match")
	require.Equal(t, "chasm-workflow", workflowClient.historyRequest.GetExecution().GetWorkflowId())

	admin.mu.Lock()
	require.Len(t, admin.requests, 2)
	for _, req := range admin.requests {
		require.True(t, req.GetSkipForceReload())
		require.Equal(t, uint32(chasm.WorkflowArchetypeID), req.GetArchetypeId())
	}
	admin.mu.Unlock()

	lines := bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)
	var candidate map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &candidate))
	require.Equal(t, "candidate", candidate["recordType"])
	require.Equal(t, "chasm-workflow", candidate["workflowId"])
	require.Equal(t, true, candidate["safeToReset"])
	require.InDelta(t, 9, candidate["recommendedResetEventId"], 0)
	var checkpoint map[string]any
	require.NoError(t, json.Unmarshal(lines[1], &checkpoint))
	require.Equal(t, "checkpoint", checkpoint["recordType"])
	require.Equal(t, true, checkpoint["complete"])
	require.InDelta(t, 1, checkpoint["candidates"], 0)
	require.NotContains(t, stdout.String(), "payload-secret")
	require.NotContains(t, stdout.String(), "completion-token-secret")
	require.Contains(t, stderr.String(), "2 listed, 2 described, 1 candidates, 0 errors")
}

func TestAdminScanNexusChasmRequiresExplicitNamespace(t *testing.T) {
	t.Parallel()

	app := tdbgtest.NewCliApp()
	err := app.Run([]string{"tdbg", "workflow", "scan-nexus-chasm"})
	require.ErrorContains(t, err, "explicit --namespace is required")
}

func nexusScanMutableStateWithOperation(t *testing.T, scheduledTime time.Time) *persistencespb.WorkflowMutableState {
	t.Helper()
	registry, err := all.NewNilRegistry(log.NewTestLogger())
	require.NoError(t, err)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(scheduledTime.Add(time.Hour))
	executionInfo := &persistencespb.WorkflowExecutionInfo{NamespaceId: "namespace-id", WorkflowId: "chasm-workflow"}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:  "chasm-run",
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	backend := &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("namespace-id", "chasm-workflow", "chasm-run")
		},
		HandleGetExecutionInfo:    func() *persistencespb.WorkflowExecutionInfo { return executionInfo },
		HandleGetExecutionState:   func() *persistencespb.WorkflowExecutionState { return executionState },
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNow:                 timeSource.Now,
	}
	tree := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	mutableCtx := chasm.NewMutableContext(context.Background(), tree)
	workflow := chasmworkflow.NewWorkflow(mutableCtx, chasm.NewMSPointer(backend))
	workflow.Operations = chasm.Map[int64, *nexusoperation.Operation]{
		5: chasm.NewComponentField(mutableCtx, nexusoperation.NewOperation(&nexusoperationpb.OperationState{
			Status:         nexusoperationpb.OPERATION_STATUS_STARTED,
			ScheduledTime:  timestamppb.New(scheduledTime),
			StartedTime:    timestamppb.New(scheduledTime.Add(2 * time.Minute)),
			Attempt:        1,
			Endpoint:       "endpoint-secret",
			OperationToken: "completion-token-secret",
		})),
	}
	require.NoError(t, tree.SetRootComponent(workflow))
	_, err = tree.CloseTransaction(context.Background(), true)
	require.NoError(t, err)
	return &persistencespb.WorkflowMutableState{
		ChasmNodes:     tree.Snapshot(nil).Nodes,
		ExecutionInfo:  executionInfo,
		ExecutionState: executionState,
	}
}

func workflowTaskCompletedHistoryEvent(eventID int64, eventTime time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		EventTime: timestamppb.New(eventTime),
	}
}
