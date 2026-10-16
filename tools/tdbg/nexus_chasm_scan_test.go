package tdbg

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	hsmnexusoperations "go.temporal.io/server/service/history/hsm/nexusoperations"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDecodePendingChasmNexusOperations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.October, 15, 12, 0, 0, 0, time.UTC)
	registry, err := newChasmRegistry(log.NewTestLogger())
	require.NoError(t, err)
	mutableState := workflowMutableStateWithNexusOperations(t, registry, now, map[int64]*nexusoperationpb.OperationState{
		5: {
			Status:                  nexusoperationpb.OPERATION_STATUS_STARTED,
			ScheduledTime:           timestamppb.New(now.Add(-20 * time.Minute)),
			StartedTime:             timestamppb.New(now.Add(-15 * time.Minute)),
			LastAttemptCompleteTime: timestamppb.New(now.Add(-16 * time.Minute)),
			Attempt:                 3,
			Endpoint:                "customer-endpoint-secret",
			Service:                 "customer-service-secret",
			Operation:               "customer-operation-secret",
			RequestId:               "request-id-secret",
			OperationToken:          "completion-token-secret",
			LastAttemptFailure:      &failurepb.Failure{Message: "failure-secret"},
		},
		11: {
			Status:        nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			ScheduledTime: timestamppb.New(now.Add(-time.Minute)),
		},
		17: {
			Status:        nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			ScheduledTime: timestamppb.New(now.Add(-time.Hour)),
		},
	})

	operations, err := decodePendingNexusOperations(
		context.Background(),
		mutableState,
		registry,
	)
	require.NoError(t, err)
	require.True(t, markNexusChasmCandidateTriggers(operations, now.Add(-10*time.Minute)))
	require.Equal(t, []nexusChasmScanOperation{
		{
			ScheduledEventID:       5,
			Backend:                "chasm",
			CandidateTrigger:       true,
			State:                  nexusoperationpb.OPERATION_STATUS_STARTED.String(),
			Attempt:                3,
			ScheduledTime:          new(now.Add(-20 * time.Minute)),
			LastPersistedStateTime: new(now.Add(-15 * time.Minute)),
		},
		{
			ScheduledEventID:       11,
			Backend:                "chasm",
			State:                  nexusoperationpb.OPERATION_STATUS_SCHEDULED.String(),
			ScheduledTime:          new(now.Add(-time.Minute)),
			LastPersistedStateTime: new(now.Add(-time.Minute)),
		},
	}, operations)

	encoded, err := json.Marshal(operations)
	require.NoError(t, err)
	for _, secret := range []string{
		"customer-endpoint-secret",
		"customer-service-secret",
		"customer-operation-secret",
		"request-id-secret",
		"completion-token-secret",
		"failure-secret",
	} {
		require.NotContains(t, string(encoded), secret)
	}
}

func TestDecodePendingNexusOperationsIncludesBothBackends(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.October, 15, 12, 0, 0, 0, time.UTC)
	registry, err := newChasmRegistry(log.NewTestLogger())
	require.NoError(t, err)
	mutableState := workflowMutableStateWithNexusOperations(t, registry, now, map[int64]*nexusoperationpb.OperationState{
		5: {
			Status:        nexusoperationpb.OPERATION_STATUS_STARTED,
			ScheduledTime: timestamppb.New(now.Add(-20 * time.Minute)),
		},
		11: {
			Status:        nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			ScheduledTime: timestamppb.New(now.Add(-time.Minute)),
		},
	})
	addHSMNexusOperation(t, mutableState, 20, &persistencespb.NexusOperationInfo{
		State:         enumsspb.NEXUS_OPERATION_STATE_STARTED,
		ScheduledTime: timestamppb.New(now.Add(-2 * time.Minute)),
		StartedTime:   timestamppb.New(now.Add(-time.Minute)),
		Attempt:       2,
	})
	addHSMNexusOperation(t, mutableState, 25, &persistencespb.NexusOperationInfo{
		State:         enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
		ScheduledTime: timestamppb.New(now.Add(-time.Hour)),
	})

	operations, err := decodePendingNexusOperations(context.Background(), mutableState, registry)
	require.NoError(t, err)
	require.True(t, markNexusChasmCandidateTriggers(operations, now.Add(-10*time.Minute)))
	require.Len(t, operations, 3)
	require.Equal(t, []int64{5, 11, 20}, []int64{
		operations[0].ScheduledEventID,
		operations[1].ScheduledEventID,
		operations[2].ScheduledEventID,
	})
	require.Equal(t, []string{"chasm", "chasm", "hsm"}, []string{
		operations[0].Backend,
		operations[1].Backend,
		operations[2].Backend,
	})
	require.True(t, operations[0].CandidateTrigger)
	require.False(t, operations[1].CandidateTrigger)
	require.False(t, operations[2].CandidateTrigger)

	candidate := analyzeNexusChasmResetSafety(
		"namespace",
		"workflow",
		"run",
		operations,
		[]*historypb.HistoryEvent{
			nexusScheduledEvent(5, now.Add(-20*time.Minute)),
			nexusScheduledEvent(11, now.Add(-time.Minute)),
			nexusScheduledEvent(20, now.Add(-2*time.Minute)),
			workflowTaskCompletedEvent(21, now),
		},
	)
	require.True(t, candidate.ResetPointValid)
	require.Equal(t, int64(20), candidate.LatestRelevantNexusEvent.EventID)
	require.Equal(t, int64(21), candidate.ResetEventID)
}

func TestAnalyzeNexusChasmResetSafety(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.October, 15, 12, 0, 0, 0, time.UTC)
	operations := []nexusChasmScanOperation{{
		ScheduledEventID: 5,
		State:            "STARTED",
		ScheduledTime:    new(now.Add(-20 * time.Minute)),
	}}
	tests := []struct {
		name               string
		events             []*historypb.HistoryEvent
		resetPointValid    bool
		unsafeContains     string
		latestRelevantID   int64
		recommendedResetID int64
	}{
		{
			name: "safe legacy cancel outcome",
			events: []*historypb.HistoryEvent{
				nexusScheduledEvent(5, now),
				{
					EventId:   8,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
					EventTime: timestamppb.New(now.Add(time.Second)),
					Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
						NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{ScheduledEventId: 5},
					},
				},
				{
					EventId:   9,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
					EventTime: timestamppb.New(now.Add(2 * time.Second)),
					Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestCompletedEventAttributes{
						NexusOperationCancelRequestCompletedEventAttributes: &historypb.NexusOperationCancelRequestCompletedEventAttributes{RequestedEventId: 8},
					},
				},
				workflowTaskCompletedEvent(10, now.Add(3*time.Second)),
				workflowTaskCompletedEvent(12, now.Add(4*time.Second)),
			},
			resetPointValid:    true,
			latestRelevantID:   9,
			recommendedResetID: 12,
		},
		{
			name: "terminal event contradicts pending component",
			events: []*historypb.HistoryEvent{
				nexusScheduledEvent(5, now),
				{
					EventId:   8,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
					EventTime: timestamppb.New(now.Add(time.Second)),
					Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
						NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{ScheduledEventId: 5},
					},
				},
				workflowTaskCompletedEvent(9, now.Add(2*time.Second)),
			},
			resetPointValid:    false,
			unsafeContains:     "terminal history event 8",
			latestRelevantID:   5,
			recommendedResetID: 9,
		},
		{
			name: "missing scheduled event and workflow task boundary",
			events: []*historypb.HistoryEvent{
				{
					EventId:   8,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
					EventTime: timestamppb.New(now),
					Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
						NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{ScheduledEventId: 5},
					},
				},
			},
			resetPointValid:  false,
			unsafeContains:   "scheduled event 5 is missing",
			latestRelevantID: 8,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := analyzeNexusChasmResetSafety("namespace", "workflow", "run", operations, test.events)
			require.Equal(t, test.resetPointValid, candidate.ResetPointValid)
			require.Contains(t, candidate.ResetPointIssue, test.unsafeContains)
			require.True(t, candidate.OperatorConfirmationRequired)
			require.Contains(t, candidate.ReviewReason, "does not prove")
			require.NotNil(t, candidate.LatestRelevantNexusEvent)
			require.Equal(t, test.latestRelevantID, candidate.LatestRelevantNexusEvent.EventID)
			require.Equal(t, test.recommendedResetID, candidate.ResetEventID)
		})
	}
}

func TestAnalyzeNexusChasmResetSafetyRejectsOperationInBothTrees(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	candidate := analyzeNexusChasmResetSafety(
		"namespace",
		"workflow",
		"run",
		[]nexusChasmScanOperation{
			{ScheduledEventID: 5, Backend: "chasm", ScheduledTime: new(now)},
			{ScheduledEventID: 5, Backend: "hsm", ScheduledTime: new(now)},
		},
		[]*historypb.HistoryEvent{nexusScheduledEvent(5, now), workflowTaskCompletedEvent(6, now)},
	)
	require.False(t, candidate.ResetPointValid)
	require.Contains(t, candidate.ResetPointIssue, "exists in both state trees")
}

func TestNexusChasmScanCursorRoundTrip(t *testing.T) {
	t.Parallel()

	in := &nexusChasmScanInput{
		namespace:       "namespace",
		query:           "ExecutionStatus = 'Running'",
		minPendingAge:   10 * time.Minute,
		maxHistoryPages: 100,
		asOf:            time.Date(2026, time.October, 15, 12, 0, 0, 123, time.UTC),
	}
	token, err := encodeNexusChasmScanCursor(in, []byte("next-page"), 10, 9, 2)
	require.NoError(t, err)
	cursor, err := decodeNexusChasmScanCursor(token)
	require.NoError(t, err)
	require.Equal(t, nexusChasmScanCursorVersion, cursor.Version)
	require.Equal(t, in.namespace, cursor.Namespace)
	require.Equal(t, nexusChasmQuerySHA256(in.query), cursor.QuerySHA256)
	require.Equal(t, in.minPendingAge.String(), cursor.MinPendingAge)
	require.Equal(t, in.maxHistoryPages, cursor.MaxHistoryPages)
	require.Equal(t, in.asOf.Format(time.RFC3339Nano), cursor.AsOf)
	require.Equal(t, int64(10), cursor.Listed)
	require.Equal(t, int64(9), cursor.Described)
	require.Equal(t, int64(2), cursor.Candidates)
	require.NotContains(t, token, "next-page")
}

func TestNexusChasmScanRetryable(t *testing.T) {
	t.Parallel()

	active := context.Background()
	require.True(t, nexusChasmScanRetryable(active, serviceerror.NewUnavailable("retry")))
	require.True(t, nexusChasmScanRetryable(active, serviceerror.NewInternal("retry")))
	require.True(t, nexusChasmScanRetryable(active, context.DeadlineExceeded))
	require.False(t, nexusChasmScanRetryable(active, serviceerror.NewPermissionDenied("stop", "")))
	require.False(t, nexusChasmScanRetryable(active, serviceerror.NewUnimplemented("stop")))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, nexusChasmScanRetryable(canceled, context.DeadlineExceeded))
}

func TestCanonicalVisibilityPredicate(t *testing.T) {
	t.Parallel()

	predicate, err := canonicalVisibilityPredicate("WorkflowType = 'type' OR WorkflowId = 'id'")
	require.NoError(t, err)
	require.Equal(t, "WorkflowType = 'type' or WorkflowId = 'id'", predicate)

	_, err = canonicalVisibilityPredicate("WorkflowId = 'id') OR ExecutionStatus = 'Completed' OR (WorkflowId = 'other'")
	require.Error(t, err)
	_, err = canonicalVisibilityPredicate("WorkflowType = 'type' ORDER BY StartTime")
	require.ErrorContains(t, err, "ORDER BY")
}

type reverseHistoryPagingClient struct {
	workflowservice.WorkflowServiceClient
	pages    []*workflowservice.GetWorkflowExecutionHistoryReverseResponse
	requests []*workflowservice.GetWorkflowExecutionHistoryReverseRequest
}

func (c *reverseHistoryPagingClient) GetWorkflowExecutionHistoryReverse(
	_ context.Context,
	req *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
	_ ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	c.requests = append(c.requests, req)
	return c.pages[len(c.requests)-1], nil
}

func TestLoadHistoryStopsAfterOldestTarget(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	client := &reverseHistoryPagingClient{pages: []*workflowservice.GetWorkflowExecutionHistoryReverseResponse{
		{
			History: &historypb.History{Events: []*historypb.HistoryEvent{
				workflowTaskCompletedEvent(20, now),
				{
					EventId:   15,
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
					EventTime: timestamppb.New(now.Add(-time.Second)),
					Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
						NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{ScheduledEventId: 5},
					},
				},
			}},
			NextPageToken: []byte("page-2"),
		},
		{
			History:       &historypb.History{Events: []*historypb.HistoryEvent{nexusScheduledEvent(5, now.Add(-time.Minute))}},
			NextPageToken: []byte("page-3-that-should-not-be-read"),
		},
		{
			History: &historypb.History{},
		},
	}}
	scanner := nexusChasmScanner{
		input:          &nexusChasmScanInput{namespace: "namespace", maxHistoryPages: 100},
		workflowClient: client,
		nonListLimiter: rate.NewLimiter(rate.Inf, 1),
		rpcTimeout:     time.Second,
	}

	events, err := scanner.loadHistory(
		context.Background(),
		"workflow",
		"run",
		[]nexusChasmScanOperation{{ScheduledEventID: 5}},
	)
	require.NoError(t, err)
	require.Len(t, client.requests, 2)
	require.Len(t, events, 3)
	require.Equal(t, int64(5), events[0].GetEventId())
	require.Equal(t, []byte("page-2"), client.requests[1].GetNextPageToken())
}

func TestLoadHistoryStopsAfterPassingMissingScheduledEvent(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	client := &reverseHistoryPagingClient{pages: []*workflowservice.GetWorkflowExecutionHistoryReverseResponse{
		{
			History: &historypb.History{Events: []*historypb.HistoryEvent{
				workflowTaskCompletedEvent(20, now),
			}},
			NextPageToken: []byte("page-2"),
		},
		{
			History:       &historypb.History{Events: []*historypb.HistoryEvent{workflowTaskCompletedEvent(4, now.Add(-time.Minute))}},
			NextPageToken: []byte("page-3-that-should-not-be-read"),
		},
	}}
	scanner := nexusChasmScanner{
		input:          &nexusChasmScanInput{namespace: "namespace", maxHistoryPages: 100},
		workflowClient: client,
		nonListLimiter: rate.NewLimiter(rate.Inf, 1),
		rpcTimeout:     time.Second,
	}

	events, err := scanner.loadHistory(
		context.Background(),
		"workflow",
		"run",
		[]nexusChasmScanOperation{{ScheduledEventID: 5}},
	)
	require.NoError(t, err)
	require.Len(t, client.requests, 2)
	require.Len(t, events, 2)
}

func TestLoadHistoryHonorsPageCap(t *testing.T) {
	t.Parallel()

	client := &reverseHistoryPagingClient{pages: []*workflowservice.GetWorkflowExecutionHistoryReverseResponse{{
		History:       &historypb.History{Events: []*historypb.HistoryEvent{workflowTaskCompletedEvent(20, time.Now().UTC())}},
		NextPageToken: []byte("more"),
	}}}
	scanner := nexusChasmScanner{
		input:          &nexusChasmScanInput{namespace: "namespace", maxHistoryPages: 1},
		workflowClient: client,
		nonListLimiter: rate.NewLimiter(rate.Inf, 1),
		rpcTimeout:     time.Second,
	}

	_, err := scanner.loadHistory(
		context.Background(),
		"workflow",
		"run",
		[]nexusChasmScanOperation{{ScheduledEventID: 5}},
	)
	require.ErrorContains(t, err, "exceeded --max-history-pages=1")
}

func workflowMutableStateWithNexusOperations(
	t *testing.T,
	registry *chasm.Registry,
	now time.Time,
	states map[int64]*nexusoperationpb.OperationState,
) *persistencespb.WorkflowMutableState {
	t.Helper()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: "namespace-id",
		WorkflowId:  "workflow-id",
	}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:  "run-id",
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	backend := &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
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
	workflow.Operations = make(chasm.Map[int64, *nexusoperation.Operation], len(states))
	for scheduledEventID, state := range states {
		workflow.Operations[scheduledEventID] = chasm.NewComponentField(mutableCtx, nexusoperation.NewOperation(state))
	}
	require.NoError(t, tree.SetRootComponent(workflow))
	_, err := tree.CloseTransaction(context.Background(), true)
	require.NoError(t, err)

	return &persistencespb.WorkflowMutableState{
		ChasmNodes:     tree.Snapshot(nil).Nodes,
		ExecutionInfo:  executionInfo,
		ExecutionState: executionState,
	}
}

func addHSMNexusOperation(
	t *testing.T,
	mutableState *persistencespb.WorkflowMutableState,
	scheduledEventID int64,
	operation *persistencespb.NexusOperationInfo,
) {
	t.Helper()
	data, err := proto.Marshal(operation)
	require.NoError(t, err)
	if mutableState.ExecutionInfo.SubStateMachinesByType == nil {
		mutableState.ExecutionInfo.SubStateMachinesByType = make(map[string]*persistencespb.StateMachineMap)
	}
	machineMap := mutableState.ExecutionInfo.SubStateMachinesByType[hsmnexusoperations.OperationMachineType]
	if machineMap == nil {
		machineMap = &persistencespb.StateMachineMap{MachinesById: make(map[string]*persistencespb.StateMachineNode)}
		mutableState.ExecutionInfo.SubStateMachinesByType[hsmnexusoperations.OperationMachineType] = machineMap
	}
	machineMap.MachinesById[strconv.FormatInt(scheduledEventID, 10)] = &persistencespb.StateMachineNode{Data: data}
}

func nexusScheduledEvent(eventID int64, eventTime time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		EventTime: timestamppb.New(eventTime),
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Input: &commonpb.Payload{Data: []byte("payload-secret")},
			},
		},
	}
}

func workflowTaskCompletedEvent(eventID int64, eventTime time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		EventTime: timestamppb.New(eventTime),
	}
}

//go:fix inline
func timePtr(value time.Time) *time.Time {
	return new(value)
}

func TestScanOutputDoesNotContainHistoryPayload(t *testing.T) {
	t.Parallel()

	candidate := analyzeNexusChasmResetSafety(
		"namespace",
		"workflow",
		"run",
		[]nexusChasmScanOperation{{ScheduledEventID: 5, State: "STARTED"}},
		[]*historypb.HistoryEvent{nexusScheduledEvent(5, time.Now()), workflowTaskCompletedEvent(6, time.Now())},
	)
	encoded, err := json.Marshal(candidate)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "payload-secret")
}
