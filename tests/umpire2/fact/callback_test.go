package fact

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/components/nexusoperations"
)

func TestNexusCallbackObservationNormalizesHSMTargetWithoutSecrets(t *testing.T) {
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "operation-id",
		RunId:       "run-id",
		Ref:         &persistencespb.StateMachineRef{},
		RequestId:   "operation-request-id",
	})
	require.NoError(t, err)
	start := &nexuspb.StartOperationRequest{
		RequestId: "handler-request-id",
		Callback:  "https://secret.example/callback",
		CallbackHeader: map[string]string{
			strings.ToLower(commonnexus.CallbackTokenHeader): token,
			"authorization": "secret",
		},
	}

	observed := NewNexusCallbackObservation("namespace-id", start)
	require.False(t, observed.Malformed)
	require.Equal(t, "operation-id", observed.OperationID)
	require.Equal(t, "run-id", observed.OperationRunID)
	require.Equal(t, "operation-request-id", observed.OperationRequestID)
	require.Equal(t, "handler-request-id", observed.HandlerRequestID)
	require.NotContains(t, observed.CallbackID, "secret")
	require.NotContains(t, observed.CallbackID, "example")
	require.Equal(t, CallbackType, observed.TargetEntity().EntityID.Type)
}

func TestNexusCallbackObservationNormalizesCHASMTargetAndMalformedEvidence(t *testing.T) {
	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: "namespace-id",
		BusinessId:  "operation-id",
		RunId:       "run-id",
	}).Marshal()
	require.NoError(t, err)
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		ComponentRef: componentRef,
		RequestId:    "operation-request-id",
	})
	require.NoError(t, err)
	observed := NewNexusCallbackObservation("namespace-id", &nexuspb.StartOperationRequest{
		Callback:       "https://callback",
		CallbackHeader: map[string]string{commonnexus.CallbackTokenHeader: token},
	})
	require.False(t, observed.Malformed)
	require.Equal(t, "operation-id", observed.OperationID)

	malformed := NewNexusCallbackObservation("namespace-id", &nexuspb.StartOperationRequest{
		Callback:       "https://callback",
		CallbackHeader: map[string]string{commonnexus.CallbackTokenHeader: "not-a-token"},
	})
	require.True(t, malformed.Malformed)
	require.Equal(t, "invalid-callback-token", malformed.ErrorClass)
	require.NotEmpty(t, malformed.CallbackID)

	invalidRouting := NewNexusCallbackObservation("namespace-id", &nexuspb.StartOperationRequest{
		Callback: string([]byte{0xff}),
	})
	require.True(t, invalidRouting.Malformed)
	require.Equal(t, "invalid-callback", invalidRouting.ErrorClass)
}

func TestWorkflowCallbackAttachmentUsesSameRoutingIdentity(t *testing.T) {
	callback := &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
		Url: "https://callback",
		Header: map[string]string{
			"second": "value-2",
			"first":  "value-1",
		},
	}}}
	observed := NewWorkflowCallbackAttachment("namespace-id", "handler-id", "handler-run-id", "request-id", callback)
	require.False(t, observed.Malformed)
	require.Equal(t, "handler-id", observed.HandlerWorkflowID)
	require.Equal(t, "handler-run-id", observed.HandlerRunID)
	require.Equal(t, "request-id", observed.RequestID)

	reordered := &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
		Url:    "https://callback",
		Header: map[string]string{"first": "value-1", "second": "value-2"},
	}}}
	require.Equal(t, observed.CallbackID, NewWorkflowCallbackAttachment("namespace-id", "handler-id", "handler-run-id", "request-id", reordered).CallbackID)
}

func TestNexusTerminalObservationHashesResultAndSortsLinks(t *testing.T) {
	left := &commonpb.Link{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{
		Namespace: "namespace", ActivityId: "activity-b", RunId: "run-b",
	}}}
	right := &commonpb.Link{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{
		Namespace: "namespace", ActivityId: "activity-a", RunId: "run-a",
	}}}
	event := &historypb.HistoryEvent{
		EventId:   9,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: 5,
				Result:           payloads.MustEncodeSingle("secret-result"),
			},
		},
		Links: []*commonpb.Link{left, right},
	}

	observed := NewNexusOperationTerminal("namespace-id", "workflow-id", event)
	require.NotNil(t, observed)
	require.NotContains(t, observed.ResultDigest, "secret-result")
	require.Len(t, observed.LinkDigests, 2)
	require.IsIncreasing(t, observed.LinkDigests)
	require.Equal(t, []string{
		"activity:namespace/activity-a/run-a",
		"activity:namespace/activity-b/run-b",
	}, observed.LinkEndpoints)

	event.Links = append(event.Links, &commonpb.Link{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{
		Namespace: string([]byte{0xff}), ActivityId: "activity", RunId: "run",
	}}})
	malformed := NewNexusOperationTerminal("namespace-id", "workflow-id", event)
	require.True(t, malformed.Malformed)
	require.Equal(t, "invalid-terminal-link", malformed.ErrorClass)
}

func TestWorkflowNexusStorageSnapshotListsHSMAndCHASMOperations(t *testing.T) {
	state := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{SubStateMachinesByType: map[string]*persistencespb.StateMachineMap{
			nexusoperations.OperationMachineType: {MachinesById: map[string]*persistencespb.StateMachineNode{"2": {}, "1": {}}},
		}},
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"Operations#4": {}, "Other#ignored": {}, "Operations#3": {},
		},
	}
	observed := NewWorkflowNexusStorageSnapshot("namespace-id", "workflow-id", state)
	require.Equal(t, []string{"1", "2"}, observed.HSMOperationIDs)
	require.Equal(t, []string{"3", "4"}, observed.CHASMOperationIDs)
	require.Equal(t, []string{"1", "2", "3", "4"}, observed.OperationIDs)

	empty := NewWorkflowNexusStorageSnapshot("namespace-id", "workflow-id", &persistencespb.WorkflowMutableState{})
	require.Empty(t, empty.OperationIDs)
}
