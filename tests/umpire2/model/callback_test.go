package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

func TestCallbackRetainsFirstNonSecretTargetsAcrossDuplicateObservations(t *testing.T) {
	callback := NewCallback()
	path := &umpire.EntityPath{EntityID: umpire.NewEntityID(CallbackType, "callback-id")}
	facts := []umpire.Fact{
		&fact.NexusCallbackObservation{CallbackID: "callback-id", OperationID: "operation-id", OperationRunID: "operation-run-id"},
		&fact.NexusCallbackObservation{CallbackID: "callback-id", OperationID: "operation-id", OperationRunID: "operation-run-id"},
		&fact.WorkflowCallbackAttachment{CallbackID: "callback-id", HandlerWorkflowID: "handler-id", HandlerRunID: "handler-run-id"},
	}
	require.NoError(t, callback.OnFact(context.Background(), path, func(yield func(umpire.Fact) bool) {
		for _, observed := range facts {
			if !yield(observed) {
				return
			}
		}
	}))
	require.Equal(t, "callback-id", callback.CallbackID)
	require.Equal(t, "operation-id", callback.OperationID)
	require.Equal(t, "handler-run-id", callback.HandlerRunID)
}

func TestCallbackRetainsMalformedEvidence(t *testing.T) {
	callback := NewCallback()
	require.NoError(t, callback.OnFact(context.Background(), nil, func(yield func(umpire.Fact) bool) {
		yield(&fact.NexusCallbackObservation{CallbackID: "callback-id", Malformed: true, ErrorClass: "invalid-callback-token"})
	}))
	require.True(t, callback.Malformed)
	require.Equal(t, "invalid-callback-token", callback.ErrorClass)
}

func TestCallbackRetainsHistoryAttachmentReference(t *testing.T) {
	startedAt := time.Date(2026, time.August, 12, 18, 0, 0, 0, time.UTC)
	attachedAt := startedAt.Add(time.Minute)
	callback := NewCallback()
	attachment := &fact.WorkflowCallbackAttachment{
		CallbackID:               "callback-id",
		HandlerWorkflowID:        "handler-id",
		HandlerRunID:             "handler-run-id",
		HandlerWorkflowStartTime: startedAt,
		AttachmentEventTime:      attachedAt,
		AttachmentEventID:        7,
		ReferenceKind:            "request",
		ReferenceValue:           "request-id",
		ReferencedEventType:      enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
	}

	require.NoError(t, callback.OnFact(context.Background(), nil, func(yield func(umpire.Fact) bool) {
		yield(attachment)
	}))
	require.Equal(t, startedAt, callback.HandlerWorkflowStartTime)
	require.Equal(t, attachedAt, callback.AttachmentEventTime)
	require.Equal(t, int64(7), callback.AttachmentEventID)
	require.Equal(t, "request", callback.ReferenceKind)
	require.Equal(t, "request-id", callback.ReferenceValue)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, callback.ReferencedEventType)
}

func TestCallbackAcceptsDuplicateResponseAndRetainsConflictingEvidence(t *testing.T) {
	firstAt := time.Date(2026, time.August, 12, 19, 0, 0, 0, time.UTC)
	callback := NewCallback()
	accepted := &fact.NexusStartResponse{
		CallbackID:          "callback-id",
		DeliveryID:          "delivery-id",
		ResponseKind:        "async_success",
		ResponseFingerprint: "accepted-fingerprint",
		ObservedAt:          firstAt,
	}
	duplicate := *accepted
	duplicate.ObservedAt = firstAt.Add(time.Second)
	conflict := *accepted
	conflict.ResponseKind = "failure"
	conflict.ResponseFingerprint = "conflicting-fingerprint"
	conflict.ObservedAt = firstAt.Add(2 * time.Second)

	require.NoError(t, callback.OnFact(context.Background(), nil, func(yield func(umpire.Fact) bool) {
		for _, observed := range []umpire.Fact{accepted, &duplicate, &conflict} {
			yield(observed)
		}
	}))
	require.Equal(t, "async_success", callback.ResponseKind)
	require.Equal(t, firstAt, callback.FirstResponseTime)
	require.Len(t, callback.DeliveryResponses, 1)
	require.Equal(t, "accepted-fingerprint", callback.DeliveryResponses["delivery-id"].Fingerprint)
	require.Len(t, callback.ConflictingResponses, 1)
	require.Equal(t, "conflicting-fingerprint", callback.ConflictingResponses[0].Fingerprint)
}
