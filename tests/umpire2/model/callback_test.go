package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
