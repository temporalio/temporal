package rule

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
)

// CallbackReferenceConsistency verifies the public callback attachment selected by a Nexus start.
type CallbackReferenceConsistency struct{}

func (*CallbackReferenceConsistency) Name() string { return "CallbackReferenceConsistencyRule" }

func (*CallbackReferenceConsistency) CheckSafety(c *umpire.SafetyContext) {
	if c.Config.Relations == nil {
		return
	}
	for result := range c.ChangedEntities() {
		callback, ok := result.Entity.(*model.Callback)
		if !ok {
			continue
		}
		if callback.NamespaceID == "" || callback.CallbackID == "" {
			continue
		}
		callbackID := scopedCallbackEntity(model.CallbackType, callback.NamespaceID, callback.CallbackID)
		operations := c.Config.Relations.Targets(protocol.CallbackOperationRelation, callbackID)
		handlerRuns := c.Config.Relations.Targets(protocol.CallbackHandlerRunRelation, callbackID)
		if len(operations) != 1 || len(handlerRuns) != 1 {
			continue
		}
		operation := findCallbackOperation(c.ModelState, callback.NamespaceID, operations[0])
		if operation == nil {
			continue
		}
		if !callback.Malformed && !operation.StartHistoryMalformed && (callback.ReferenceKind == "" || operation.StartReferenceKind == "" || operation.StartHistoryEventTime.IsZero()) {
			continue
		}
		expectedHandlerRun := scopedCallbackEntity(model.WorkflowRunType, callback.NamespaceID, callback.HandlerRunID)
		holds := !callback.Malformed &&
			!operation.StartHistoryMalformed &&
			handlerRuns[0] == expectedHandlerRun &&
			operation.HandlerWorkflowID == callback.HandlerWorkflowID &&
			operation.HandlerRunID == callback.HandlerRunID &&
			operation.StartReferenceKind == callback.ReferenceKind &&
			operation.StartReferenceValue == callback.ReferenceValue &&
			operation.StartReferencedEventType == callback.ReferencedEventType &&
			callbackReferenceTimeConsistent(operation, callback)
		c.Eval(result.Key, holds, umpire.Violation{
			Message: "Nexus start callback reference does not match the handler attachment",
			Tags: map[string]string{
				"callbackID":   callback.CallbackID,
				"operationID":  operations[0].ID,
				"handlerRunID": callback.HandlerRunID,
			},
		})
	}
}

func callbackReferenceTimeConsistent(operation *model.NexusOperation, callback *model.Callback) bool {
	referencedAt := callback.HandlerWorkflowStartTime
	if callback.ReferenceKind == "request" {
		referencedAt = callback.AttachmentEventTime
	}
	return !referencedAt.IsZero() && !operation.StartHistoryEventTime.Before(referencedAt)
}

func findCallbackOperation(state *umpire.ModelState, namespaceID string, target umpire.EntityID) *model.NexusOperation {
	for _, entry := range state.QueryEntities(model.NexusOperationType, 0, nil) {
		operation, ok := entry.Entity.(*model.NexusOperation)
		if !ok || operation.NamespaceID != namespaceID {
			continue
		}
		identity := operation.WorkflowID + ":" + operation.ScheduledEventID
		if scopedCallbackEntity(model.NexusOperationType, namespaceID, identity) == target {
			return operation
		}
	}
	return nil
}

func scopedCallbackEntity(entityType umpire.EntityType, namespaceID, id string) umpire.EntityID {
	return umpire.NewEntityID(entityType, namespaceID+"\x00"+id)
}
