package rule

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
)

// CallbackResponseConsistency verifies response idempotency without mutating accepted evidence.
type CallbackResponseConsistency struct{}

func (*CallbackResponseConsistency) Name() string { return "CallbackResponseConsistencyRule" }

func (*CallbackResponseConsistency) CheckSafety(c *umpire.SafetyContext) {
	if c.Config.Relations == nil {
		return
	}
	for result := range c.ChangedEntities() {
		callback, ok := result.Entity.(*model.Callback)
		if !ok {
			continue
		}
		if len(callback.DeliveryResponses) == 0 {
			continue
		}
		callbackID := scopedCallbackEntity(model.CallbackType, callback.NamespaceID, callback.CallbackID)
		operations := c.Config.Relations.Targets(protocol.CallbackOperationRelation, callbackID)
		if len(operations) != 1 {
			continue
		}
		operation := findCallbackOperation(c.ModelState, callback.NamespaceID, operations[0])
		if operation == nil {
			continue
		}
		holds := len(callback.ConflictingResponses) == 0
		if settledAt, settled := operation.SettledAt(); settled && callback.FirstResponseTime.After(settledAt) {
			holds = holds && operation.FSM.IsTerminal()
		}
		c.Eval(result.Key, holds, umpire.Violation{
			Message: "callback delivery received conflicting Nexus start responses",
			Tags: map[string]string{
				"callbackID":  callback.CallbackID,
				"operationID": operations[0].ID,
			},
		})
	}
}
