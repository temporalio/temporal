package rule

import (
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// NexusOperationTimeoutSemantics checks timeout kind and failure metadata against the observed configuration.
type NexusOperationTimeoutSemantics struct{}

func (*NexusOperationTimeoutSemantics) Name() string {
	return "NexusOperationTimeoutSemanticsRule"
}

func (*NexusOperationTimeoutSemantics) CheckSafety(c *umpire.SafetyContext) {
	for result := range c.ChangedLifecycles() {
		operation, ok := result.Entity.(*model.NexusOperation)
		if !ok || operation.StartToCloseTimeout <= 0 || operation.TimeoutType == enumspb.TIMEOUT_TYPE_UNSPECIFIED {
			continue
		}
		operationID := operation.WorkflowID + ":" + operation.ScheduledEventID
		c.Eval(operationID+":type", operation.TimeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE, umpire.Violation{
			Message: "Nexus operation timeout kind does not match configured start-to-close timeout",
			Tags: map[string]string{
				"operationID": operationID,
				"timeoutType": operation.TimeoutType.String(),
			},
		})
		c.Eval(operationID+":message", strings.Contains(operation.TimeoutMessage, "operation timed out"), umpire.Violation{
			Message: "Nexus operation timeout failure metadata does not describe an operation timeout",
			Tags: map[string]string{
				"operationID": operationID,
				"message":     operation.TimeoutMessage,
			},
		})
	}
}
