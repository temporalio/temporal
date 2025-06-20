package persistence

import "go.temporal.io/api/serviceerror"

func OperationPossiblySucceeded(err error) bool {
	switch err.(type) {
	case *CurrentWorkflowConditionFailedError,
		*WorkflowConditionFailedError,
		*ConditionFailedError,
		*ShardOwnershipLostError,
		*InvalidPersistenceRequestError,
		*TransactionSizeLimitError,
		*AppendHistoryTimeoutError, // this means task operations is not started
		*serviceerror.ResourceExhausted,
		*serviceerror.NotFound,
		*serviceerror.NamespaceNotFound:
		// Persistence failure that means that write was definitely not committed.
		return false
	default:
		return true
	}
}
