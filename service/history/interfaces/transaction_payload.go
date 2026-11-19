package interfaces

import "go.temporal.io/server/common/persistence"

type MutationTransactionPayload struct {
	ExecutionMutation         *persistence.WorkflowMutation
	ExecutionEvents           []*persistence.WorkflowEvents
	ExecutionChasmLogMessages []*persistence.ChasmLogMessages
}

type SnapshotTransactionPayload struct {
	ExecutionSnapshot         *persistence.WorkflowSnapshot
	ExecutionEvents           []*persistence.WorkflowEvents
	ExecutionChasmLogMessages []*persistence.ChasmLogMessages
}
