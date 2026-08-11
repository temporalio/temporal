package namespacereplication

import (
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
)

// NamespaceMutationComponent is a CHASM component that applies a single
// namespace mutation: local CAS commit on the host cell, then parallel apply-if-higher
// fan-out to peer cells via the cross-cluster admin RPC.
//
// One component per mutation invocation. BusinessID = namespace_id:mutation_uuid
// (unique per invocation), so concurrent mutations on the same namespace each get
// their own component rather than blocking one another; the component is started
// with chasm.StartExecution and awaited with chasm.PollComponent. Serialization of
// concurrent same-namespace mutations happens at the metadata store's version-CAS
// in ApplyLocalTask (matching legacy UpdateNamespace), not in the CHASM engine.
type NamespaceMutationComponent struct {
	chasm.UnimplementedComponent

	*namespacereplicationpb.NamespaceMutationState

	// Visibility child component. Makes the parent component listable via
	// `temporal workflow list` (with the BusinessID alias registered in
	// library.go surfaced as the NamespaceMutationId search attribute). Without
	// this field, no visibility record is written and the component is
	// only discoverable via tdbg.
	Visibility chasm.Field[*chasm.Visibility]
}

var _ chasm.RootComponent = (*NamespaceMutationComponent)(nil)
var _ chasm.StateMachine[namespacereplicationpb.ComponentStatus] = (*NamespaceMutationComponent)(nil)

// NewNamespaceMutationComponent constructs a fresh component with the mutation set
// and per-peer status entries initialized to PENDING.
func NewNamespaceMutationComponent(mutation *namespacereplicationpb.NamespaceMutation) *NamespaceMutationComponent {
	peerApply := make(map[string]*namespacereplicationpb.PeerApplyStatus, len(mutation.GetPeerCells()))
	for _, cell := range mutation.GetPeerCells() {
		peerApply[cell] = &namespacereplicationpb.PeerApplyStatus{
			Outcome: namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING,
		}
	}
	return &NamespaceMutationComponent{
		NamespaceMutationState: &namespacereplicationpb.NamespaceMutationState{
			Mutation: mutation,
			Status:   namespacereplicationpb.COMPONENT_STATUS_RUNNING,
			LocalApply: &namespacereplicationpb.LocalApplyStatus{
				Outcome: namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING,
			},
			PeerApply: peerApply,
		},
	}
}

// LifecycleState implements the chasm.Component interface.
// COMPONENT_STATUS_RUNNING → LifecycleStateRunning (the component is doing work).
// COMPONENT_STATUS_COMPLETED → LifecycleStateCompleted (all tasks reached terminal state).
// COMPONENT_STATUS_FAILED → LifecycleStateFailed (local apply failed; peers never attempted).
func (c *NamespaceMutationComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch c.GetStatus() {
	case namespacereplicationpb.COMPONENT_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case namespacereplicationpb.COMPONENT_STATUS_FAILED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (c *NamespaceMutationComponent) StateMachineState() namespacereplicationpb.ComponentStatus {
	return c.GetStatus()
}

func (c *NamespaceMutationComponent) SetStateMachineState(status namespacereplicationpb.ComponentStatus) {
	c.Status = status
}

// ContextMetadata implements chasm.RootComponent. Returns metadata to propagate
// on the request context. For namespace replication we don't need extra metadata
// today; returning nil is fine.
func (c *NamespaceMutationComponent) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate implements chasm.RootComponent. Allows the framework to force-close
// the component (e.g., on execution state size limits). For the namespace
// replication transport, terminate just marks the component as failed; any
// in-flight tasks will see the closed lifecycle on next validate and skip.
//
// Apply-if-higher on receivers makes future mutations safe even when a
// component was terminated mid-flight.
func (c *NamespaceMutationComponent) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	c.SetStateMachineState(namespacereplicationpb.COMPONENT_STATUS_FAILED)
	return chasm.TerminateComponentResponse{}, nil
}

// allPeersTerminal reports whether every peer has reached a terminal outcome
// (Applied, NoOpStale, NotAdmitted, or FailedTerminal). Used to decide when to
// move the component to COMPLETED.
func (c *NamespaceMutationComponent) allPeersTerminal() bool {
	for _, status := range c.GetPeerApply() {
		switch status.GetOutcome() {
		case namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED,
			namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE,
			namespacereplicationpb.PEER_APPLY_OUTCOME_NOT_ADMITTED,
			namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL:
			// terminal
		default:
			return false
		}
	}
	return true
}
