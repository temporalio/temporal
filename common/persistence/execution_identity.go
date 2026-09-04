package persistence

import "go.temporal.io/server/chasm"

// ExecutionIdentity returns the archetype and primary execution key for the request.
func (r *InternalCreateWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, workflowSnapshotExecutionKey(r.NewWorkflowSnapshot)
}

// ExecutionIdentity returns the archetype and primary execution key for the request.
func (r *InternalUpdateWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, workflowMutationExecutionKey(r.UpdateWorkflowMutation)
}

// ExecutionIdentity returns the archetype and primary execution key for the request.
func (r *InternalConflictResolveWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, workflowSnapshotExecutionKey(r.ResetWorkflowSnapshot)
}

// ExecutionIdentity returns the archetype and primary execution key for the request.
func (r *InternalSetWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, workflowSnapshotExecutionKey(r.SetWorkflowSnapshot)
}

// ExecutionIdentity returns the archetype and execution key for the request.
func (r *GetCurrentExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, chasm.ExecutionKey{
		NamespaceID: r.NamespaceID,
		BusinessID:  r.WorkflowID,
	}
}

// ExecutionIdentity returns the archetype and execution key for the request.
func (r *GetWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, chasm.ExecutionKey{
		NamespaceID: r.NamespaceID,
		BusinessID:  r.WorkflowID,
		RunID:       r.RunID,
	}
}

// ExecutionIdentity returns the archetype and execution key for the request.
func (r *DeleteCurrentWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, chasm.ExecutionKey{
		NamespaceID: r.NamespaceID,
		BusinessID:  r.WorkflowID,
		RunID:       r.RunID,
	}
}

// ExecutionIdentity returns the archetype and execution key for the request.
func (r *DeleteWorkflowExecutionRequest) ExecutionIdentity() (chasm.ArchetypeID, chasm.ExecutionKey) {
	return r.ArchetypeID, chasm.ExecutionKey{
		NamespaceID: r.NamespaceID,
		BusinessID:  r.WorkflowID,
		RunID:       r.RunID,
	}
}

func workflowMutationExecutionKey(mutation InternalWorkflowMutation) chasm.ExecutionKey {
	return chasm.ExecutionKey{
		NamespaceID: mutation.NamespaceID,
		BusinessID:  mutation.WorkflowID,
		RunID:       mutation.RunID,
	}
}

func workflowSnapshotExecutionKey(snapshot InternalWorkflowSnapshot) chasm.ExecutionKey {
	return chasm.ExecutionKey{
		NamespaceID: snapshot.NamespaceID,
		BusinessID:  snapshot.WorkflowID,
		RunID:       snapshot.RunID,
	}
}
