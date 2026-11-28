package persistence

// RequestNamespaceID returns the ID of the namespace that a store request operates on, or "" if the
// request has none. Requests of shard-level, task range, cluster metadata, queue and Nexus
// endpoint operations have no namespace. Of the replication DLQ requests, only
// PutReplicationTaskToDLQRequest has one.
//
// History branch requests are left without a namespace on purpose, although the Info field of
// AppendHistoryNodes and ForkHistoryBranch can hold the namespace ID (see
// BuildHistoryGarbageCleanupInfo): it is free-form text that the other branch operations lack, so
// failpoints for the branch methods apply to all namespaces.
func RequestNamespaceID(request any) string {
	if r, ok := request.(interface{ RequestNamespaceID() string }); ok {
		return r.RequestNamespaceID()
	}
	return ""
}

// ExecutionStore.

func (r *InternalAddHistoryTasksRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *InternalConflictResolveWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.ResetWorkflowSnapshot.NamespaceID
}

func (r *InternalCreateWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.NewWorkflowSnapshot.NamespaceID
}

func (r *InternalSetWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.SetWorkflowSnapshot.NamespaceID
}

func (r *InternalUpdateWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.UpdateWorkflowMutation.NamespaceID
}

func (r *DeleteCurrentWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *DeleteWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *GetCurrentExecutionRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *GetWorkflowExecutionRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *PutReplicationTaskToDLQRequest) RequestNamespaceID() string {
	return r.TaskInfo.GetNamespaceId()
}

// MetadataStore.

func (r *InternalCreateNamespaceRequest) RequestNamespaceID() string {
	return r.ID
}

func (r *InternalUpdateNamespaceRequest) RequestNamespaceID() string {
	return r.Id
}

func (r *InternalRenameNamespaceRequest) RequestNamespaceID() string {
	if r.InternalUpdateNamespaceRequest == nil {
		return ""
	}
	return r.Id
}

func (r *DeleteNamespaceRequest) RequestNamespaceID() string {
	return r.ID
}

// RequestNamespaceID is empty if the namespace is looked up by name.
func (r *GetNamespaceRequest) RequestNamespaceID() string {
	return r.ID
}

// TaskStore.

func (r *InternalCreateTaskQueueRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *InternalGetTaskQueueRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *InternalUpdateTaskQueueRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *DeleteTaskQueueRequest) RequestNamespaceID() string {
	if r.TaskQueue == nil {
		return ""
	}
	return r.TaskQueue.NamespaceID
}

func (r *InternalCreateTasksRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *GetTasksRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *CompleteTasksLessThanRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *GetTaskQueueUserDataRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *InternalUpdateTaskQueueUserDataRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *ListTaskQueueUserDataEntriesRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *GetTaskQueuesByBuildIdRequest) RequestNamespaceID() string {
	return r.NamespaceID
}

func (r *CountTaskQueuesByBuildIdRequest) RequestNamespaceID() string {
	return r.NamespaceID
}
