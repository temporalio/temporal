package faultinjection

import (
	"go.temporal.io/server/common/persistence"
)

// requestNamespaceID returns the ID of the namespace that a store request operates on, or "" if
// the request has none or is nil. Requests of shard-level, history branch, task range, cluster
// metadata, queue and Nexus endpoint operations have no namespace. Of the replication DLQ requests,
// only PutReplicationTaskToDLQ has one.
//
//nolint:revive // cyclomatic: each request type stores its namespace ID in its own field
func requestNamespaceID(request any) string {
	switch r := request.(type) {
	// ExecutionStore
	case *persistence.InternalAddHistoryTasksRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.InternalConflictResolveWorkflowExecutionRequest:
		if r != nil {
			return r.ResetWorkflowSnapshot.NamespaceID
		}
	case *persistence.InternalCreateWorkflowExecutionRequest:
		if r != nil {
			return r.NewWorkflowSnapshot.NamespaceID
		}
	case *persistence.InternalSetWorkflowExecutionRequest:
		if r != nil {
			return r.SetWorkflowSnapshot.NamespaceID
		}
	case *persistence.InternalUpdateWorkflowExecutionRequest:
		if r != nil {
			return r.UpdateWorkflowMutation.NamespaceID
		}
	case *persistence.DeleteCurrentWorkflowExecutionRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.DeleteWorkflowExecutionRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.GetCurrentExecutionRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.GetWorkflowExecutionRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.PutReplicationTaskToDLQRequest:
		if r != nil {
			return r.TaskInfo.GetNamespaceId()
		}

	// MetadataStore
	case *persistence.InternalCreateNamespaceRequest:
		if r != nil {
			return r.ID
		}
	case *persistence.InternalUpdateNamespaceRequest:
		if r != nil {
			return r.Id
		}
	case *persistence.InternalRenameNamespaceRequest:
		if r != nil && r.InternalUpdateNamespaceRequest != nil {
			return r.Id
		}
	case *persistence.DeleteNamespaceRequest:
		if r != nil {
			return r.ID
		}
	case *persistence.GetNamespaceRequest:
		// The ID is empty when the namespace is looked up by name.
		if r != nil {
			return r.ID
		}

	// TaskStore
	case *persistence.InternalCreateTaskQueueRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.InternalGetTaskQueueRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.InternalUpdateTaskQueueRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.DeleteTaskQueueRequest:
		if r != nil && r.TaskQueue != nil {
			return r.TaskQueue.NamespaceID
		}
	case *persistence.InternalCreateTasksRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.GetTasksRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.CompleteTasksLessThanRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.GetTaskQueueUserDataRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.InternalUpdateTaskQueueUserDataRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.ListTaskQueueUserDataEntriesRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.GetTaskQueuesByBuildIdRequest:
		if r != nil {
			return r.NamespaceID
		}
	case *persistence.CountTaskQueuesByBuildIdRequest:
		if r != nil {
			return r.NamespaceID
		}
	}
	return ""
}
