package util

import "go.temporal.io/server/common/persistence"

// WrappingExecutionStore is a utility for creating an ExecutionStore which delegates to another ExecutionStore and
// overrides some functions.
type WrappingExecutionStore struct {
	Delegate persistence.ExecutionStore
}

func (w *WrappingExecutionStore) Close() {
	w.Delegate.Close()
}

func (w *WrappingExecutionStore) GetName() string {
	return w.Delegate.GetName()
}

func (w *WrappingExecutionStore) GetWorkflowExecution(request *persistence.GetWorkflowExecutionRequest) (
	*persistence.InternalGetWorkflowExecutionResponse,
	error,
) {
	return w.Delegate.GetWorkflowExecution(request)
}

func (w *WrappingExecutionStore) UpdateWorkflowExecution(request *persistence.InternalUpdateWorkflowExecutionRequest) error {
	return w.Delegate.UpdateWorkflowExecution(request)
}

func (w *WrappingExecutionStore) ConflictResolveWorkflowExecution(request *persistence.InternalConflictResolveWorkflowExecutionRequest) error {
	return w.Delegate.ConflictResolveWorkflowExecution(request)
}

func (w *WrappingExecutionStore) CreateWorkflowExecution(request *persistence.InternalCreateWorkflowExecutionRequest) (
	*persistence.InternalCreateWorkflowExecutionResponse,
	error,
) {
	return w.Delegate.CreateWorkflowExecution(request)
}

func (w *WrappingExecutionStore) DeleteWorkflowExecution(request *persistence.DeleteWorkflowExecutionRequest) error {
	return w.Delegate.DeleteWorkflowExecution(request)
}

func (w *WrappingExecutionStore) DeleteCurrentWorkflowExecution(request *persistence.DeleteCurrentWorkflowExecutionRequest) error {
	return w.Delegate.DeleteCurrentWorkflowExecution(request)
}

func (w *WrappingExecutionStore) GetCurrentExecution(request *persistence.GetCurrentExecutionRequest) (
	*persistence.InternalGetCurrentExecutionResponse,
	error,
) {
	return w.Delegate.GetCurrentExecution(request)
}

func (w *WrappingExecutionStore) ListConcreteExecutions(request *persistence.ListConcreteExecutionsRequest) (
	*persistence.InternalListConcreteExecutionsResponse,
	error,
) {
	return w.Delegate.ListConcreteExecutions(request)
}

func (w *WrappingExecutionStore) AddTasks(request *persistence.InternalAddTasksRequest) error {
	return w.Delegate.AddTasks(request)
}

func (w *WrappingExecutionStore) GetTransferTask(request *persistence.GetTransferTaskRequest) (
	*persistence.InternalGetTransferTaskResponse,
	error,
) {
	return w.Delegate.GetTransferTask(request)
}

func (w *WrappingExecutionStore) GetTransferTasks(request *persistence.GetTransferTasksRequest) (
	*persistence.InternalGetTransferTasksResponse,
	error,
) {
	return w.Delegate.GetTransferTasks(request)
}

func (w *WrappingExecutionStore) CompleteTransferTask(request *persistence.CompleteTransferTaskRequest) error {
	return w.Delegate.CompleteTransferTask(request)
}

func (w *WrappingExecutionStore) RangeCompleteTransferTask(request *persistence.RangeCompleteTransferTaskRequest) error {
	return w.Delegate.RangeCompleteTransferTask(request)
}

func (w *WrappingExecutionStore) GetTimerTask(request *persistence.GetTimerTaskRequest) (
	*persistence.InternalGetTimerTaskResponse,
	error,
) {
	return w.Delegate.GetTimerTask(request)
}

func (w *WrappingExecutionStore) GetTimerTasks(request *persistence.GetTimerTasksRequest) (
	*persistence.InternalGetTimerTasksResponse,
	error,
) {
	return w.Delegate.GetTimerTasks(request)
}

func (w *WrappingExecutionStore) CompleteTimerTask(request *persistence.CompleteTimerTaskRequest) error {
	return w.Delegate.CompleteTimerTask(request)
}

func (w *WrappingExecutionStore) RangeCompleteTimerTask(request *persistence.RangeCompleteTimerTaskRequest) error {
	return w.Delegate.RangeCompleteTimerTask(request)
}

func (w *WrappingExecutionStore) GetReplicationTask(request *persistence.GetReplicationTaskRequest) (
	*persistence.InternalGetReplicationTaskResponse,
	error,
) {
	return w.Delegate.GetReplicationTask(request)
}

func (w *WrappingExecutionStore) GetReplicationTasks(request *persistence.GetReplicationTasksRequest) (
	*persistence.InternalGetReplicationTasksResponse,
	error,
) {
	return w.Delegate.GetReplicationTasks(request)
}

func (w *WrappingExecutionStore) CompleteReplicationTask(request *persistence.CompleteReplicationTaskRequest) error {
	return w.Delegate.CompleteReplicationTask(request)
}

func (w *WrappingExecutionStore) RangeCompleteReplicationTask(request *persistence.RangeCompleteReplicationTaskRequest) error {
	return w.Delegate.RangeCompleteReplicationTask(request)
}

func (w *WrappingExecutionStore) PutReplicationTaskToDLQ(request *persistence.PutReplicationTaskToDLQRequest) error {
	return w.Delegate.PutReplicationTaskToDLQ(request)
}

func (w *WrappingExecutionStore) GetReplicationTasksFromDLQ(request *persistence.GetReplicationTasksFromDLQRequest) (
	*persistence.InternalGetReplicationTasksFromDLQResponse,
	error,
) {
	return w.Delegate.GetReplicationTasksFromDLQ(request)
}

func (w *WrappingExecutionStore) DeleteReplicationTaskFromDLQ(request *persistence.DeleteReplicationTaskFromDLQRequest) error {
	return w.Delegate.DeleteReplicationTaskFromDLQ(request)
}

func (w *WrappingExecutionStore) RangeDeleteReplicationTaskFromDLQ(request *persistence.RangeDeleteReplicationTaskFromDLQRequest) error {
	return w.Delegate.RangeDeleteReplicationTaskFromDLQ(request)
}

func (w *WrappingExecutionStore) GetVisibilityTask(request *persistence.GetVisibilityTaskRequest) (
	*persistence.InternalGetVisibilityTaskResponse,
	error,
) {
	return w.Delegate.GetVisibilityTask(request)
}

func (w *WrappingExecutionStore) GetVisibilityTasks(request *persistence.GetVisibilityTasksRequest) (
	*persistence.InternalGetVisibilityTasksResponse,
	error,
) {
	return w.Delegate.GetVisibilityTasks(request)
}

func (w *WrappingExecutionStore) CompleteVisibilityTask(request *persistence.CompleteVisibilityTaskRequest) error {
	return w.Delegate.CompleteVisibilityTask(request)
}

func (w *WrappingExecutionStore) RangeCompleteVisibilityTask(request *persistence.RangeCompleteVisibilityTaskRequest) error {
	return w.Delegate.RangeCompleteVisibilityTask(request)
}

func (w *WrappingExecutionStore) GetTieredStorageTask(request *persistence.GetTieredStorageTaskRequest) (
	*persistence.InternalGetTieredStorageTaskResponse,
	error,
) {
	return w.Delegate.GetTieredStorageTask(request)
}

func (w *WrappingExecutionStore) GetTieredStorageTasks(request *persistence.GetTieredStorageTasksRequest) (
	*persistence.InternalGetTieredStorageTasksResponse,
	error,
) {
	return w.Delegate.GetTieredStorageTasks(request)
}

func (w *WrappingExecutionStore) CompleteTieredStorageTask(request *persistence.CompleteTieredStorageTaskRequest) error {
	return w.Delegate.CompleteTieredStorageTask(request)
}

func (w *WrappingExecutionStore) RangeCompleteTieredStorageTask(request *persistence.RangeCompleteTieredStorageTaskRequest) error {
	return w.Delegate.RangeCompleteTieredStorageTask(request)
}

func (w *WrappingExecutionStore) AppendHistoryNodes(request *persistence.InternalAppendHistoryNodesRequest) error {
	return w.Delegate.AppendHistoryNodes(request)
}

func (w *WrappingExecutionStore) DeleteHistoryNodes(request *persistence.InternalDeleteHistoryNodesRequest) error {
	return w.Delegate.DeleteHistoryNodes(request)
}

func (w *WrappingExecutionStore) ReadHistoryBranch(request *persistence.InternalReadHistoryBranchRequest) (
	*persistence.InternalReadHistoryBranchResponse,
	error,
) {
	return w.Delegate.ReadHistoryBranch(request)
}

func (w *WrappingExecutionStore) ForkHistoryBranch(request *persistence.InternalForkHistoryBranchRequest) error {
	return w.Delegate.ForkHistoryBranch(request)
}

func (w *WrappingExecutionStore) DeleteHistoryBranch(request *persistence.InternalDeleteHistoryBranchRequest) error {
	return w.Delegate.DeleteHistoryBranch(request)
}

func (w *WrappingExecutionStore) GetHistoryTree(request *persistence.GetHistoryTreeRequest) (
	*persistence.InternalGetHistoryTreeResponse,
	error,
) {
	return w.Delegate.GetHistoryTree(request)
}

func (w *WrappingExecutionStore) GetAllHistoryTreeBranches(request *persistence.GetAllHistoryTreeBranchesRequest) (
	*persistence.InternalGetAllHistoryTreeBranchesResponse,
	error,
) {
	return w.Delegate.GetAllHistoryTreeBranches(request)
}
