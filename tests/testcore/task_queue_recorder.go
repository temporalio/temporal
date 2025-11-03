package testcore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// TaskQueueRecorder wraps an ExecutionManager to record ALL task writes
// to the history task queues (transfer, timer, replication, visibility, archival, etc.).
// This is useful for integration tests where you want to assert on what tasks
// were generated and in what order.
type TaskQueueRecorder struct {
	mu             sync.RWMutex
	capturedWrites []CapturedTaskWrite
	delegate       persistence.ExecutionManager
}

// CapturedTaskWrite represents a single task write operation with all its metadata
type CapturedTaskWrite struct {
	Timestamp   time.Time                       `json:"timestamp"`
	ShardID     int32                           `json:"shardId"`
	RangeID     int64                           `json:"rangeId"`
	NamespaceID string                          `json:"namespaceId"`
	WorkflowID  string                          `json:"workflowId"`
	Tasks       map[tasks.Category][]tasks.Task `json:"tasks"` // Full task objects - they serialize to JSON fine
	Error       error                           `json:"-"`
}

// NewTaskQueueRecorder creates a recorder that wraps the given ExecutionManager
func NewTaskQueueRecorder(delegate persistence.ExecutionManager) *TaskQueueRecorder {
	return &TaskQueueRecorder{
		capturedWrites: make([]CapturedTaskWrite, 0),
		delegate:       delegate,
	}
}

// AddHistoryTasks records the task write and then delegates to the underlying manager
func (r *TaskQueueRecorder) AddHistoryTasks(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
) error {
	// Record the tasks before calling delegate
	r.recordTasks(request.ShardID, 0, request.NamespaceID, request.WorkflowID, request.Tasks)

	// Call the delegate
	err := r.delegate.AddHistoryTasks(ctx, request)
	return err
}

func (r *TaskQueueRecorder) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	// Record tasks from the mutation
	r.recordTasks(
		request.ShardID,
		request.RangeID,
		request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId,
		request.UpdateWorkflowMutation.ExecutionInfo.WorkflowId,
		request.UpdateWorkflowMutation.Tasks,
	)

	// Record tasks from new workflow snapshot if present
	if request.NewWorkflowSnapshot != nil {
		r.recordTasks(
			request.ShardID,
			request.RangeID,
			request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId,
			request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId,
			request.NewWorkflowSnapshot.Tasks,
		)
	}

	// Call the delegate
	return r.delegate.UpdateWorkflowExecution(ctx, request)
}

func (r *TaskQueueRecorder) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	// Record tasks from the new workflow snapshot
	r.recordTasks(
		request.ShardID,
		request.RangeID,
		request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId,
		request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId,
		request.NewWorkflowSnapshot.Tasks,
	)

	// Call the delegate
	return r.delegate.CreateWorkflowExecution(ctx, request)
}

// recordTasks is a helper to capture tasks from any source
func (r *TaskQueueRecorder) recordTasks(
	shardID int32,
	rangeID int64,
	namespaceID string,
	workflowID string,
	tasksMap map[tasks.Category][]tasks.Task,
) {
	if len(tasksMap) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Create a deep copy of tasks to preserve their state at the time of writing
	tasksCopy := make(map[tasks.Category][]tasks.Task)

	for category, taskList := range tasksMap {
		copiedTasks := make([]tasks.Task, len(taskList))
		for i, task := range taskList {
			copiedTasks[i] = task
		}
		tasksCopy[category] = copiedTasks
	}

	// Record the write
	captured := CapturedTaskWrite{
		Timestamp:   time.Now(),
		ShardID:     shardID,
		RangeID:     rangeID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		Tasks:       tasksCopy,
		Error:       nil,
	}

	r.capturedWrites = append(r.capturedWrites, captured)
}

// GetCapturedWrites returns all captured task writes
func (r *TaskQueueRecorder) GetCapturedWrites() []CapturedTaskWrite {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]CapturedTaskWrite, len(r.capturedWrites))
	copy(result, r.capturedWrites)
	return result
}

// GetTransferTasks returns all transfer tasks across all writes
func (r *TaskQueueRecorder) GetTransferTasks() []tasks.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getTasksByCategory(tasks.CategoryTransfer)
}

// GetTimerTasks returns all timer tasks across all writes
func (r *TaskQueueRecorder) GetTimerTasks() []tasks.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getTasksByCategory(tasks.CategoryTimer)
}

// GetReplicationTasks returns all replication tasks across all writes
func (r *TaskQueueRecorder) GetReplicationTasks() []tasks.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getTasksByCategory(tasks.CategoryReplication)
}

// GetVisibilityTasks returns all visibility tasks across all writes
func (r *TaskQueueRecorder) GetVisibilityTasks() []tasks.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getTasksByCategory(tasks.CategoryVisibility)
}

// GetTasksByCategory returns all tasks of a specific category
func (r *TaskQueueRecorder) GetTasksByCategory(category tasks.Category) []tasks.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getTasksByCategory(category)
}

// getTasksByCategory is the internal version without locking (caller must hold lock)
func (r *TaskQueueRecorder) getTasksByCategory(category tasks.Category) []tasks.Task {
	var result []tasks.Task
	for _, write := range r.capturedWrites {
		if taskList, ok := write.Tasks[category]; ok {
			result = append(result, taskList...)
		}
	}
	return result
}

// GetWriteCount returns the total number of AddHistoryTasks calls
func (r *TaskQueueRecorder) GetWriteCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.capturedWrites)
}

// Clear removes all captured writes
func (r *TaskQueueRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capturedWrites = make([]CapturedTaskWrite, 0)
}

// WriteToLog writes all captured task writes to a file in JSON format
func (r *TaskQueueRecorder) WriteToLog(filePath string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Marshal to pretty JSON
	jsonBytes, err := json.MarshalIndent(r.capturedWrites, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal captured writes: %w", err)
	}

	// Write to file (using os.WriteFile would be fine here)
	if err := writeFile(filePath, jsonBytes); err != nil {
		return fmt.Errorf("failed to write to file %s: %w", filePath, err)
	}

	return nil
}

// Helper function to write to file
func writeFile(filePath string, data []byte) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

// Delegate all other ExecutionManager methods to the underlying implementation
// These are pass-through methods that don't need recording

func (r *TaskQueueRecorder) GetName() string {
	return r.delegate.GetName()
}

func (r *TaskQueueRecorder) Close() {
	r.delegate.Close()
}

func (r *TaskQueueRecorder) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {
	return r.delegate.GetWorkflowExecution(ctx, request)
}

func (r *TaskQueueRecorder) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	return r.delegate.ConflictResolveWorkflowExecution(ctx, request)
}

func (r *TaskQueueRecorder) DeleteWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteWorkflowExecutionRequest,
) error {
	return r.delegate.DeleteWorkflowExecution(ctx, request)
}

func (r *TaskQueueRecorder) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	return r.delegate.DeleteCurrentWorkflowExecution(ctx, request)
}

func (r *TaskQueueRecorder) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {
	return r.delegate.GetCurrentExecution(ctx, request)
}

func (r *TaskQueueRecorder) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {
	return r.delegate.SetWorkflowExecution(ctx, request)
}

func (r *TaskQueueRecorder) ListConcreteExecutions(
	ctx context.Context,
	request *persistence.ListConcreteExecutionsRequest,
) (*persistence.ListConcreteExecutionsResponse, error) {
	return r.delegate.ListConcreteExecutions(ctx, request)
}

func (r *TaskQueueRecorder) GetHistoryTasks(
	ctx context.Context,
	request *persistence.GetHistoryTasksRequest,
) (*persistence.GetHistoryTasksResponse, error) {
	return r.delegate.GetHistoryTasks(ctx, request)
}

func (r *TaskQueueRecorder) CompleteHistoryTask(
	ctx context.Context,
	request *persistence.CompleteHistoryTaskRequest,
) error {
	return r.delegate.CompleteHistoryTask(ctx, request)
}

func (r *TaskQueueRecorder) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	return r.delegate.RangeCompleteHistoryTasks(ctx, request)
}

func (r *TaskQueueRecorder) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	return r.delegate.PutReplicationTaskToDLQ(ctx, request)
}

func (r *TaskQueueRecorder) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (*persistence.GetHistoryTasksResponse, error) {
	return r.delegate.GetReplicationTasksFromDLQ(ctx, request)
}

func (r *TaskQueueRecorder) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.DeleteReplicationTaskFromDLQRequest,
) error {
	return r.delegate.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (r *TaskQueueRecorder) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	return r.delegate.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (r *TaskQueueRecorder) IsReplicationDLQEmpty(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	return r.delegate.IsReplicationDLQEmpty(ctx, request)
}

func (r *TaskQueueRecorder) GetHistoryBranchUtil() persistence.HistoryBranchUtil {
	return r.delegate.GetHistoryBranchUtil()
}

func (r *TaskQueueRecorder) AppendHistoryNodes(
	ctx context.Context,
	request *persistence.AppendHistoryNodesRequest,
) (*persistence.AppendHistoryNodesResponse, error) {
	return r.delegate.AppendHistoryNodes(ctx, request)
}

func (r *TaskQueueRecorder) AppendRawHistoryNodes(
	ctx context.Context,
	request *persistence.AppendRawHistoryNodesRequest,
) (*persistence.AppendHistoryNodesResponse, error) {
	return r.delegate.AppendRawHistoryNodes(ctx, request)
}

func (r *TaskQueueRecorder) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	return r.delegate.ReadHistoryBranch(ctx, request)
}

func (r *TaskQueueRecorder) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchByBatchResponse, error) {
	return r.delegate.ReadHistoryBranchByBatch(ctx, request)
}

func (r *TaskQueueRecorder) ReadHistoryBranchReverse(
	ctx context.Context,
	request *persistence.ReadHistoryBranchReverseRequest,
) (*persistence.ReadHistoryBranchReverseResponse, error) {
	return r.delegate.ReadHistoryBranchReverse(ctx, request)
}

func (r *TaskQueueRecorder) ReadRawHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadRawHistoryBranchResponse, error) {
	return r.delegate.ReadRawHistoryBranch(ctx, request)
}

func (r *TaskQueueRecorder) ForkHistoryBranch(
	ctx context.Context,
	request *persistence.ForkHistoryBranchRequest,
) (*persistence.ForkHistoryBranchResponse, error) {
	return r.delegate.ForkHistoryBranch(ctx, request)
}

func (r *TaskQueueRecorder) DeleteHistoryBranch(
	ctx context.Context,
	request *persistence.DeleteHistoryBranchRequest,
) error {
	return r.delegate.DeleteHistoryBranch(ctx, request)
}

func (r *TaskQueueRecorder) TrimHistoryBranch(
	ctx context.Context,
	request *persistence.TrimHistoryBranchRequest,
) (*persistence.TrimHistoryBranchResponse, error) {
	return r.delegate.TrimHistoryBranch(ctx, request)
}

func (r *TaskQueueRecorder) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *persistence.GetAllHistoryTreeBranchesRequest,
) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
	return r.delegate.GetAllHistoryTreeBranches(ctx, request)
}
