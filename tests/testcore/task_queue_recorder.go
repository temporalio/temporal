package testcore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// TaskQueueRecorder wraps an ExecutionManager to record ALL task writes
// to the history task queues (transfer, timer, replication, visibility, archival, etc.).
// This is useful for integration tests where you want to assert on what tasks
// were generated and in what order.
// Tasks are stored flattened by category - all tasks of the same type are in a single list,
// with each task wrapped with metadata about when/where it was written.
type TaskQueueRecorder struct {
	mu       sync.RWMutex
	tasks    map[tasks.Category][]RecordedTask // All tasks by category, in order
	delegate persistence.ExecutionManager
	logger   log.Logger
}

// RecordedTask wraps a task with metadata about when and where it was written
type RecordedTask struct {
	Timestamp   time.Time  `json:"timestamp"`
	TaskType    string     `json:"taskType"` // The specific task type (e.g., "TASK_TYPE_ACTIVITY_RETRY_TIMER")
	ShardID     int32      `json:"shardId"`
	RangeID     int64      `json:"rangeId,omitempty"`
	NamespaceID string     `json:"namespaceId"`
	WorkflowID  string     `json:"workflowId"`
	RunID       string     `json:"runId"`
	Task        tasks.Task `json:"task"` // The actual task object
}

// NewTaskQueueRecorder creates a recorder that wraps the given ExecutionManager
func NewTaskQueueRecorder(delegate persistence.ExecutionManager, logger log.Logger) *TaskQueueRecorder {
	return &TaskQueueRecorder{
		tasks:    make(map[tasks.Category][]RecordedTask),
		delegate: delegate,
		logger:   logger,
	}
}

// AddHistoryTasks records the task write and then delegates to the underlying manager
func (r *TaskQueueRecorder) AddHistoryTasks(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
) error {
	// Call the delegate first
	err := r.delegate.AddHistoryTasks(ctx, request)

	// Only record if successful
	if err == nil {
		r.recordTasks(request.ShardID, 0, request.NamespaceID, request.WorkflowID, request.Tasks)
	}

	return err
}

func (r *TaskQueueRecorder) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	// Call the delegate first
	resp, err := r.delegate.UpdateWorkflowExecution(ctx, request)

	// Only record if successful
	if err == nil {
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
	}

	return resp, err
}

func (r *TaskQueueRecorder) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	// Call the delegate first
	resp, err := r.delegate.CreateWorkflowExecution(ctx, request)

	// Only record if successful
	if err == nil {
		r.recordTasks(
			request.ShardID,
			request.RangeID,
			request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId,
			request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId,
			request.NewWorkflowSnapshot.Tasks,
		)
	}

	return resp, err
}

// recordTasks appends tasks to the flattened list by category, wrapping each with metadata
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

	timestamp := time.Now()

	// Append tasks to their respective category lists, wrapped with metadata
	for category, taskList := range tasksMap {
		for _, task := range taskList {
			recorded := RecordedTask{
				Timestamp:   timestamp,
				ShardID:     shardID,
				RangeID:     rangeID,
				NamespaceID: namespaceID,
				WorkflowID:  workflowID,
				RunID:       task.GetRunID(),
				TaskType:    task.GetType().String(),
				Task:        task,
			}
			r.tasks[category] = append(r.tasks[category], recorded)
		}
	}
}

// GetAllTasks returns all tasks grouped by category (unwrapped, without metadata)
func (r *TaskQueueRecorder) GetAllTasks() map[tasks.Category][]tasks.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[tasks.Category][]tasks.Task)
	for category, recordedList := range r.tasks {
		taskList := make([]tasks.Task, len(recordedList))
		for i, recorded := range recordedList {
			taskList[i] = recorded.Task
		}
		result[category] = taskList
	}
	return result
}

// GetAllRecordedTasks returns all recorded tasks WITH metadata, grouped by category
func (r *TaskQueueRecorder) GetAllRecordedTasks() map[tasks.Category][]RecordedTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a deep copy
	result := make(map[tasks.Category][]RecordedTask)
	for category, recordedList := range r.tasks {
		copiedList := make([]RecordedTask, len(recordedList))
		copy(copiedList, recordedList)
		result[category] = copiedList
	}
	return result
}

// TaskMatcher is a function that tests whether a RecordedTask matches some criteria
type TaskMatcher func(RecordedTask) bool

// MatchTasks returns all tasks in a category that match the given matcher function
func (r *TaskQueueRecorder) MatchTasks(category tasks.Category, matcher TaskMatcher) []RecordedTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	recordedList, ok := r.tasks[category]
	if !ok {
		return nil
	}

	var matched []RecordedTask
	for _, recorded := range recordedList {
		if matcher(recorded) {
			matched = append(matched, recorded)
		}
	}
	return matched
}

// CountMatchingTasks returns the count of tasks in a category that match the given matcher
func (r *TaskQueueRecorder) CountMatchingTasks(category tasks.Category, matcher TaskMatcher) int {
	return len(r.MatchTasks(category, matcher))
}

// TaskFilter specifies criteria for filtering recorded tasks
type TaskFilter struct {
	NamespaceID string // Required: namespace ID to filter by
	WorkflowID  string // Optional: workflow ID to filter by (empty string means no filter)
	RunID       string // Optional: run ID to filter by (empty string means no filter)
}

// GetRecordedTasksByCategoryFiltered returns recorded tasks WITH metadata for a specific category,
// filtered by namespace (required) and optionally by workflow ID and run ID.
// This is the preferred API for tests to ensure tasks are properly scoped.
func (r *TaskQueueRecorder) GetRecordedTasksByCategoryFiltered(category tasks.Category, filter TaskFilter) []RecordedTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if filter.NamespaceID == "" {
		r.logger.DPanic("TaskFilter.NamespaceID is required - use GetRecordedTasksByCategoryFiltered to prevent accidentally checking all tasks")
	}

	recordedList, ok := r.tasks[category]
	if !ok {
		return nil
	}

	var filtered []RecordedTask
	for _, recorded := range recordedList {
		// Namespace ID is required
		if recorded.NamespaceID != filter.NamespaceID {
			continue
		}

		// WorkflowID is optional - if specified, must match
		if filter.WorkflowID != "" && recorded.WorkflowID != filter.WorkflowID {
			continue
		}

		// RunID is optional - if specified, must match
		if filter.RunID != "" && recorded.RunID != filter.RunID {
			continue
		}

		filtered = append(filtered, recorded)
	}

	return filtered
}

// MatchTasksForWorkflow returns all tasks in a category for a specific workflow
// If namespaceID is empty, it matches any namespace
// If runID is empty, it matches any runID for the given workflowID
func (r *TaskQueueRecorder) MatchTasksForWorkflow(
	category tasks.Category,
	namespaceID string,
	workflowID string,
	runID string,
	matcher TaskMatcher,
) []RecordedTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	recordedList, ok := r.tasks[category]
	if !ok {
		return nil
	}

	var matched []RecordedTask
	for _, recorded := range recordedList {
		// Filter by namespaceID if provided
		if namespaceID != "" && recorded.NamespaceID != namespaceID {
			continue
		}
		// Filter by workflowID
		if recorded.WorkflowID != workflowID {
			continue
		}
		// Filter by runID if provided
		if runID != "" && recorded.RunID != runID {
			continue
		}
		// Apply additional matcher if provided
		if matcher != nil && !matcher(recorded) {
			continue
		}
		matched = append(matched, recorded)
	}
	return matched
}

// CountTasksForWorkflow returns the count of tasks in a category for a specific workflow
// If namespaceID is empty, it matches any namespace
// If runID is empty, it matches any runID for the given workflowID
func (r *TaskQueueRecorder) CountTasksForWorkflow(
	category tasks.Category,
	namespaceID string,
	workflowID string,
	runID string,
	matcher TaskMatcher,
) int {
	return len(r.MatchTasksForWorkflow(category, namespaceID, workflowID, runID, matcher))
}

// MatchTasksForNamespace returns all tasks in a category for a specific namespace
func (r *TaskQueueRecorder) MatchTasksForNamespace(
	category tasks.Category,
	namespaceID string,
	matcher TaskMatcher,
) []RecordedTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	recordedList, ok := r.tasks[category]
	if !ok {
		return nil
	}

	var matched []RecordedTask
	for _, recorded := range recordedList {
		// Filter by namespaceID
		if recorded.NamespaceID != namespaceID {
			continue
		}
		// Apply additional matcher if provided
		if matcher != nil && !matcher(recorded) {
			continue
		}
		matched = append(matched, recorded)
	}
	return matched
}

// CountTasksForNamespace returns the count of tasks in a category for a specific namespace
func (r *TaskQueueRecorder) CountTasksForNamespace(
	category tasks.Category,
	namespaceID string,
	matcher TaskMatcher,
) int {
	return len(r.MatchTasksForNamespace(category, namespaceID, matcher))
}

// WriteToLog writes all captured tasks to a file in JSON format
func (r *TaskQueueRecorder) WriteToLog(filePath string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Marshal to pretty JSON
	jsonBytes, err := json.MarshalIndent(r.tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal captured tasks: %w", err)
	}

	// Write to file
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
	defer func() {
		_ = file.Close()
	}()

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
