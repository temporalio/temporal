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
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/service/history/tasks"
)

// HistoryTaskRecorder wraps an ExecutionManager to record ALL task writes
// to the history task queues (transfer, timer, replication, visibility, archival, etc.).
// This is useful for integration tests where you want to assert on what tasks
// were generated and in what order.
// Tasks are stored flattened by category - all tasks of the same type are in a single list,
// with each task wrapped with metadata about when/where it was written.
type HistoryTaskRecorder struct {
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

// NewHistoryTaskRecorder creates a recorder that wraps the given ExecutionManager
func NewHistoryTaskRecorder(delegate persistence.ExecutionManager, logger log.Logger) *HistoryTaskRecorder {
	return &HistoryTaskRecorder{
		tasks:    make(map[tasks.Category][]RecordedTask),
		delegate: delegate,
		logger:   logger,
	}
}

type historyTaskRecordingPersistenceFactory struct {
	persistenceclient.Factory
	logger      log.Logger
	setRecorder func(*HistoryTaskRecorder)
}

func (f *historyTaskRecordingPersistenceFactory) NewExecutionManager() (persistence.ExecutionManager, error) {
	manager, err := f.Factory.NewExecutionManager()
	if err != nil {
		return nil, err
	}
	recorder := NewHistoryTaskRecorder(manager, f.logger)
	f.setRecorder(recorder)
	return recorder, nil
}

// AddHistoryTasks records the task write and then delegates to the underlying manager
func (r *HistoryTaskRecorder) AddHistoryTasks(
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

func (r *HistoryTaskRecorder) UpdateWorkflowExecution(
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

func (r *HistoryTaskRecorder) CreateWorkflowExecution(
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
func (r *HistoryTaskRecorder) recordTasks(
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
func (r *HistoryTaskRecorder) GetAllTasks() map[tasks.Category][]tasks.Task {
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
func (r *HistoryTaskRecorder) GetAllRecordedTasks() map[tasks.Category][]RecordedTask {
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
func (r *HistoryTaskRecorder) MatchTasks(category tasks.Category, matcher TaskMatcher) []RecordedTask {
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
func (r *HistoryTaskRecorder) CountMatchingTasks(category tasks.Category, matcher TaskMatcher) int {
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
func (r *HistoryTaskRecorder) GetRecordedTasksByCategoryFiltered(category tasks.Category, filter TaskFilter) []RecordedTask {
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
func (r *HistoryTaskRecorder) MatchTasksForWorkflow(
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
func (r *HistoryTaskRecorder) CountTasksForWorkflow(
	category tasks.Category,
	namespaceID string,
	workflowID string,
	runID string,
	matcher TaskMatcher,
) int {
	return len(r.MatchTasksForWorkflow(category, namespaceID, workflowID, runID, matcher))
}

// MatchTasksForNamespace returns all tasks in a category for a specific namespace
func (r *HistoryTaskRecorder) MatchTasksForNamespace(
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
func (r *HistoryTaskRecorder) CountTasksForNamespace(
	category tasks.Category,
	namespaceID string,
	matcher TaskMatcher,
) int {
	return len(r.MatchTasksForNamespace(category, namespaceID, matcher))
}

// WriteToLog writes all captured tasks to a file in JSON format
func (r *HistoryTaskRecorder) WriteToLog(filePath string) error {
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

func (r *HistoryTaskRecorder) GetName() string {
	return r.delegate.GetName()
}

func (r *HistoryTaskRecorder) Close() {
	r.delegate.Close()
}

func (r *HistoryTaskRecorder) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {
	return r.delegate.GetWorkflowExecution(ctx, request)
}

func (r *HistoryTaskRecorder) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	resp, err := r.delegate.ConflictResolveWorkflowExecution(ctx, request)

	if err == nil {
		r.recordTasks(
			request.ShardID,
			request.RangeID,
			request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId,
			request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowId,
			request.ResetWorkflowSnapshot.Tasks,
		)

		if request.NewWorkflowSnapshot != nil {
			r.recordTasks(
				request.ShardID,
				request.RangeID,
				request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId,
				request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId,
				request.NewWorkflowSnapshot.Tasks,
			)
		}

		if request.CurrentWorkflowMutation != nil {
			r.recordTasks(
				request.ShardID,
				request.RangeID,
				request.CurrentWorkflowMutation.ExecutionInfo.NamespaceId,
				request.CurrentWorkflowMutation.ExecutionInfo.WorkflowId,
				request.CurrentWorkflowMutation.Tasks,
			)
		}
	}

	return resp, err
}

func (r *HistoryTaskRecorder) DeleteWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteWorkflowExecutionRequest,
) error {
	return r.delegate.DeleteWorkflowExecution(ctx, request)
}

func (r *HistoryTaskRecorder) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	return r.delegate.DeleteCurrentWorkflowExecution(ctx, request)
}

func (r *HistoryTaskRecorder) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {
	return r.delegate.GetCurrentExecution(ctx, request)
}

func (r *HistoryTaskRecorder) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {
	return r.delegate.SetWorkflowExecution(ctx, request)
}

func (r *HistoryTaskRecorder) ListConcreteExecutions(
	ctx context.Context,
	request *persistence.ListConcreteExecutionsRequest,
) (*persistence.ListConcreteExecutionsResponse, error) {
	return r.delegate.ListConcreteExecutions(ctx, request)
}

func (r *HistoryTaskRecorder) GetHistoryTasks(
	ctx context.Context,
	request *persistence.GetHistoryTasksRequest,
) (*persistence.GetHistoryTasksResponse, error) {
	return r.delegate.GetHistoryTasks(ctx, request)
}

func (r *HistoryTaskRecorder) CompleteHistoryTask(
	ctx context.Context,
	request *persistence.CompleteHistoryTaskRequest,
) error {
	return r.delegate.CompleteHistoryTask(ctx, request)
}

func (r *HistoryTaskRecorder) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	return r.delegate.RangeCompleteHistoryTasks(ctx, request)
}

func (r *HistoryTaskRecorder) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	return r.delegate.PutReplicationTaskToDLQ(ctx, request)
}

func (r *HistoryTaskRecorder) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (*persistence.GetHistoryTasksResponse, error) {
	return r.delegate.GetReplicationTasksFromDLQ(ctx, request)
}

func (r *HistoryTaskRecorder) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.DeleteReplicationTaskFromDLQRequest,
) error {
	return r.delegate.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (r *HistoryTaskRecorder) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	return r.delegate.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (r *HistoryTaskRecorder) IsReplicationDLQEmpty(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	return r.delegate.IsReplicationDLQEmpty(ctx, request)
}

func (r *HistoryTaskRecorder) GetHistoryBranchUtil() persistence.HistoryBranchUtil {
	return r.delegate.GetHistoryBranchUtil()
}

func (r *HistoryTaskRecorder) AppendHistoryNodes(
	ctx context.Context,
	request *persistence.AppendHistoryNodesRequest,
) (*persistence.AppendHistoryNodesResponse, error) {
	return r.delegate.AppendHistoryNodes(ctx, request)
}

func (r *HistoryTaskRecorder) AppendRawHistoryNodes(
	ctx context.Context,
	request *persistence.AppendRawHistoryNodesRequest,
) (*persistence.AppendHistoryNodesResponse, error) {
	return r.delegate.AppendRawHistoryNodes(ctx, request)
}

func (r *HistoryTaskRecorder) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	return r.delegate.ReadHistoryBranch(ctx, request)
}

func (r *HistoryTaskRecorder) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchByBatchResponse, error) {
	return r.delegate.ReadHistoryBranchByBatch(ctx, request)
}

func (r *HistoryTaskRecorder) ReadHistoryBranchReverse(
	ctx context.Context,
	request *persistence.ReadHistoryBranchReverseRequest,
) (*persistence.ReadHistoryBranchReverseResponse, error) {
	return r.delegate.ReadHistoryBranchReverse(ctx, request)
}

func (r *HistoryTaskRecorder) ReadRawHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadRawHistoryBranchResponse, error) {
	return r.delegate.ReadRawHistoryBranch(ctx, request)
}

func (r *HistoryTaskRecorder) ForkHistoryBranch(
	ctx context.Context,
	request *persistence.ForkHistoryBranchRequest,
) (*persistence.ForkHistoryBranchResponse, error) {
	return r.delegate.ForkHistoryBranch(ctx, request)
}

func (r *HistoryTaskRecorder) DeleteHistoryBranch(
	ctx context.Context,
	request *persistence.DeleteHistoryBranchRequest,
) error {
	return r.delegate.DeleteHistoryBranch(ctx, request)
}

func (r *HistoryTaskRecorder) TrimHistoryBranch(
	ctx context.Context,
	request *persistence.TrimHistoryBranchRequest,
) (*persistence.TrimHistoryBranchResponse, error) {
	return r.delegate.TrimHistoryBranch(ctx, request)
}

func (r *HistoryTaskRecorder) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *persistence.GetAllHistoryTreeBranchesRequest,
) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
	return r.delegate.GetAllHistoryTreeBranches(ctx, request)
}
