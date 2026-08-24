package roundtrip

// Task capture for the round trip.
//
// The passive side runs its real transaction manager, so the tasks the replication apply
// produced reach persistence like any other write. Wrapping the passive cluster's
// ExecutionManager and reading them off the write request means the diff compares what the
// passive cluster actually stored, and requires no test hook inside the ndc package.
//
// Only the three mutable-state writes carry tasks. Everything else on the very wide
// ExecutionManager interface is embedded and passes straight through, so this decorator does
// not need updating when that interface grows.

import (
	"context"
	"sync"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// rtCapturedTasks accumulates the tasks seen on a cluster's write path.
type rtCapturedTasks struct {
	mu    sync.Mutex
	tasks map[tasks.Category][]tasks.Task
}

func newRtCapturedTasks() *rtCapturedTasks {
	return &rtCapturedTasks{tasks: make(map[tasks.Category][]tasks.Task)}
}

func (c *rtCapturedTasks) add(in map[tasks.Category][]tasks.Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for category, categoryTasks := range in {
		c.tasks[category] = append(c.tasks[category], categoryTasks...)
	}
}

func (c *rtCapturedTasks) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tasks = make(map[tasks.Category][]tasks.Task)
}

func (c *rtCapturedTasks) drain() map[tasks.Category][]tasks.Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.tasks
	c.tasks = make(map[tasks.Category][]tasks.Task)
	return out
}

// rtCapturingExecutionManager records the tasks in every mutable-state write, then delegates.
type rtCapturingExecutionManager struct {
	persistence.ExecutionManager
	captured *rtCapturedTasks
}

func newRtCapturingExecutionManager(
	inner persistence.ExecutionManager,
	captured *rtCapturedTasks,
) persistence.ExecutionManager {
	return &rtCapturingExecutionManager{ExecutionManager: inner, captured: captured}
}

func (m *rtCapturingExecutionManager) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	m.captured.add(request.NewWorkflowSnapshot.Tasks)
	return m.ExecutionManager.CreateWorkflowExecution(ctx, request)
}

func (m *rtCapturingExecutionManager) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	m.captured.add(request.UpdateWorkflowMutation.Tasks)
	if request.NewWorkflowSnapshot != nil {
		m.captured.add(request.NewWorkflowSnapshot.Tasks)
	}
	return m.ExecutionManager.UpdateWorkflowExecution(ctx, request)
}

func (m *rtCapturingExecutionManager) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	m.captured.add(request.ResetWorkflowSnapshot.Tasks)
	if request.NewWorkflowSnapshot != nil {
		m.captured.add(request.NewWorkflowSnapshot.Tasks)
	}
	if request.CurrentWorkflowMutation != nil {
		m.captured.add(request.CurrentWorkflowMutation.Tasks)
	}
	return m.ExecutionManager.ConflictResolveWorkflowExecution(ctx, request)
}
