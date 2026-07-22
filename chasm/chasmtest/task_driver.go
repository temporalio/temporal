package chasmtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// TaskExecutionResult describes the logical work performed for one physical task.
type TaskExecutionResult struct {
	Executed int  `json:"executed"`
	Dropped  bool `json:"dropped"`
}

// DeliveryRef identifies one physical task delivery in a replayable trace.
// A reference remains valid after acknowledgement so tests can explicitly
// model duplicate redelivery. It is intentionally a test-host identity, not a
// History queue task key.
type DeliveryRef struct {
	ID             string    `json:"id"`
	NamespaceID    string    `json:"namespace_id"`
	BusinessID     string    `json:"business_id"`
	RunID          string    `json:"run_id"`
	CategoryID     int       `json:"category_id"`
	TaskID         int64     `json:"task_id"`
	VisibilityTime time.Time `json:"visibility_time"`
}

// DeliveryReceipt records the observable outcome of a delivery attempt.
type DeliveryReceipt struct {
	Ref        DeliveryRef         `json:"ref"`
	Result     TaskExecutionResult `json:"result"`
	Redelivery bool                `json:"redelivery"`
}

// DeliveryTrace is a JSON-serializable record of host choices. The host only
// records task delivery and reload choices; it does not model History queue
// acknowledgement, shards, or replication.
type DeliveryTrace struct {
	Version int                  `json:"version"`
	Events  []DeliveryTraceEvent `json:"events"`
}

// DeliveryTraceEvent is one replayable host action.
type DeliveryTraceEvent struct {
	Kind    string           `json:"kind"`
	Ref     *DeliveryRef     `json:"ref,omitempty"`
	Receipt *DeliveryReceipt `json:"receipt,omitempty"`
}

// MarshalJSON always writes a version so traces can evolve compatibly.
func (t DeliveryTrace) MarshalJSON() ([]byte, error) {
	if t.Version == 0 {
		t.Version = 1
	}
	type trace DeliveryTrace
	return json.Marshal(trace(t))
}

// RunnableDeliveries returns one due CHASM task from each non-visibility
// category. The category head is the only task eligible for a first delivery;
// categories can be selected in any order by the caller.
func (e *Engine) RunnableDeliveries(ref chasm.ComponentRef) ([]DeliveryRef, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return nil, err
	}
	return e.runnableDeliveries(exec), nil
}

func (e *Engine) runnableDeliveries(exec *execution) []DeliveryRef {
	var candidates []tasks.Task
	for category, categoryTasks := range exec.backend.TasksByCategory {
		if category == tasks.CategoryVisibility || len(categoryTasks) == 0 {
			continue
		}
		task := categoryTasks[0]
		if !isCHASMTask(task) || task.GetVisibilityTime().After(e.timeSource.Now()) {
			continue
		}
		candidates = append(candidates, task)
	}
	slices.SortFunc(candidates, func(a, b tasks.Task) int {
		if a.GetCategory().ID() < b.GetCategory().ID() {
			return -1
		}
		if a.GetCategory().ID() > b.GetCategory().ID() {
			return 1
		}
		return a.GetVisibilityTime().Compare(b.GetVisibilityTime())
	})
	deliveries := make([]DeliveryRef, len(candidates))
	for i, task := range candidates {
		deliveries[i] = e.deliveryRef(exec, task)
	}
	return deliveries
}

// Deliver executes a due category-head task and retains its physical identity
// for an explicit later Redeliver call. It does not permit incidental global
// FIFO ordering across categories.
func (e *Engine) Deliver(
	ctx context.Context,
	ref chasm.ComponentRef,
	delivery DeliveryRef,
) (DeliveryReceipt, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	task, err := e.pendingDeliveryTask(exec, delivery)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	result, err := e.executeTask(ctx, exec, task)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	e.acknowledgeTask(exec, task)
	exec.deliveredTasks[delivery.ID] = task
	return DeliveryReceipt{Ref: delivery, Result: result}, nil
}

// Redeliver executes a retained, previously delivered task. It is the only
// API that models duplicate delivery; new queue work must use Deliver.
func (e *Engine) Redeliver(
	ctx context.Context,
	ref chasm.ComponentRef,
	delivery DeliveryRef,
) (DeliveryReceipt, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	task, ok := exec.deliveredTasks[delivery.ID]
	if !ok {
		return DeliveryReceipt{}, fmt.Errorf("chasmtest: delivery %q was not previously delivered", delivery.ID)
	}
	if err := validateTaskExecution(exec, task, e.timeSource.Now()); err != nil {
		return DeliveryReceipt{}, err
	}
	result, err := e.executeTask(ctx, exec, task)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	return DeliveryReceipt{Ref: delivery, Result: result, Redelivery: true}, nil
}

// Reload records and performs a snapshot/reload action in a delivery trace.
func (e *Engine) Reload(ctx context.Context, ref chasm.ComponentRef, trace *DeliveryTrace) error {
	if err := e.ReloadExecution(ctx, ref); err != nil {
		return err
	}
	if trace != nil {
		trace.append(DeliveryTraceEvent{Kind: "reload"})
	}
	return nil
}

// RecordDelivery appends a successful delivery receipt to trace. Keeping trace
// recording explicit lets tests include commands and dependency outcomes in
// their own trace vocabulary.
func (t *DeliveryTrace) RecordDelivery(receipt DeliveryReceipt) {
	kind := "deliver"
	if receipt.Redelivery {
		kind = "redeliver"
	}
	t.append(DeliveryTraceEvent{Kind: kind, Ref: &receipt.Ref, Receipt: &receipt})
}

func (t *DeliveryTrace) append(event DeliveryTraceEvent) {
	if t.Version == 0 {
		t.Version = 1
	}
	t.Events = append(t.Events, event)
}

func (e *Engine) deliveryRef(exec *execution, task tasks.Task) DeliveryRef {
	for id, knownTask := range exec.deliveryRefs {
		if knownTask == task {
			return deliveryRefForTask(id, task)
		}
	}
	exec.nextDeliveryID++
	id := fmt.Sprintf("delivery-%d", exec.nextDeliveryID)
	exec.deliveryRefs[id] = task
	return deliveryRefForTask(id, task)
}

func deliveryRefForTask(id string, task tasks.Task) DeliveryRef {
	return DeliveryRef{
		ID:             id,
		NamespaceID:    task.GetNamespaceID(),
		BusinessID:     task.GetWorkflowID(),
		RunID:          task.GetRunID(),
		CategoryID:     task.GetCategory().ID(),
		TaskID:         task.GetTaskID(),
		VisibilityTime: task.GetVisibilityTime(),
	}
}

func (e *Engine) pendingDeliveryTask(exec *execution, delivery DeliveryRef) (tasks.Task, error) {
	// Re-register current heads so a decoded trace can be replayed without the
	// caller first asking for RunnableDeliveries.
	e.runnableDeliveries(exec)
	target, ok := exec.deliveryRefs[delivery.ID]
	if !ok {
		return nil, fmt.Errorf("chasmtest: unknown delivery %q", delivery.ID)
	}
	for category, categoryTasks := range exec.backend.TasksByCategory {
		if category.ID() != delivery.CategoryID || len(categoryTasks) == 0 {
			continue
		}
		task := categoryTasks[0]
		if task != target || !isCHASMTask(task) {
			continue
		}
		if err := validateTaskExecution(exec, task, e.timeSource.Now()); err != nil {
			return nil, err
		}
		return task, nil
	}
	return nil, fmt.Errorf("chasmtest: delivery %q is not a due category-head task", delivery.ID)
}

func isCHASMTask(task tasks.Task) bool {
	switch task.(type) {
	case *tasks.ChasmTask, *tasks.ChasmTaskPure:
		return true
	default:
		return false
	}
}

type runnableTask struct {
	task      tasks.Task
	category  tasks.Category
	insertion int
}

// ReloadExecution reconstructs an execution's component tree from a cloned
// persistence snapshot while retaining the execution and its physical tasks.
func (e *Engine) ReloadExecution(ctx context.Context, ref chasm.ComponentRef) error {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return err
	}
	node, root, err := e.cloneExecutionTree(ctx, exec)
	if err != nil {
		return err
	}
	exec.node = node
	exec.root = root
	return nil
}

// RunnableTasks returns the undelivered CHASM tasks for ref that are due at the
// engine's current time. Visibility maintenance tasks are excluded.
func (e *Engine) RunnableTasks(ref chasm.ComponentRef) ([]tasks.Task, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return nil, err
	}

	now := e.timeSource.Now()
	var runnable []runnableTask
	for category, categoryTasks := range exec.backend.TasksByCategory {
		if category == tasks.CategoryVisibility {
			continue
		}
		for insertion, task := range categoryTasks {
			switch task.(type) {
			case *tasks.ChasmTask, *tasks.ChasmTaskPure:
			default:
				continue
			}
			if task.GetVisibilityTime().After(now) {
				continue
			}
			runnable = append(runnable, runnableTask{
				task:      task,
				category:  category,
				insertion: insertion,
			})
		}
	}

	slices.SortStableFunc(runnable, func(a, b runnableTask) int {
		if order := a.task.GetVisibilityTime().Compare(b.task.GetVisibilityTime()); order != 0 {
			return order
		}
		if a.category.ID() < b.category.ID() {
			return -1
		}
		if a.category.ID() > b.category.ID() {
			return 1
		}
		return a.insertion - b.insertion
	})

	result := make([]tasks.Task, len(runnable))
	for i, item := range runnable {
		result[i] = item.task
	}
	return result, nil
}

// ExecuteTask executes one CHASM physical task. Successful and stale tasks are
// acknowledged; errors leave a queued task pending for retry.
func (e *Engine) ExecuteTask(
	ctx context.Context,
	ref chasm.ComponentRef,
	task tasks.Task,
) (TaskExecutionResult, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	if err := validateTaskExecution(exec, task, e.timeSource.Now()); err != nil {
		return TaskExecutionResult{}, err
	}
	result, err := e.executeTask(ctx, exec, task)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	e.acknowledgeTask(exec, task)
	return result, nil
}

func (e *Engine) executeTask(
	ctx context.Context,
	exec *execution,
	task tasks.Task,
) (TaskExecutionResult, error) {
	var result TaskExecutionResult
	var err error
	switch task := task.(type) {
	case *tasks.ChasmTaskPure:
		result, err = e.executePureTask(ctx, exec, task)
	case *tasks.ChasmTask:
		result, err = e.executeSideEffectTask(ctx, exec, task)
	default:
		err = fmt.Errorf("chasmtest: unsupported task type %T", task)
	}
	if err != nil {
		return TaskExecutionResult{}, err
	}
	return result, nil
}

// DrainTasks executes ready tasks until none remain. It returns the number of
// physical tasks acknowledged.
func (e *Engine) DrainTasks(
	ctx context.Context,
	ref chasm.ComponentRef,
	maxTasks int,
) (int, error) {
	if maxTasks < 0 {
		return 0, fmt.Errorf("chasmtest: maxTasks must be non-negative: %d", maxTasks)
	}

	drained := 0
	for drained < maxTasks {
		runnable, err := e.RunnableTasks(ref)
		if err != nil {
			return drained, err
		}
		if len(runnable) == 0 {
			return drained, nil
		}
		if _, err := e.ExecuteTask(ctx, ref, runnable[0]); err != nil {
			return drained, err
		}
		drained++
	}

	runnable, err := e.RunnableTasks(ref)
	if err != nil {
		return drained, err
	}
	if len(runnable) == 0 {
		return drained, nil
	}
	return drained, fmt.Errorf(
		"chasmtest: task drain limit %d reached with %d runnable tasks remaining (next: %T, category: %s, visibility time: %s)",
		maxTasks,
		len(runnable),
		runnable[0],
		runnable[0].GetCategory().Name(),
		runnable[0].GetVisibilityTime().Format(time.RFC3339Nano),
	)
}

func (e *Engine) executePureTask(
	ctx context.Context,
	exec *execution,
	task *tasks.ChasmTaskPure,
) (TaskExecutionResult, error) {
	if task.ArchetypeID != uint32(exec.node.ArchetypeID()) {
		return TaskExecutionResult{Dropped: true}, nil
	}

	workingNode, workingRoot, err := e.cloneExecutionTree(ctx, exec)
	if err != nil {
		return TaskExecutionResult{}, err
	}

	result := TaskExecutionResult{}
	processed := 0
	err = workingNode.EachPureTask(e.timeSource.Now(), func(
		handler chasm.NodePureTask,
		attrs chasm.TaskAttributes,
		logicalTask any,
	) (bool, error) {
		processed++
		executed, err := handler.ExecutePureTask(ctx, attrs, logicalTask)
		if executed {
			result.Executed++
		}
		return executed, err
	})
	if err != nil {
		return TaskExecutionResult{}, err
	}
	if processed == 0 {
		return TaskExecutionResult{Dropped: true}, nil
	}
	if err = e.closeTransaction(exec, workingNode); err != nil {
		return TaskExecutionResult{}, err
	}

	exec.node = workingNode
	exec.root = workingRoot
	return result, nil
}

func (e *Engine) executeSideEffectTask(
	ctx context.Context,
	exec *execution,
	task *tasks.ChasmTask,
) (TaskExecutionResult, error) {
	if task.Info == nil {
		return TaskExecutionResult{}, errors.New("chasmtest: CHASM side-effect task has no task info")
	}
	if task.Info.ArchetypeId != uint32(exec.node.ArchetypeID()) {
		return TaskExecutionResult{Dropped: true}, nil
	}

	inTree, valid, err := exec.node.ValidateSideEffectTask(ctx, task)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	if !inTree || !valid {
		return TaskExecutionResult{Dropped: true}, nil
	}

	engineCtx := chasm.NewEngineContext(ctx, e)
	err = exec.node.ExecuteSideEffectTask(
		engineCtx,
		exec.key,
		task,
		func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil },
	)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	return TaskExecutionResult{Executed: 1}, nil
}

func (e *Engine) cloneExecutionTree(
	ctx context.Context,
	exec *execution,
) (*chasm.Node, chasm.RootComponent, error) {
	snapshot := exec.node.Snapshot(nil)
	nodes := make(map[string]*persistencespb.ChasmNode, len(snapshot.Nodes))
	for path, node := range snapshot.Nodes {
		nodes[path] = proto.CloneOf(node)
	}

	cloned, err := chasm.NewTreeFromDB(
		nodes,
		e.registry,
		e.timeSource,
		exec.backend,
		chasm.DefaultPathEncoder,
		e.logger,
		e.metrics,
	)
	if err != nil {
		return nil, nil, err
	}
	component, err := cloned.Component(chasm.NewContext(ctx, cloned), chasm.ComponentRef{})
	if err != nil {
		return nil, nil, err
	}
	root, ok := component.(chasm.RootComponent)
	if !ok {
		return nil, nil, fmt.Errorf("chasmtest: execution root has unexpected type %T", component)
	}
	return cloned, root, nil
}

func validateTaskExecution(exec *execution, task tasks.Task, now time.Time) error {
	if task == nil {
		return errors.New("chasmtest: task is nil")
	}
	if task.GetCategory() == tasks.CategoryVisibility {
		return errors.New("chasmtest: visibility maintenance tasks are not executable")
	}
	if task.GetNamespaceID() != exec.key.NamespaceID ||
		task.GetWorkflowID() != exec.key.BusinessID ||
		task.GetRunID() != exec.key.RunID {
		return fmt.Errorf(
			"chasmtest: task execution key (%q, %q, %q) does not match ref execution key (%q, %q, %q)",
			task.GetNamespaceID(),
			task.GetWorkflowID(),
			task.GetRunID(),
			exec.key.NamespaceID,
			exec.key.BusinessID,
			exec.key.RunID,
		)
	}
	if task.GetVisibilityTime().After(now) {
		return fmt.Errorf(
			"chasmtest: task is not runnable until %s (current time: %s)",
			task.GetVisibilityTime().Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano),
		)
	}
	return nil
}

func (e *Engine) acknowledgeTask(exec *execution, target tasks.Task) {
	categoryTasks := exec.backend.TasksByCategory[target.GetCategory()]
	for i, task := range categoryTasks {
		if task == target {
			exec.backend.TasksByCategory[target.GetCategory()] = slices.Delete(categoryTasks, i, i+1)
			return
		}
	}
}
