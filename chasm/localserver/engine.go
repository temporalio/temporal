package localserver

import (
	"context"
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// engine is a single-threaded in-memory chasm.Engine. After each transaction it runs the side-effect
// tasks the transaction produced; pure tasks run when the caller advances time.
type engine struct {
	registry   *chasm.Registry
	logger     log.Logger
	now        time.Time
	executions map[chasm.ExecutionKey]*execution
	// current maps (namespace, business ID) to the latest run.
	current map[[2]string]*execution
}

type execution struct {
	key     chasm.ExecutionKey
	node    *chasm.Node
	backend *backend
	// nodes is the serialized tree as of the last transaction. As the server reloads mutable
	// state when an update fails, the tree is rebuilt from it.
	nodes map[string]*persistencespb.ChasmNode
}

var _ chasm.Engine = (*engine)(nil)

func newEngine(registry *chasm.Registry, now time.Time) *engine {
	return &engine{
		registry:   registry,
		logger:     log.NewNoopLogger(),
		now:        now,
		executions: map[chasm.ExecutionKey]*execution{},
		current:    map[[2]string]*execution{},
	}
}

func (e *engine) StartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	_ ...chasm.TransitionOption,
) (chasm.StartExecutionResult, error) {
	if x, ok := e.current[[2]string{ref.NamespaceID, ref.BusinessID}]; ok && x.backend.isRunning() {
		return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr("execution already started", "", x.key.RunID)
	}
	x := e.newExecution(ref.ExecutionKey)
	mutableCtx := chasm.NewMutableContext(ctx, x.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if err := x.node.SetRootComponent(root); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if err := e.closeTransaction(ctx, x); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	e.executions[x.key] = x
	e.current[[2]string{x.key.NamespaceID, x.key.BusinessID}] = x
	serializedRef, err := x.node.Ref(root)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if err := e.runSideEffectTasks(ctx, x); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	return chasm.StartExecutionResult{ExecutionKey: x.key, ExecutionRef: serializedRef, Created: true}, nil
}

func (e *engine) UpdateWithStartExecution(
	context.Context,
	chasm.ComponentRef,
	func(chasm.MutableContext) (chasm.RootComponent, error),
	func(chasm.MutableContext, chasm.Component) error,
	...chasm.TransitionOption,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	return chasm.EngineUpdateWithStartExecutionResult{}, serviceerror.NewUnimplemented("UpdateWithStartExecution")
}

func (e *engine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	_ ...chasm.TransitionOption,
) ([]byte, error) {
	x, err := e.execution(ref)
	if err != nil {
		return nil, err
	}
	mutableCtx := chasm.NewMutableContext(ctx, x.node)
	component, err := x.node.Component(mutableCtx, ref)
	if err != nil {
		return nil, err
	}
	if err := updateFn(mutableCtx, component); err != nil {
		if discardErr := e.discardTransaction(x); discardErr != nil {
			return nil, discardErr
		}
		return nil, err
	}
	if err := e.closeTransaction(ctx, x); err != nil {
		return nil, err
	}
	serializedRef, err := mutableCtx.Ref(component)
	if _, deleted := err.(*serviceerror.NotFound); deleted {
		// As in the server: the update deleted the component, so there is no new ref.
		return nil, e.runSideEffectTasks(ctx, x)
	}
	if err != nil {
		return nil, err
	}
	return serializedRef, e.runSideEffectTasks(ctx, x)
}

func (e *engine) ReadComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	readFn func(chasm.Context, chasm.Component) error,
	_ ...chasm.TransitionOption,
) error {
	x, err := e.execution(ref)
	if err != nil {
		return err
	}
	chasmCtx := chasm.NewContext(ctx, x.node)
	component, err := x.node.Component(chasmCtx, ref)
	if err != nil {
		return err
	}
	return readFn(chasmCtx, component)
}

func (e *engine) PollComponent(
	context.Context,
	chasm.ComponentRef,
	func(chasm.Context, chasm.Component) (bool, error),
	...chasm.TransitionOption,
) ([]byte, error) {
	return nil, serviceerror.NewUnimplemented("PollComponent")
}

func (e *engine) DeleteExecution(context.Context, chasm.ComponentRef, chasm.DeleteExecutionRequest) error {
	return serviceerror.NewUnimplemented("DeleteExecution")
}

func (e *engine) NotifyExecution(chasm.ExecutionKey) {}

// advanceTime sets the clock and runs the pure and side-effect tasks that are due.
func (e *engine) advanceTime(ctx context.Context, now time.Time) error {
	e.now = now
	for _, x := range e.executions {
		if !x.backend.hasDueTask(now) {
			continue
		}
		if err := e.runSideEffectTasks(ctx, x); err != nil {
			return err
		}
		ran := false
		if err := x.node.EachPureTask(now, func(handler chasm.NodePureTask, attrs chasm.TaskAttributes, task any) (bool, error) {
			executed, err := handler.ExecutePureTask(ctx, attrs, task)
			ran = ran || executed
			return executed, err
		}); err != nil {
			return err
		}
		if !ran {
			continue
		}
		if err := e.closeTransaction(ctx, x); err != nil {
			return err
		}
		if err := e.runSideEffectTasks(ctx, x); err != nil {
			return err
		}
	}
	return nil
}

func (e *engine) closeTransaction(ctx context.Context, x *execution) error {
	mutation, err := x.node.CloseTransaction()
	if err != nil {
		return err
	}
	for path, node := range mutation.UpdatedNodes {
		x.nodes[path] = proto.CloneOf(node)
	}
	for path := range mutation.DeletedNodes {
		delete(x.nodes, path)
	}
	x.backend.transitionCount++
	return nil
}

// discardTransaction rebuilds the tree from the last transaction. Tasks are added to the backend
// only when a transaction closes, so the backend has nothing to discard.
func (e *engine) discardTransaction(x *execution) error {
	node, err := chasm.NewTreeFromDB(x.nodes, e.registry, x.backend, chasm.DefaultPathEncoder, e.logger, metrics.NoopMetricsHandler)
	if err != nil {
		return err
	}
	x.node = node
	return nil
}

// nextDeadline returns the earliest time after now at which a task is due, if any.
func (e *engine) nextDeadline() (time.Time, bool) {
	var deadline time.Time
	for _, x := range e.executions {
		times := slices.Clone(x.backend.pureTaskTimes)
		for _, task := range x.backend.sideEffectTasks {
			times = append(times, task.VisibilityTimestamp)
		}
		for _, t := range times {
			if t.After(e.now) && (deadline.IsZero() || t.Before(deadline)) {
				deadline = t
			}
		}
	}
	return deadline, !deadline.IsZero()
}

// runSideEffectTasks executes the side-effect tasks that are due. Each runs in its own
// transaction(s) via the chasm engine functions, so it may produce further tasks.
func (e *engine) runSideEffectTasks(ctx context.Context, x *execution) error {
	engineCtx := chasm.NewEngineContext(ctx, e)
	for {
		i := slices.IndexFunc(x.backend.sideEffectTasks, func(task *tasks.ChasmTask) bool {
			return !task.VisibilityTimestamp.After(e.now)
		})
		if i < 0 {
			return nil
		}
		task := x.backend.sideEffectTasks[i]
		x.backend.sideEffectTasks = slices.Delete(x.backend.sideEffectTasks, i, i+1)
		inTree, valid, err := x.node.ValidateSideEffectTask(engineCtx, task)
		if err != nil {
			return err
		}
		if !inTree || !valid {
			continue
		}
		if err := x.node.ExecuteSideEffectTask(engineCtx, x.key, task, func(chasm.NodeBackend, chasm.Context, chasm.Component) error {
			return nil
		}); err != nil {
			return fmt.Errorf("side effect task: %w", err)
		}
	}
}

func (e *engine) deleteClosedExecution(key chasm.ExecutionKey) error {
	x, err := e.execution(chasm.ComponentRef{ExecutionKey: key})
	if err != nil {
		return err
	}
	if x.backend.isRunning() {
		return serviceerror.NewFailedPrecondition("deleting a running workflow is not supported")
	}
	e.deleteExecution(x)
	return nil
}

// deleteExecution removes an execution, running or not. Its queued tasks are discarded when they
// are dequeued, since they no longer resolve to an execution.
func (e *engine) deleteExecution(x *execution) {
	delete(e.executions, x.key)
	current := [2]string{x.key.NamespaceID, x.key.BusinessID}
	if e.current[current] == x {
		delete(e.current, current)
	}
}

func (e *engine) execution(ref chasm.ComponentRef) (*execution, error) {
	if ref.RunID == "" {
		if x, ok := e.current[[2]string{ref.NamespaceID, ref.BusinessID}]; ok {
			return x, nil
		}
	} else if x, ok := e.executions[ref.ExecutionKey]; ok {
		return x, nil
	}
	return nil, serviceerror.NewNotFoundf("execution not found: %s/%s", ref.BusinessID, ref.RunID)
}

func (e *engine) newExecution(key chasm.ExecutionKey) *execution {
	b := &backend{
		key:             key,
		logger:          e.logger,
		now:             func() time.Time { return e.now },
		transitionCount: 1,
		state:           enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		namespace: namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: key.NamespaceID, Name: key.NamespaceID},
			&persistencespb.NamespaceConfig{},
			"local",
		),
	}
	return &execution{
		key:     key,
		backend: b,
		nodes:   map[string]*persistencespb.ChasmNode{},
		node:    chasm.NewEmptyTree(e.registry, b, chasm.DefaultPathEncoder, e.logger, metrics.NoopMetricsHandler),
	}
}

// backend implements chasm.NodeBackend for one in-memory execution.
type backend struct {
	key             chasm.ExecutionKey
	logger          log.Logger
	now             func() time.Time
	transitionCount int64
	state           enumsspb.WorkflowExecutionState
	status          enumspb.WorkflowExecutionStatus
	namespace       *namespace.Namespace
	sideEffectTasks []*tasks.ChasmTask
	pureTaskTimes   []time.Time
}

var _ chasm.NodeBackend = (*backend)(nil)

// hasDueTask reports whether a pure or side-effect task is due, so that advancing time does not
// have to visit every node of every execution.
func (b *backend) hasDueTask(now time.Time) bool {
	isDue := func(t time.Time) bool { return !t.After(now) }
	return slices.ContainsFunc(b.pureTaskTimes, isDue) ||
		slices.ContainsFunc(b.sideEffectTasks, func(task *tasks.ChasmTask) bool {
			return isDue(task.VisibilityTimestamp)
		})
}

func (b *backend) isRunning() bool {
	return b.state == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED || b.state == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
}

func (b *backend) GetExecutionState() *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{State: b.state, Status: b.status, RunId: b.key.RunID}
}

func (b *backend) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	return &persistencespb.WorkflowExecutionInfo{
		NamespaceId:          b.key.NamespaceID,
		WorkflowId:           b.key.BusinessID,
		StateTransitionCount: b.transitionCount,
	}
}

func (b *backend) GetApproximatePersistedSize() int                   { return 0 }
func (b *backend) ChasmSkipPersistenceEnabled() bool                  { return false }
func (b *backend) ChasmDLQScheduledPureTaskOnValidationEnabled() bool { return false }
func (b *backend) GetNamespaceEntry() *namespace.Namespace            { return b.namespace }
func (b *backend) GetCurrentVersion() int64                           { return 1 }
func (b *backend) NextTransitionCount() int64                         { return b.transitionCount + 1 }
func (b *backend) DeleteCHASMPureTasks(before time.Time) {
	b.pureTaskTimes = slices.DeleteFunc(b.pureTaskTimes, func(t time.Time) bool { return t.Before(before) })
}
func (b *backend) IsWorkflow() bool                                           { return false }
func (b *backend) EndpointRegistry() chasm.EndpointRegistry                   { return nil }
func (b *backend) Now() time.Time                                             { return b.now() }
func (b *backend) RecordTimeSkippingTransition(*chasm.TimeSkippingTransition) {}

func (b *backend) SetTimeSkippingConfig(*commonpb.TimeSkippingConfig) {}

func (b *backend) GetWorkflowKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(b.key.NamespaceID, b.key.BusinessID, b.key.RunID)
}

func (b *backend) CurrentVersionedTransition() *persistencespb.VersionedTransition {
	return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: b.transitionCount}
}

func (b *backend) AddTasks(ts ...tasks.Task) {
	for _, task := range ts {
		switch task := task.(type) {
		case *tasks.ChasmTask:
			b.sideEffectTasks = append(b.sideEffectTasks, task)
		case *tasks.ChasmTaskPure:
			b.pureTaskTimes = append(b.pureTaskTimes, task.VisibilityTimestamp)
		default:
			softassert.Fail(b.logger, "unexpected CHASM task type", tag.NewStringTag("type", fmt.Sprintf("%T", task)))
		}
	}
}

func (b *backend) UpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) (bool, error) {
	changed := b.state != state || b.status != status
	b.state, b.status = state, status
	return changed, nil
}

// The methods below serve the server's workflow implementation, which this engine does not run;
// its executions are never workflows backed by mutable state (IsWorkflow is false).

func (b *backend) AddHistoryEvent(enumspb.EventType, func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	return nil
}

func (b *backend) GenerateEventLoadToken(*historypb.HistoryEvent) ([]byte, error) {
	return nil, serviceerror.NewUnimplemented("GenerateEventLoadToken")
}

func (b *backend) LoadHistoryEvent(context.Context, []byte) (*historypb.HistoryEvent, error) {
	return nil, serviceerror.NewUnimplemented("LoadHistoryEvent")
}

func (b *backend) HasAnyBufferedEvent(func(*historypb.HistoryEvent) bool) bool { return false }

func (b *backend) GetNexusCompletion(context.Context, string) (nexusrpc.CompleteOperationOptions, error) {
	return nexusrpc.CompleteOperationOptions{}, serviceerror.NewUnimplemented("GetNexusCompletion")
}

func (b *backend) GetNexusUpdateCompletion(context.Context, string, string) (nexusrpc.CompleteOperationOptions, error) {
	return nexusrpc.CompleteOperationOptions{}, serviceerror.NewUnimplemented("GetNexusUpdateCompletion")
}
