package chasmtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
)

type (
	EngineOption func(*Engine)

	// Engine is a lightweight in memory CHASM engine for unit tests. It implements
	// [chasm.Engine] and supports the full set of conflict and reuse policies, as
	// well as blocking [PollComponent] with [NotifyExecution], matching the behavior
	// of the production engine as closely as possible without persistence or shard logic.
	Engine struct {
		t          *testing.T
		registry   *chasm.Registry
		logger     log.Logger
		metrics    metrics.Handler
		timeSource clock.TimeSource
		// currentExecutions maps (namespaceID, businessID) to the latest run (running or closed).
		currentExecutions map[businessKey]*execution
		// allExecutions maps (namespaceID, businessID, runID) to any run, for lookups by specific RunID.
		allExecutions map[runKey]*execution
		notifier      *executionNotifier
	}

	execution struct {
		key       chasm.ExecutionKey
		node      *chasm.Node
		backend   *chasm.MockNodeBackend
		root      chasm.RootComponent
		requestID string
	}

	businessKey struct {
		namespaceID string
		businessID  string
	}

	runKey struct {
		namespaceID string
		businessID  string
		runID       string
	}
)

// WithTimeSource overrides the engine's default time source.
// The default is a [clock.EventTimeSource] initialized to [time.Now] at engine creation,
// which gives deterministic, frozen time suitable for most unit tests.
// Pass a *clock.EventTimeSource when tests need to advance time explicitly;
// the caller holds the reference and calls ts.Update(...) directly.
func WithTimeSource(ts clock.TimeSource) EngineOption {
	return func(e *Engine) {
		e.timeSource = ts
	}
}

var defaultTransitionOptions = chasm.TransitionOptions{
	ReusePolicy:    chasm.BusinessIDReusePolicyAllowDuplicate,
	ConflictPolicy: chasm.BusinessIDConflictPolicyFail,
}

var _ chasm.Engine = (*Engine)(nil)

func NewEngine(
	t *testing.T,
	registry *chasm.Registry,
	opts ...EngineOption,
) *Engine {
	t.Helper()

	ts := clock.NewEventTimeSource()
	ts.Update(time.Now())
	e := &Engine{
		t:                 t,
		registry:          registry,
		logger:            testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly),
		metrics:           metrics.NoopMetricsHandler,
		timeSource:        ts,
		currentExecutions: make(map[businessKey]*execution),
		allExecutions:     make(map[runKey]*execution),
		notifier:          newExecutionNotifier(),
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Tasks returns all physical tasks scheduled for the execution identified by ref, grouped by category.
// Logical tasks accumulate across every [Engine.UpdateComponent], [Engine.StartExecution], and
// [Engine.UpdateWithStartExecution] call on the execution, and convert to physical tasks on CloseTransaction,
// matching what the real engine would deliver to task processors.
func (e *Engine) Tasks(ref chasm.ComponentRef) (map[tasks.Category][]tasks.Task, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return nil, err
	}
	// Return a shallow copy so callers cannot mutate the internal task lists.
	result := make(map[tasks.Category][]tasks.Task, len(exec.backend.TasksByCategory))
	for cat, ts := range exec.backend.TasksByCategory {
		result[cat] = ts
	}
	return result, nil
}

func (e *Engine) StartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	opts ...chasm.TransitionOption,
) (chasm.StartExecutionResult, error) {
	options := constructTransitionOptions(opts...)
	bKey := newBusinessKey(ref.ExecutionKey)

	current, hasCurrent := e.currentExecutions[bKey]
	if hasCurrent {
		// if the requestID matches the original create request, return the existing run.
		if options.RequestID != "" && options.RequestID == current.requestID {
			serializedRef, err := current.node.Ref(current.root)
			if err != nil {
				return chasm.StartExecutionResult{}, err
			}
			return chasm.StartExecutionResult{
				ExecutionKey: current.key,
				ExecutionRef: serializedRef,
				Created:      false,
			}, nil
		}

		switch current.backend.GetExecutionState().State {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			return e.handleConflictPolicy(ctx, ref, current, startFn, options)
		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			return e.handleReusePolicy(ctx, ref, current, startFn, options)
		default:
			return chasm.StartExecutionResult{}, serviceerror.NewInternal(
				fmt.Sprintf("unexpected execution state: %v", current.backend.GetExecutionState().State),
			)
		}
	}

	return e.startNew(ctx, ref.ExecutionKey, startFn, options.RequestID)
}

func (e *Engine) UpdateWithStartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	options := constructTransitionOptions(opts...)
	bKey := newBusinessKey(ref.ExecutionKey)

	current, hasCurrent := e.currentExecutions[bKey]
	if hasCurrent {
		switch current.backend.GetExecutionState().State {
		case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
			serializedRef, err := e.updateComponentInExecution(ctx, current, ref, updateFn)
			if err != nil {
				return chasm.EngineUpdateWithStartExecutionResult{}, err
			}
			return chasm.EngineUpdateWithStartExecutionResult{
				ExecutionKey: current.key,
				ExecutionRef: serializedRef,
				Created:      false,
			}, nil
		case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
			switch options.ReusePolicy {
			case chasm.BusinessIDReusePolicyAllowDuplicate:
			case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
				if !executionFailed(current) {
					return chasm.EngineUpdateWithStartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
						fmt.Sprintf(
							"CHASM execution already completed successfully. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
							ref.BusinessID, current.key.RunID, options.ReusePolicy,
						),
						current.requestID,
						current.key.RunID,
					)
				}
			case chasm.BusinessIDReusePolicyRejectDuplicate:
				return chasm.EngineUpdateWithStartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
					fmt.Sprintf(
						"CHASM execution already finished. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
						ref.BusinessID, current.key.RunID, options.ReusePolicy,
					),
					current.requestID,
					current.key.RunID,
				)
			default:
				return chasm.EngineUpdateWithStartExecutionResult{}, serviceerror.NewInternal(
					fmt.Sprintf("unknown business ID reuse policy: %v", options.ReusePolicy),
				)
			}
		default:
			return chasm.EngineUpdateWithStartExecutionResult{}, serviceerror.NewInternal(
				fmt.Sprintf("unexpected execution state: %v", current.backend.GetExecutionState().State),
			)
		}
	}

	return e.startAndUpdateNew(ctx, ref.ExecutionKey, startFn, updateFn, options.RequestID)
}

func (e *Engine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	_ ...chasm.TransitionOption,
) ([]byte, error) {
	execution, err := e.executionForRef(ref)
	if err != nil {
		return nil, err
	}
	return e.updateComponentInExecution(ctx, execution, ref, updateFn)
}

func (e *Engine) ReadComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	readFn func(chasm.Context, chasm.Component) error,
	_ ...chasm.TransitionOption,
) error {
	execution, err := e.executionForRef(ref)
	if err != nil {
		return err
	}

	chasmCtx := chasm.NewContext(ctx, execution.node)
	component, err := execution.node.Component(chasmCtx, ref)
	if err != nil {
		return err
	}

	return readFn(chasmCtx, component)
}

// PollComponent waits until the supplied predicate is satisfied when evaluated against the
// component identified by ref. If the predicate is true immediately it returns without blocking.
// Otherwise it subscribes to [NotifyExecution] signals and re evaluates after each one, just
// like the production engine. Returns (nil, nil) if ctx is cancelled, matching the long poll
// timeout semantics of the production engine where the caller is expected to re-poll.
func (e *Engine) PollComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	predicate func(chasm.Context, chasm.Component) (bool, error),
	_ ...chasm.TransitionOption,
) ([]byte, error) {
	executionKey := ref.ExecutionKey

	checkPredicate := func() ([]byte, bool, error) {
		exec, err := e.executionForRef(ref)
		if err != nil {
			return nil, false, err
		}
		chasmCtx := chasm.NewContext(ctx, exec.node)
		component, err := exec.node.Component(chasmCtx, ref)
		if err != nil {
			return nil, false, err
		}
		satisfied, err := predicate(chasmCtx, component)
		if err != nil || !satisfied {
			return nil, satisfied, err
		}
		serializedRef, err := exec.node.Ref(component)
		return serializedRef, true, err
	}

	// Evaluate once before subscribing.
	serializedRef, satisfied, err := checkPredicate()
	if err != nil || satisfied {
		return serializedRef, err
	}

	for {
		ch, unsubscribe := e.notifier.subscribe(executionKey)
		// Re evaluate while holding the subscription to avoid missing a notification
		// that arrives between the failed check above and this subscribe call.
		serializedRef, satisfied, err = checkPredicate()
		if err != nil || satisfied {
			unsubscribe()
			return serializedRef, err
		}

		select {
		case <-ch:
			unsubscribe()
			serializedRef, satisfied, err = checkPredicate()
			if err != nil || satisfied {
				return serializedRef, err
			}
		case <-ctx.Done():
			unsubscribe()
			return nil, nil //nolint:nilerr // nil, nil = long-poll timeout; caller should re-poll
		}
	}
}

// NotifyExecution wakes up any [PollComponent] callers waiting on the execution.
func (e *Engine) NotifyExecution(key chasm.ExecutionKey) {
	e.notifier.notify(key)
}

func (e *Engine) DeleteExecution(
	_ context.Context,
	ref chasm.ComponentRef,
	_ chasm.DeleteExecutionRequest,
) error {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return err
	}
	rKey := newRunKey(exec.key)
	bKey := newBusinessKey(exec.key)
	delete(e.allExecutions, rKey)
	// Only evict from current if this is still the current run for the businessID.
	if cur, ok := e.currentExecutions[bKey]; ok && cur == exec {
		delete(e.currentExecutions, bKey)
	}
	return nil
}

// handleConflictPolicy is called when a StartExecution arrives for a business ID whose
// current run is still running.
func (e *Engine) handleConflictPolicy(
	ctx context.Context,
	ref chasm.ComponentRef,
	current *execution,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	options chasm.TransitionOptions,
) (chasm.StartExecutionResult, error) {
	switch options.ConflictPolicy {
	case chasm.BusinessIDConflictPolicyFail:
		return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
			fmt.Sprintf(
				"CHASM execution still running. BusinessID: %s, RunID: %s, ID Conflict Policy: %v",
				ref.BusinessID, current.key.RunID, options.ConflictPolicy,
			),
			current.requestID,
			current.key.RunID,
		)
	case chasm.BusinessIDConflictPolicyTerminateExisting:
		_, _ = current.backend.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		)
		return e.startNew(ctx, ref.ExecutionKey, startFn, options.RequestID)
	case chasm.BusinessIDConflictPolicyUseExisting:
		serializedRef, err := current.node.Ref(current.root)
		if err != nil {
			return chasm.StartExecutionResult{}, err
		}
		return chasm.StartExecutionResult{
			ExecutionKey: current.key,
			ExecutionRef: serializedRef,
			Created:      false,
		}, nil
	default:
		return chasm.StartExecutionResult{}, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID conflict policy: %v", options.ConflictPolicy),
		)
	}
}

// handleReusePolicy is called when a StartExecution arrives for a business ID whose
// current run is closed or completed.
func (e *Engine) handleReusePolicy(
	ctx context.Context,
	ref chasm.ComponentRef,
	current *execution,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	options chasm.TransitionOptions,
) (chasm.StartExecutionResult, error) {
	switch options.ReusePolicy {
	case chasm.BusinessIDReusePolicyAllowDuplicate:
	case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
		if !executionFailed(current) {
			return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
				fmt.Sprintf(
					"CHASM execution already completed successfully. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
					ref.BusinessID, current.key.RunID, options.ReusePolicy,
				),
				current.requestID,
				current.key.RunID,
			)
		}
	case chasm.BusinessIDReusePolicyRejectDuplicate:
		return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
			fmt.Sprintf(
				"CHASM execution already finished. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
				ref.BusinessID, current.key.RunID, options.ReusePolicy,
			),
			current.requestID,
			current.key.RunID,
		)
	default:
		return chasm.StartExecutionResult{}, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID reuse policy: %v", options.ReusePolicy),
		)
	}
	return e.startNew(ctx, ref.ExecutionKey, startFn, options.RequestID)
}

// startNew creates a new execution and registers it as the current run for the business ID.
func (e *Engine) startNew(
	ctx context.Context,
	key chasm.ExecutionKey,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	requestID string,
) (chasm.StartExecutionResult, error) {
	exec := e.newExecution(key)
	exec.requestID = requestID

	mutableCtx := chasm.NewMutableContext(ctx, exec.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if err := exec.node.SetRootComponent(root); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if _, err = exec.node.CloseTransaction(); err != nil {
		return chasm.StartExecutionResult{}, err
	}

	exec.root = root
	e.currentExecutions[newBusinessKey(exec.key)] = exec
	e.allExecutions[newRunKey(exec.key)] = exec

	serializedRef, err := exec.node.Ref(root)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	return chasm.StartExecutionResult{
		ExecutionKey: exec.key,
		ExecutionRef: serializedRef,
		Created:      true,
	}, nil
}

// startAndUpdateNew creates a new execution, applies startFn and updateFn in the same
// transaction, and registers it as the current run for the business ID.
func (e *Engine) startAndUpdateNew(
	ctx context.Context,
	key chasm.ExecutionKey,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	requestID string,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	exec := e.newExecution(key)
	exec.requestID = requestID

	mutableCtx := chasm.NewMutableContext(ctx, exec.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if err := exec.node.SetRootComponent(root); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if err := updateFn(mutableCtx, root); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if _, err = exec.node.CloseTransaction(); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	exec.root = root
	e.currentExecutions[newBusinessKey(exec.key)] = exec
	e.allExecutions[newRunKey(exec.key)] = exec

	serializedRef, err := exec.node.Ref(root)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	return chasm.EngineUpdateWithStartExecutionResult{
		ExecutionKey: exec.key,
		ExecutionRef: serializedRef,
		Created:      true,
	}, nil
}

func (e *Engine) newExecution(key chasm.ExecutionKey) *execution {
	// bsMu (backend state mutex) guards transitionCount and execState, which are shared
	// across handler closures. It is separate from MockNodeBackend's internal mu to avoid deadlocks.
	var (
		bsMu            sync.Mutex
		transitionCount int64 = 1
		execState             = persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}
	)

	backend := &chasm.MockNodeBackend{
		// NextTransitionCount increments on every CloseTransaction call, matching
		// the real engine's per transition monotonic counter.
		HandleNextTransitionCount: func() int64 {
			bsMu.Lock()
			defer bsMu.Unlock()
			transitionCount++
			return transitionCount
		},
		// CurrentVersionedTransition reflects the latest committed transition count.
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			bsMu.Lock()
			defer bsMu.Unlock()
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          transitionCount,
			}
		},
		HandleGetCurrentVersion: func() int64 { return 1 },
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(key.NamespaceID, key.BusinessID, key.RunID)
		},
		HandleIsWorkflow: func() bool { return false },
		// GetExecutionState returns the current lifecycle state, which CloseTransaction
		// uses to decide whether to call UpdateWorkflowStateStatus on the backend.
		HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
			bsMu.Lock()
			defer bsMu.Unlock()
			return &persistencespb.WorkflowExecutionState{
				State:  execState.State,
				Status: execState.Status,
			}
		},
		// UpdateWorkflowStateStatus is called by CloseTransaction when the root
		// component's LifecycleState changes from Running to Completed, Failed, or Terminated.
		HandleUpdateWorkflowStateStatus: func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
			bsMu.Lock()
			defer bsMu.Unlock()
			changed := execState.State != state || execState.Status != status
			execState.State = state
			execState.Status = status
			return changed, nil
		},
	}
	return &execution{
		key:     key,
		backend: backend,
		node: chasm.NewEmptyTree(
			e.registry,
			e.timeSource,
			backend,
			chasm.DefaultPathEncoder,
			e.logger,
			e.metrics,
		),
	}
}

// executionForRef looks up an execution by the ref's RunID when present, or falls back
// to the current run for the business ID when RunID is empty.
func (e *Engine) executionForRef(ref chasm.ComponentRef) (*execution, error) {
	if ref.RunID != "" {
		exec, ok := e.allExecutions[newRunKey(ref.ExecutionKey)]
		if !ok {
			return nil, serviceerror.NewNotFound(
				fmt.Sprintf("execution not found: namespace=%q business_id=%q run_id=%q", ref.NamespaceID, ref.BusinessID, ref.RunID),
			)
		}
		return exec, nil
	}
	exec, ok := e.currentExecutions[newBusinessKey(ref.ExecutionKey)]
	if !ok {
		return nil, serviceerror.NewNotFound(
			fmt.Sprintf("execution not found: namespace=%q business_id=%q", ref.NamespaceID, ref.BusinessID),
		)
	}
	return exec, nil
}

func (e *Engine) updateComponentInExecution(
	ctx context.Context,
	execution *execution,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
) ([]byte, error) {
	chasmCtx := chasm.NewContext(ctx, execution.node)
	component, err := execution.node.Component(chasmCtx, ref)
	if err != nil {
		return nil, err
	}

	mutableCtx := chasm.NewMutableContext(ctx, execution.node)
	if err := updateFn(mutableCtx, component); err != nil {
		return nil, err
	}

	if _, err = execution.node.CloseTransaction(); err != nil {
		return nil, err
	}

	return mutableCtx.Ref(component)
}

// refForComponent looks up the ComponentRef for a component instance by scanning
// all executions. It works because Node.CloseTransaction (called after every mutation)
// runs syncSubComponents, which populates the node's valueToNode map for all
// subcomponents. Returns an error if the component is not found in any execution.
func (e *Engine) refForComponent(component chasm.Component) (chasm.ComponentRef, error) {
	for _, exec := range e.allExecutions {
		serialized, err := exec.node.Ref(component)
		if err != nil {
			if errors.As(err, new(*serviceerror.NotFound)) {
				continue // component not registered in this execution's node
			}
			return chasm.ComponentRef{}, err
		}
		return chasm.DeserializeComponentRef(serialized)
	}
	return chasm.ComponentRef{}, fmt.Errorf("component %T not found in any execution managed by this engine", component)
}

func constructTransitionOptions(opts ...chasm.TransitionOption) chasm.TransitionOptions {
	options := defaultTransitionOptions
	for _, opt := range opts {
		opt(&options)
	}
	// NOTE: TransitionOptions.Speculative is intentionally not implemented here. It is also
	// unimplemented in the production engine (see the TODO in service/history/chasm_engine.go).
	return options
}

// executionFailed reports whether a closed execution ended in a failure state
// (failed, terminated, cancelled, or timed out). This drives the
// [chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly] reuse policy check.
func executionFailed(exec *execution) bool {
	return exec.backend.GetExecutionState().Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
}

func newBusinessKey(key chasm.ExecutionKey) businessKey {
	return businessKey{namespaceID: key.NamespaceID, businessID: key.BusinessID}
}

func newRunKey(key chasm.ExecutionKey) runKey {
	return runKey{namespaceID: key.NamespaceID, businessID: key.BusinessID, runID: key.RunID}
}

// executionNotifier allows [PollComponent] callers to subscribe to state change
// signals for a given execution. notify closes the channel for all current
// subscribers and each subscriber must resubscribe after being woken.
type executionNotifier struct {
	mu          sync.Mutex
	subscribers map[chasm.ExecutionKey][]chan struct{}
}

func newExecutionNotifier() *executionNotifier {
	return &executionNotifier{
		subscribers: make(map[chasm.ExecutionKey][]chan struct{}),
	}
}

// subscribe returns a channel that will be closed on the next notify call for key,
// and an unsubscribe function that must be called when the caller is done waiting.
func (n *executionNotifier) subscribe(key chasm.ExecutionKey) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	n.mu.Lock()
	n.subscribers[key] = append(n.subscribers[key], ch)
	n.mu.Unlock()

	unsubscribed := false
	unsubscribe := func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if unsubscribed {
			return
		}
		unsubscribed = true
		subs := n.subscribers[key]
		for i, s := range subs {
			if s == ch {
				n.subscribers[key] = append(subs[:i], subs[i+1:]...)
				if len(n.subscribers[key]) == 0 {
					delete(n.subscribers, key)
				}
				break
			}
		}
	}
	return ch, unsubscribe
}

// notify closes all subscriber channels for key, waking any blocked PollComponent callers.
func (n *executionNotifier) notify(key chasm.ExecutionKey) {
	n.mu.Lock()
	subs := n.subscribers[key]
	delete(n.subscribers, key)
	n.mu.Unlock()

	for _, ch := range subs {
		close(ch)
	}
}
