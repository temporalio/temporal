package chasmtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
)

// WithTimeSource overrides the engine's default real-time clock with the provided time source.
// Pass a *clock.EventTimeSource when tests need to control what ctx.Now() returns inside handlers.
// The caller holds the reference and calls ts.Update(...) directly to advance time.
func WithTimeSource(ts clock.TimeSource) EngineOption {
	return func(e *Engine) {
		e.timeSource = ts
	}
}

type (
	EngineOption func(*Engine)

	Engine struct {
		t          *testing.T
		registry   *chasm.Registry
		logger     log.Logger
		metrics    metrics.Handler
		timeSource clock.TimeSource
		executions map[executionKey]*execution
	}

	execution struct {
		key     chasm.ExecutionKey
		node    *chasm.Node
		backend *chasm.MockNodeBackend
		root    chasm.RootComponent
	}

	executionKey struct {
		namespaceID string
		businessID  string
	}
)

var _ chasm.Engine = (*Engine)(nil)

func NewEngine(
	t *testing.T,
	registry *chasm.Registry,
	opts ...EngineOption,
) *Engine {
	t.Helper()

	e := &Engine{
		t:          t,
		registry:   registry,
		logger:     testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly),
		metrics:    metrics.NoopMetricsHandler,
		timeSource: clock.NewRealTimeSource(),
		executions: make(map[executionKey]*execution),
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Ref returns a ComponentRef for a subcomponent attached to this engine. For root components,
// prefer constructing a ref directly with [chasm.NewComponentRef] using the execution key.
// Subcomponent refs cannot be constructed externally (the component path is unexported), so
// this method is needed when testing task handlers that operate on subcomponents.
func (e *Engine) Ref(component chasm.Component) chasm.ComponentRef {
	for _, execution := range e.executions {
		ref, err := execution.node.Ref(component)
		if err != nil {
			continue
		}
		structuredRef, err := chasm.DeserializeComponentRef(ref)
		require.NoError(e.t, err)
		return structuredRef
	}

	e.t.Fatalf("component %T is not attached to the chasmtest engine", component)
	return chasm.ComponentRef{}
}

func (e *Engine) StartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	_ ...chasm.TransitionOption,
) (chasm.StartExecutionResult, error) {
	if _, ok := e.executions[newExecutionKey(ref.ExecutionKey)]; ok {
		return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr("already exists", "", ref.RunID)
	}

	execution := e.newExecution(ref.ExecutionKey)
	mutableCtx := chasm.NewMutableContext(ctx, execution.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if err := execution.node.SetRootComponent(root); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	_, err = execution.node.CloseTransaction()
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	execution.root = root
	e.executions[newExecutionKey(execution.key)] = execution

	serializedRef, err := execution.node.Ref(root)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	return chasm.StartExecutionResult{
		ExecutionKey: execution.key,
		ExecutionRef: serializedRef,
		Created:      true,
	}, nil
}

func (e *Engine) UpdateWithStartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	_ ...chasm.TransitionOption,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	if execution, ok := e.executions[newExecutionKey(ref.ExecutionKey)]; ok {
		serializedRef, err := e.updateComponentInExecution(ctx, execution, ref, updateFn)
		if err != nil {
			return chasm.EngineUpdateWithStartExecutionResult{}, err
		}
		return chasm.EngineUpdateWithStartExecutionResult{
			ExecutionKey: execution.key,
			ExecutionRef: serializedRef,
			Created:      false,
		}, nil
	}

	execution := e.newExecution(ref.ExecutionKey)
	mutableCtx := chasm.NewMutableContext(ctx, execution.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if err := execution.node.SetRootComponent(root); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if err := updateFn(mutableCtx, root); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	_, err = execution.node.CloseTransaction()
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	execution.root = root
	e.executions[newExecutionKey(execution.key)] = execution

	serializedRef, err := execution.node.Ref(root)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	return chasm.EngineUpdateWithStartExecutionResult{
		ExecutionKey: execution.key,
		ExecutionRef: serializedRef,
		Created:      true,
	}, nil
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

	component, err := execution.node.Component(chasm.NewContext(ctx, execution.node), ref)
	if err != nil {
		return err
	}

	readCtx := chasm.NewContext(ctx, execution.node)
	return readFn(readCtx, component)
}

func (e *Engine) PollComponent(
	context.Context,
	chasm.ComponentRef,
	func(chasm.Context, chasm.Component) (bool, error),
	...chasm.TransitionOption,
) ([]byte, error) {
	return nil, serviceerror.NewUnimplemented("chasmtest.Engine.PollComponent")
}

func (e *Engine) DeleteExecution(
	_ context.Context,
	ref chasm.ComponentRef,
	_ chasm.DeleteExecutionRequest,
) error {
	key := newExecutionKey(ref.ExecutionKey)
	if _, ok := e.executions[key]; !ok {
		return serviceerror.NewNotFound(
			fmt.Sprintf("execution not found: namespace=%q business_id=%q run_id=%q", ref.NamespaceID, ref.BusinessID, ref.RunID),
		)
	}
	delete(e.executions, key)
	return nil
}

func (e *Engine) NotifyExecution(chasm.ExecutionKey) {}

func (e *Engine) newExecution(key chasm.ExecutionKey) *execution {
	backend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(key.NamespaceID, key.BusinessID, key.RunID)
		},
		HandleIsWorkflow: func() bool { return false },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
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

func (e *Engine) executionForRef(ref chasm.ComponentRef) (*execution, error) {
	execution, ok := e.executions[newExecutionKey(ref.ExecutionKey)]
	if !ok {
		return nil, serviceerror.NewNotFound(
			fmt.Sprintf("execution not found: namespace=%q business_id=%q run_id=%q", ref.NamespaceID, ref.BusinessID, ref.RunID),
		)
	}
	return execution, nil
}

func (e *Engine) updateComponentInExecution(
	ctx context.Context,
	execution *execution,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
) ([]byte, error) {
	component, err := execution.node.Component(chasm.NewContext(ctx, execution.node), ref)
	if err != nil {
		return nil, err
	}

	mutableCtx := chasm.NewMutableContext(ctx, execution.node)
	if err := updateFn(mutableCtx, component); err != nil {
		return nil, err
	}

	_, err = execution.node.CloseTransaction()
	if err != nil {
		return nil, err
	}

	return mutableCtx.Ref(component)
}

func newExecutionKey(key chasm.ExecutionKey) executionKey {
	return executionKey{
		namespaceID: key.NamespaceID,
		businessID:  key.BusinessID,
	}
}
