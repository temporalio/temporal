package chasmtest

import (
	"context"
	"fmt"
	"testing"
	"time"

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

type (
	Option[T chasm.RootComponent] func(*Engine[T])

	Engine[T chasm.RootComponent] struct {
		t        *testing.T
		registry *chasm.Registry
		logger   log.Logger
		metrics  metrics.Handler

		rootExecutionKey chasm.ExecutionKey
		root             T
		rootExecution    *execution
		executions       map[executionLookupKey]*execution
	}

	execution struct {
		key        chasm.ExecutionKey
		node       *chasm.Node
		backend    *chasm.MockNodeBackend
		timeSource *clock.EventTimeSource
		root       chasm.RootComponent
	}

	executionLookupKey struct {
		namespaceID string
		businessID  string
	}
)

func NewEngine[T chasm.RootComponent](
	t *testing.T,
	registry *chasm.Registry,
	opts ...Option[T],
) *Engine[T] {
	t.Helper()

	e := &Engine[T]{
		t:        t,
		registry: registry,
		logger:   testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly),
		metrics:  metrics.NoopMetricsHandler,
		rootExecutionKey: chasm.ExecutionKey{
			NamespaceID: "test-namespace-id",
			BusinessID:  "test-workflow-id",
			RunID:       "test-run-id",
		},
		executions: make(map[executionLookupKey]*execution),
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

func WithRoot[T chasm.RootComponent](
	factory func(chasm.MutableContext) T,
) Option[T] {
	return func(e *Engine[T]) {
		execution := e.newExecution(e.rootExecutionKey)
		ctx := chasm.NewMutableContext(context.Background(), execution.node)
		root := factory(ctx)
		require.NoError(e.t, execution.node.SetRootComponent(root))
		_, err := execution.node.CloseTransaction()
		require.NoError(e.t, err)
		e.root = root
		execution.root = root
		e.rootExecution = execution
		e.executions[newExecutionLookupKey(execution.key)] = execution
	}
}

func WithExecutionKey[T chasm.RootComponent](key chasm.ExecutionKey) Option[T] {
	return func(e *Engine[T]) {
		e.rootExecutionKey = key
	}
}

func (e *Engine[T]) Root() T {
	return e.root
}

func (e *Engine[T]) EngineContext() context.Context {
	return chasm.NewEngineContext(context.Background(), e)
}

func (e *Engine[T]) Ref(component chasm.Component) chasm.ComponentRef {
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

func (e *Engine[T]) StartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	_ ...chasm.TransitionOption,
) (chasm.StartExecutionResult, error) {
	if _, ok := e.executionForKey(ref.ExecutionKey); ok {
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
	e.executions[newExecutionLookupKey(execution.key)] = execution

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

func (e *Engine[T]) UpdateWithStartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	_ ...chasm.TransitionOption,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	if execution, ok := e.executionForKey(ref.ExecutionKey); ok {
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
	e.executions[newExecutionLookupKey(execution.key)] = execution

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

func (e *Engine[T]) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	_ ...chasm.TransitionOption,
) ([]byte, error) {
	execution, err := e.mustExecutionForRef(ref)
	if err != nil {
		return nil, err
	}
	return e.updateComponentInExecution(ctx, execution, ref, updateFn)
}

func (e *Engine[T]) ReadComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	readFn func(chasm.Context, chasm.Component) error,
	_ ...chasm.TransitionOption,
) error {
	execution, err := e.mustExecutionForRef(ref)
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

func (e *Engine[T]) PollComponent(
	context.Context,
	chasm.ComponentRef,
	func(chasm.Context, chasm.Component) (bool, error),
	...chasm.TransitionOption,
) ([]byte, error) {
	return nil, serviceerror.NewUnimplemented("chasmtest.Engine.PollComponent")
}

func (e *Engine[T]) DeleteExecution(
	context.Context,
	chasm.ComponentRef,
	chasm.DeleteExecutionRequest,
) error {
	return serviceerror.NewUnimplemented("chasmtest.Engine.DeleteExecution")
}

func (e *Engine[T]) NotifyExecution(chasm.ExecutionKey) {}

func (e *Engine[T]) newExecution(key chasm.ExecutionKey) *execution {
	key = normalizeExecutionKey(key)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
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
		key:        key,
		backend:    backend,
		timeSource: timeSource,
		node: chasm.NewEmptyTree(
			e.registry,
			timeSource,
			backend,
			chasm.DefaultPathEncoder,
			e.logger,
			e.metrics,
		),
	}
}

func (e *Engine[T]) executionForKey(key chasm.ExecutionKey) (*execution, bool) {
	execution, ok := e.executions[newExecutionLookupKey(normalizeExecutionKey(key))]
	return execution, ok
}

func (e *Engine[T]) mustExecutionForRef(ref chasm.ComponentRef) (*execution, error) {
	execution, ok := e.executionForKey(ref.ExecutionKey)
	if !ok {
		return nil, serviceerror.NewNotFound(
			fmt.Sprintf("execution not found: namespace=%q business_id=%q run_id=%q", ref.NamespaceID, ref.BusinessID, ref.RunID),
		)
	}
	return execution, nil
}

func (e *Engine[T]) updateComponentInExecution(
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

func newExecutionLookupKey(key chasm.ExecutionKey) executionLookupKey {
	return executionLookupKey{
		namespaceID: key.NamespaceID,
		businessID:  key.BusinessID,
	}
}

func normalizeExecutionKey(key chasm.ExecutionKey) chasm.ExecutionKey {
	if key.NamespaceID == "" {
		key.NamespaceID = "test-namespace-id"
	}
	if key.BusinessID == "" {
		key.BusinessID = "test-workflow-id"
	}
	return key
}
