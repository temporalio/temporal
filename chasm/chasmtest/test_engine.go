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
	Option[T chasm.Component] func(*Engine[T])

	Engine[T chasm.Component] struct {
		t        *testing.T
		registry *chasm.Registry
		logger   log.Logger
		metrics  metrics.Handler

		rootExecutionKey chasm.ExecutionKey
		root             T
		rootRuntime      *runtime
		executions       map[executionLookupKey]*runtime
	}

	runtime struct {
		key        chasm.ExecutionKey
		node       *chasm.Node
		backend    *chasm.MockNodeBackend
		timeSource *clock.EventTimeSource
		root       chasm.Component
	}

	executionLookupKey struct {
		namespaceID string
		businessID  string
	}
)

func NewEngine[T chasm.Component](
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
		executions: make(map[executionLookupKey]*runtime),
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

func WithRoot[T chasm.Component](
	factory func(chasm.MutableContext) T,
) Option[T] {
	return func(e *Engine[T]) {
		runtime := e.newRuntime(e.rootExecutionKey)
		ctx := chasm.NewMutableContext(context.Background(), runtime.node)
		root := factory(ctx)
		require.NoError(e.t, runtime.node.SetRootComponent(root))
		_, err := runtime.node.CloseTransaction()
		require.NoError(e.t, err)
		e.root = root
		runtime.root = root
		e.rootRuntime = runtime
		e.executions[newExecutionLookupKey(runtime.key)] = runtime
	}
}

func WithExecutionKey[T chasm.Component](key chasm.ExecutionKey) Option[T] {
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
	for _, runtime := range e.executions {
		ref, err := runtime.node.Ref(component)
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

	runtime := e.newRuntime(ref.ExecutionKey)
	mutableCtx := chasm.NewMutableContext(ctx, runtime.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}
	if err := runtime.node.SetRootComponent(root); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	_, err = runtime.node.CloseTransaction()
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	runtime.root = root
	e.executions[newExecutionLookupKey(runtime.key)] = runtime

	serializedRef, err := runtime.node.Ref(root)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	return chasm.StartExecutionResult{
		ExecutionKey: runtime.key,
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
	if runtime, ok := e.executionForKey(ref.ExecutionKey); ok {
		serializedRef, err := e.updateComponentInRuntime(ctx, runtime, ref, updateFn)
		if err != nil {
			return chasm.EngineUpdateWithStartExecutionResult{}, err
		}
		return chasm.EngineUpdateWithStartExecutionResult{
			ExecutionKey: runtime.key,
			ExecutionRef: serializedRef,
			Created:      false,
		}, nil
	}

	runtime := e.newRuntime(ref.ExecutionKey)
	mutableCtx := chasm.NewMutableContext(ctx, runtime.node)
	root, err := startFn(mutableCtx)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if err := runtime.node.SetRootComponent(root); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	if err := updateFn(mutableCtx, root); err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
	_, err = runtime.node.CloseTransaction()
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	runtime.root = root
	e.executions[newExecutionLookupKey(runtime.key)] = runtime

	serializedRef, err := runtime.node.Ref(root)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	return chasm.EngineUpdateWithStartExecutionResult{
		ExecutionKey: runtime.key,
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
	runtime, err := e.mustExecutionForRef(ref)
	if err != nil {
		return nil, err
	}
	return e.updateComponentInRuntime(ctx, runtime, ref, updateFn)
}

func (e *Engine[T]) ReadComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	readFn func(chasm.Context, chasm.Component) error,
	_ ...chasm.TransitionOption,
) error {
	runtime, err := e.mustExecutionForRef(ref)
	if err != nil {
		return err
	}

	component, err := runtime.node.Component(chasm.NewContext(ctx, runtime.node), ref)
	if err != nil {
		return err
	}

	readCtx := chasm.NewContext(ctx, runtime.node)
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

func (e *Engine[T]) newRuntime(key chasm.ExecutionKey) *runtime {
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
	return &runtime{
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

func (e *Engine[T]) executionForKey(key chasm.ExecutionKey) (*runtime, bool) {
	runtime, ok := e.executions[newExecutionLookupKey(normalizeExecutionKey(key))]
	return runtime, ok
}

func (e *Engine[T]) mustExecutionForRef(ref chasm.ComponentRef) (*runtime, error) {
	runtime, ok := e.executionForKey(ref.ExecutionKey)
	if !ok {
		return nil, serviceerror.NewNotFound(
			fmt.Sprintf("execution not found: namespace=%q business_id=%q run_id=%q", ref.NamespaceID, ref.BusinessID, ref.RunID),
		)
	}
	return runtime, nil
}

func (e *Engine[T]) updateComponentInRuntime(
	ctx context.Context,
	runtime *runtime,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
) ([]byte, error) {
	component, err := runtime.node.Component(chasm.NewContext(ctx, runtime.node), ref)
	if err != nil {
		return nil, err
	}

	mutableCtx := chasm.NewMutableContext(ctx, runtime.node)
	if err := updateFn(mutableCtx, component); err != nil {
		return nil, err
	}

	_, err = runtime.node.CloseTransaction()
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
