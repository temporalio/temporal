package chasm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/tasks"
)

// rootEncodedPath is what DefaultPathEncoder produces for the root node.
const rootEncodedPath = ""

// NewDetachedTree builds a read only CHASM tree from persisted nodes, without mutable state,
// for callers holding CHASM bytes but not the execution they came from, such as tdbg.
//
// Register libraries with nil handlers (see chasm/lib/all); only read paths are safe, and
// write paths panic on the read only backend. Callers must strip any node paths they injected
// themselves, since the tree only understands paths its own encoder produced.
//
// The clock, logger, and metrics handler are fixed. A read reaches the logger and metrics
// handler only on paths that are unreachable here, and the clock only affects components that
// are still running, which a detached reader does not observe.
//
// There is no execution behind the tree, so whatever a component reads through
// Context.ExecutionKey or Context.ExecutionInfo comes back zero: no namespace, business ID,
// run ID, close time, state size, or transition count. Those belong to the mutable state row
// that held the nodes rather than to the nodes, so a caller that knows them fills them in.
//
//	root, err := chasm.NewDetachedTree(nodes, registry)
//	ctx := chasm.NewContext(goCtx, root)
//	component, err := root.Component(ctx, chasm.ComponentRef{})
func NewDetachedTree(
	nodes map[string]*persistencespb.ChasmNode,
	registry *Registry,
) (*Node, error) {
	return NewTreeFromDB(
		nodes,
		registry,
		clock.NewRealTimeSource(),
		newReadOnlyNodeBackend(),
		DefaultPathEncoder,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
}

// DetachedRootComponent decodes nodes and returns the root component as C, along with the
// Context to read it through. It is the usual entry point for offline readers; use
// NewDetachedTree directly only when you need the tree itself.
//
// C may be a concrete component type or an interface the root implements, such as
// DescribableComponent. A root of some other type is an error.
//
//	act, ctx, err := chasm.DetachedRootComponent[*activity.Activity](goCtx, nodes, registry)
func DetachedRootComponent[C Component](
	goCtx context.Context,
	nodes map[string]*persistencespb.ChasmNode,
	registry *Registry,
) (C, Context, error) {
	var zero C

	root, err := NewDetachedTree(nodes, registry)
	if err != nil {
		return zero, nil, err
	}

	chasmContext := NewContext(goCtx, root)
	component, err := root.Component(chasmContext, ComponentRef{})
	if err != nil {
		return zero, nil, err
	}

	typed, ok := component.(C)
	if !ok {
		return zero, nil, fmt.Errorf("root component is %T, want %s", component, reflect.TypeFor[C]())
	}
	return typed, chasmContext, nil
}

// RootArchetype returns the archetype of the root component in a persisted node map, so a
// caller can decide how to handle a tree before decoding it.
func RootArchetype(
	nodes map[string]*persistencespb.ChasmNode,
	registry *Registry,
) (Archetype, error) {
	root, ok := nodes[rootEncodedPath]
	if !ok {
		return "", errors.New("node map has no root node")
	}

	attributes := root.GetMetadata().GetComponentAttributes()
	if attributes == nil {
		return "", fmt.Errorf("root node is not a component: %v", root.GetMetadata().GetAttributes())
	}

	fqn, ok := registry.ComponentFqnByID(attributes.GetTypeId())
	if !ok {
		return "", fmt.Errorf("unknown component type ID %d", attributes.GetTypeId())
	}
	return fqn, nil
}

// readOnlyNodeBackend stands in for the mutable state a detached tree does not have. Reads
// return zero values; methods that would mutate, emit a task, or read history panic instead,
// since reaching one is a caller bug and failing quietly would return a plausible but wrong
// result.
type readOnlyNodeBackend struct{}

var _ NodeBackend = (*readOnlyNodeBackend)(nil)

func newReadOnlyNodeBackend() *readOnlyNodeBackend {
	return &readOnlyNodeBackend{}
}

func (b *readOnlyNodeBackend) unsupported(method string) {
	panic("chasm: " + method + " is not available on a detached read only tree") //nolint:forbidigo
}

func (b *readOnlyNodeBackend) GetWorkflowKey() definition.WorkflowKey {
	return definition.WorkflowKey{}
}

func (b *readOnlyNodeBackend) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	return &persistencespb.WorkflowExecutionInfo{}
}

func (b *readOnlyNodeBackend) GetExecutionState() *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{}
}

func (b *readOnlyNodeBackend) GetApproximatePersistedSize() int { return 0 }

func (b *readOnlyNodeBackend) GetNamespaceEntry() *namespace.Namespace { return nil }

// ChasmSkipPersistenceEnabled is false: a detached tree is never persisted.
func (b *readOnlyNodeBackend) ChasmSkipPersistenceEnabled() bool { return false }

// ChasmDLQScheduledPureTaskOnValidationEnabled is false: a detached tree validates no tasks.
func (b *readOnlyNodeBackend) ChasmDLQScheduledPureTaskOnValidationEnabled() bool { return false }

func (b *readOnlyNodeBackend) GetCurrentVersion() int64 { return 0 }

func (b *readOnlyNodeBackend) CurrentVersionedTransition() *persistencespb.VersionedTransition {
	return nil
}

func (b *readOnlyNodeBackend) IsWorkflow() bool { return false }

func (b *readOnlyNodeBackend) EndpointRegistry() EndpointRegistry { return nil }

func (b *readOnlyNodeBackend) NextTransitionCount() int64 {
	b.unsupported("NextTransitionCount")
	return 0
}

func (b *readOnlyNodeBackend) AddTasks(...tasks.Task) {
	b.unsupported("AddTasks")
}

func (b *readOnlyNodeBackend) DeleteCHASMPureTasks(time.Time) {
	b.unsupported("DeleteCHASMPureTasks")
}

func (b *readOnlyNodeBackend) UpdateWorkflowStateStatus(
	enumsspb.WorkflowExecutionState,
	enumspb.WorkflowExecutionStatus,
) (bool, error) {
	b.unsupported("UpdateWorkflowStateStatus")
	return false, nil
}

func (b *readOnlyNodeBackend) AddHistoryEvent(
	enumspb.EventType,
	func(*historypb.HistoryEvent),
) *historypb.HistoryEvent {
	b.unsupported("AddHistoryEvent")
	return nil
}

func (b *readOnlyNodeBackend) LoadHistoryEvent(context.Context, []byte) (*historypb.HistoryEvent, error) {
	b.unsupported("LoadHistoryEvent")
	return nil, nil
}

func (b *readOnlyNodeBackend) GenerateEventLoadToken(*historypb.HistoryEvent) ([]byte, error) {
	b.unsupported("GenerateEventLoadToken")
	return nil, nil
}

func (b *readOnlyNodeBackend) HasAnyBufferedEvent(func(*historypb.HistoryEvent) bool) bool {
	b.unsupported("HasAnyBufferedEvent")
	return false
}

func (b *readOnlyNodeBackend) GetNexusCompletion(
	context.Context,
	string,
) (nexusrpc.CompleteOperationOptions, error) {
	b.unsupported("GetNexusCompletion")
	return nexusrpc.CompleteOperationOptions{}, nil
}

func (b *readOnlyNodeBackend) GetNexusUpdateCompletion(
	context.Context,
	string,
	string,
) (nexusrpc.CompleteOperationOptions, error) {
	b.unsupported("GetNexusUpdateCompletion")
	return nexusrpc.CompleteOperationOptions{}, nil
}
