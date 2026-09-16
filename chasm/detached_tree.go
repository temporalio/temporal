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

// NewDetachedTree builds a read only CHASM tree from a persisted mutable state record, outside
// the history service. It is the supported entry point for a process holding CHASM bytes but
// not the live execution, such as tdbg or a reader that describes persisted state.
//
// Register libraries with nil handlers (see chasm/lib/all); only read paths are safe, and
// write paths panic on the read only backend. Callers must strip any node paths they injected
// themselves, since the tree only understands paths its own encoder produced.
//
// Only ChasmNodes is required. ExecutionInfo and ExecutionState supply what a component reads
// through Context.ExecutionKey and Context.ExecutionInfo: the namespace, business ID, run ID,
// close time, and transition count. Those live on the record rather than in the tree, so a
// caller that omits them gets zeros for them and everything else still decodes. A caller
// storing nodes for later offline reads should store these two alongside, since nothing can
// recover them from the tree.
//
// The clock, logger, and metrics handler are fixed. A read reaches the logger and metrics
// handler only on paths unreachable here, and the clock only affects components that are still
// running, which a detached reader does not observe.
func NewDetachedTree(
	mutableState *persistencespb.WorkflowMutableState,
	registry *Registry,
) (*Node, error) {
	if mutableState == nil {
		return nil, errors.New("mutable state is nil")
	}

	return NewTreeFromDB(
		mutableState.GetChasmNodes(),
		registry,
		clock.NewRealTimeSource(),
		newReadOnlyNodeBackend(mutableState),
		DefaultPathEncoder,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
}

// DetachedRootComponent decodes mutableState and returns the root component as C, along with
// the Context to read it through. It is the usual entry point for offline readers; use
// NewDetachedTree directly only when you need the tree itself.
//
// C may be a concrete component type or an interface the root implements, such as
// DescribableComponent. A root of some other type is an error.
//
//	act, ctx, err := chasm.DetachedRootComponent[*activity.Activity](goCtx, mutableState, registry)
func DetachedRootComponent[C Component](
	goCtx context.Context,
	mutableState *persistencespb.WorkflowMutableState,
	registry *Registry,
) (C, Context, error) {
	var zero C

	root, err := NewDetachedTree(mutableState, registry)
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

// RootArchetype returns the archetype of the root component, so a caller can decide how to
// handle a record before decoding it.
func RootArchetype(
	mutableState *persistencespb.WorkflowMutableState,
	registry *Registry,
) (Archetype, error) {
	root, ok := mutableState.GetChasmNodes()[rootEncodedPath]
	if !ok {
		return "", errors.New("mutable state has no root CHASM node")
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

// readOnlyNodeBackend serves NodeBackend reads from a persisted mutable state record. Methods
// that would mutate, emit a task, or read history panic instead, since reaching one is a
// caller bug and failing quietly would return a plausible but wrong result.
type readOnlyNodeBackend struct {
	mutableState *persistencespb.WorkflowMutableState
}

var _ NodeBackend = (*readOnlyNodeBackend)(nil)

func newReadOnlyNodeBackend(mutableState *persistencespb.WorkflowMutableState) *readOnlyNodeBackend {
	return &readOnlyNodeBackend{mutableState: mutableState}
}

func (b *readOnlyNodeBackend) unsupported(method string) {
	panic("chasm: " + method + " is not available on a detached read only tree") //nolint:forbidigo
}

// GetExecutionInfo and GetExecutionState never return nil, even when the caller supplied
// neither. The framework dereferences both without a check, and these getters cannot report an
// error, so an unsupplied record reads as an empty one.
func (b *readOnlyNodeBackend) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	if info := b.mutableState.GetExecutionInfo(); info != nil {
		return info
	}
	return &persistencespb.WorkflowExecutionInfo{}
}

func (b *readOnlyNodeBackend) GetExecutionState() *persistencespb.WorkflowExecutionState {
	if state := b.mutableState.GetExecutionState(); state != nil {
		return state
	}
	return &persistencespb.WorkflowExecutionState{}
}

func (b *readOnlyNodeBackend) GetWorkflowKey() definition.WorkflowKey {
	info := b.mutableState.GetExecutionInfo()
	return definition.NewWorkflowKey(
		info.GetNamespaceId(),
		info.GetWorkflowId(),
		b.mutableState.GetExecutionState().GetRunId(),
	)
}

// Callers that describe a component and care about identity must check that the record carried
// it. A zero execution key yields a description with empty identity fields rather than an
// error, because neither this interface nor chasm.Context has a way to report one.

// GetApproximatePersistedSize returns 0. The live value is computed as mutable state is built
// and is not part of the persisted record, so there is nothing to recover it from.
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
