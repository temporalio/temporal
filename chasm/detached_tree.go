package chasm

import (
	"context"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"google.golang.org/protobuf/proto"
)

// NewDetachedExecution decodes a persisted mutable state record outside the history service
// and returns the root component as C, along with the Context to read it through. It is the
// supported entry point for a process holding CHASM bytes but not the live execution, such as
// tdbg or a reader that describes persisted state.
//
// C may be a concrete component type or an interface the root implements, such as
// DescribableComponent. A root of some other type is an error.
//
//	act, ctx, err := chasm.NewDetachedExecution[*activity.Activity](goCtx, mutableState, registry)
//
// Register libraries with nil handlers (see chasm/lib/all); only read paths are safe, and
// write paths panic on the read only backend. Callers must strip any node paths they injected
// themselves, since the tree only understands paths its own encoder produced.
//
// ChasmNodes, ExecutionInfo, and ExecutionState are all required. The last two supply what a
// component reads through Context.ExecutionKey and Context.ExecutionInfo: the namespace,
// business ID, run ID, close time, and transition history. Those live on the record rather
// than in the tree, so a caller storing nodes for later offline reads must store them
// alongside, since nothing can recover them from the tree.
//
// A record with no CHASM nodes returns ErrNoChasmNodes: it is a workflow whose state lives
// outside the tree, so there is no component to decode. Use ExecutionArchetypeID to route such
// records first.
func NewDetachedExecution[C Component](
	goCtx context.Context,
	mutableState *persistencespb.WorkflowMutableState,
	registry *Registry,
) (C, Context, error) {
	var zero C

	root, err := newDetachedTree(mutableState, registry)
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
		return zero, nil, serviceerror.NewInternalf("root component is %T, want %s", component, reflect.TypeFor[C]())
	}
	return typed, chasmContext, nil
}

// ErrNoChasmNodes is returned by NewDetachedExecution for a record with no CHASM nodes: a
// workflow that never used a CHASM feature, whose state lives outside the tree. A debugging tool
// can match it with errors.Is to report that there is no CHASM state, rather than a failure.
var ErrNoChasmNodes = serviceerror.NewInternal("detached CHASM execution: mutable state has no CHASM nodes")

// rootEncodedPath is what DefaultPathEncoder produces for the root node.
const rootEncodedPath = ""

// ExecutionArchetypeID returns the archetype ID of a persisted execution, read from its root
// node without decoding the tree, so a caller can decide how to handle a record before calling
// NewDetachedExecution. It needs no registry, so it also works for archetypes the caller has
// not registered.
//
// A record with no CHASM nodes is a workflow that never used a CHASM feature, and reports
// WorkflowArchetypeID, matching how NewTreeFromDB reads it.
func ExecutionArchetypeID(mutableState *persistencespb.WorkflowMutableState) (ArchetypeID, error) {
	if mutableState == nil {
		return UnspecifiedArchetypeID, serviceerror.NewInternal("detached CHASM execution: mutable state is nil")
	}
	return archetypeIDOfNodes(mutableState.GetChasmNodes())
}

// archetypeIDOfNodes is the one place detached reads derive an archetype, so
// ExecutionArchetypeID, newDetachedTree, and readOnlyNodeBackend.IsWorkflow cannot disagree.
func archetypeIDOfNodes(nodes map[string]*persistencespb.ChasmNode) (ArchetypeID, error) {
	if len(nodes) == 0 {
		return WorkflowArchetypeID, nil
	}

	// The root is written before any child and never deleted, so a tree without one is corrupt.
	root, ok := nodes[rootEncodedPath]
	if !ok {
		return UnspecifiedArchetypeID, serviceerror.NewInternal("detached CHASM execution: mutable state has no root node")
	}

	// The archetype ID is the root component's type ID, as in Node.ArchetypeID.
	attributes := root.GetMetadata().GetComponentAttributes()
	if attributes == nil {
		return UnspecifiedArchetypeID, serviceerror.NewInternal("detached CHASM execution: root node is not a component")
	}

	// SetRootComponent always sets the type ID, so a persisted zero is a bug rather than an archetype.
	archetypeID := attributes.GetTypeId()
	if archetypeID == UnspecifiedArchetypeID {
		return UnspecifiedArchetypeID, serviceerror.NewInternal("detached CHASM execution: root node has no type ID")
	}
	return archetypeID, nil
}

// newDetachedTree builds the read only tree behind NewDetachedExecution.
//
// The logger and metrics handler are fixed, as is the backend's clock. A read reaches the logger and metrics
// handler only on paths unreachable here, and the clock only affects components that are still
// running, which a detached reader does not observe.
func newDetachedTree(
	mutableState *persistencespb.WorkflowMutableState,
	registry *Registry,
) (*Node, error) {
	switch {
	case mutableState == nil:
		return nil, serviceerror.NewInternal("detached CHASM execution: mutable state is nil")
	case mutableState.GetExecutionInfo() == nil:
		return nil, serviceerror.NewInternal("detached CHASM execution: mutable state has no execution info")
	case mutableState.GetExecutionState() == nil:
		return nil, serviceerror.NewInternal("detached CHASM execution: mutable state has no execution state")
	case len(mutableState.GetChasmNodes()) == 0:
		// A workflow that never used a CHASM feature. Its state lives in mutable state, outside the
		// tree, so there is no component to decode. NewTreeFromDB would also build a new root
		// here, stamping it with write-time values the read only backend cannot supply.
		return nil, ErrNoChasmNodes
	}

	// Rejects a corrupt root up front rather than failing somewhere inside decoding.
	if _, err := archetypeIDOfNodes(mutableState.GetChasmNodes()); err != nil {
		return nil, err
	}

	return NewTreeFromDB(
		mutableState.GetChasmNodes(),
		registry,
		newReadOnlyNodeBackend(mutableState),
		DefaultPathEncoder,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
}

// readOnlyNodeBackend serves NodeBackend reads from a persisted mutable state record. Methods
// that only the history service's write or task paths reach panic instead, since reaching one
// is a caller bug and failing quietly would return a plausible but wrong result.
type readOnlyNodeBackend struct {
	mutableState *persistencespb.WorkflowMutableState
	timeSource   clock.TimeSource
	// approximateSize is computed once, since the record never changes under a read only tree.
	approximateSize int
}

var _ NodeBackend = (*readOnlyNodeBackend)(nil)

// newReadOnlyNodeBackend expects mutableState to carry ExecutionInfo and ExecutionState, which
// newDetachedTree checks.
func newReadOnlyNodeBackend(mutableState *persistencespb.WorkflowMutableState) *readOnlyNodeBackend {
	return &readOnlyNodeBackend{
		mutableState:    mutableState,
		timeSource:      clock.NewRealTimeSource(),
		approximateSize: proto.Size(mutableState),
	}
}

func (b *readOnlyNodeBackend) unsupported(method string) {
	panic("chasm: " + method + " is not available on a detached read only tree") //nolint:forbidigo
}

func (b *readOnlyNodeBackend) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	return b.mutableState.GetExecutionInfo()
}

func (b *readOnlyNodeBackend) GetExecutionState() *persistencespb.WorkflowExecutionState {
	return b.mutableState.GetExecutionState()
}

func (b *readOnlyNodeBackend) GetWorkflowKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(
		b.mutableState.GetExecutionInfo().GetNamespaceId(),
		b.mutableState.GetExecutionInfo().GetWorkflowId(),
		b.mutableState.GetExecutionState().GetRunId(),
	)
}

// GetApproximatePersistedSize returns the encoded size of the record. The history service
// sums the sizes of the same pieces as it loads a record, so the two agree closely.
func (b *readOnlyNodeBackend) GetApproximatePersistedSize() int { return b.approximateSize }

// CurrentVersionedTransition is reached through Context.Ref. The live value is the last entry
// of the transition history, which the record carries.
func (b *readOnlyNodeBackend) CurrentVersionedTransition() *persistencespb.VersionedTransition {
	return transitionhistory.LastVersionedTransition(b.mutableState.GetExecutionInfo().GetTransitionHistory())
}

// Now is the real clock. It ignores any time skipping the execution recorded, which only
// matters to components that are still running, and a detached reader does not observe those.
func (b *readOnlyNodeBackend) Now() time.Time { return b.timeSource.Now() }

// EndpointRegistry returns nil, which Context.EndpointByName reports as an error.
func (b *readOnlyNodeBackend) EndpointRegistry() EndpointRegistry { return nil }

// IsWorkflow reports whether the root is a workflow, as the history service does. A record with
// no CHASM nodes is a workflow, matching how NewTreeFromDB reads one.
func (b *readOnlyNodeBackend) IsWorkflow() bool {
	archetypeID, err := archetypeIDOfNodes(b.mutableState.GetChasmNodes())
	return err == nil && archetypeID == WorkflowArchetypeID
}

// GetNamespaceEntry panics: the record carries only the namespace ID, and no read path a
// detached reader uses needs the entry.
func (b *readOnlyNodeBackend) GetNamespaceEntry() *namespace.Namespace {
	b.unsupported("GetNamespaceEntry")
	return nil
}

// The methods below are reached only while closing a transaction or executing a task.

func (b *readOnlyNodeBackend) GetCurrentVersion() int64 {
	b.unsupported("GetCurrentVersion")
	return 0
}

func (b *readOnlyNodeBackend) ChasmSkipPersistenceEnabled() bool {
	b.unsupported("ChasmSkipPersistenceEnabled")
	return false
}

func (b *readOnlyNodeBackend) SetTimeSkippingConfig(*commonpb.TimeSkippingConfig) {
	b.unsupported("SetTimeSkippingConfig")
}

func (b *readOnlyNodeBackend) RecordTimeSkippingTransition(*TimeSkippingTransition) {
	b.unsupported("RecordTimeSkippingTransition")
}

func (b *readOnlyNodeBackend) ChasmDLQScheduledPureTaskOnValidationEnabled() bool {
	b.unsupported("ChasmDLQScheduledPureTaskOnValidationEnabled")
	return false
}

func (b *readOnlyNodeBackend) NextTransitionCount() int64 {
	b.unsupported("NextTransitionCount")
	return 0
}

func (b *readOnlyNodeBackend) AddChasmSideEffectTask(TaskCategory, PhysicalSideEffectTask) {
	b.unsupported("AddChasmSideEffectTask")
}

func (b *readOnlyNodeBackend) AddChasmPureTask(PhysicalPureTask) {
	b.unsupported("AddChasmPureTask")
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
