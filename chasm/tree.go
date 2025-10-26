package chasm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	protoMessageT = reflect.TypeFor[proto.Message]()
)

var (
	errAccessCheckFailed = serviceerror.NewNotFound("access check failed, CHASM tree is closed for writes")
	errComponentNotFound = serviceerror.NewNotFound("component not found")
	errTaskNotValid      = serviceerror.NewNotFound("task is no longer valid")
)

// valueState is an in-memory indicator of the dirtiness of a deserialized node value.
// The dirtiness has two parts:
//  1. If the data part of the value is in sync with the serializedNode field.
//  2. For component node, if the structure of the component is in sync with the children field.
//
// The enum value below is defined in increasing order of "dirtiness".
// - NeedDeserialize: Value is not even deserialized yet.
// - Synced: Value is deserialized and in sync with both serializedNode and children.
// - NeedSerialize: Value is deserialized, the child tree structure is synced, but the value is not in sync with serializedNode.
// - NeedSyncStructure: Value is deserialized, neither data nor tree structure is synced.
//
// For simplicity, for a dirty component node, the logic always sync structure (potentially multiple times within a transaction) first,
// and the serialize the data at the very end of a transaction. So there will never base a case where value is synced with seralizedNode,
// but not with children.
//
// To update this field, ALWAYS use setValueState() method.
//
// NOTE: This is a different concept from the IsDirty() method which is needed by MutableState implementation to determine
// if the state in memory matches the state in DB.
type valueState uint8

const (
	valueStateUndefined valueState = iota
	valueStateNeedDeserialize
	valueStateSynced
	valueStateNeedSerialize
	valueStateNeedSyncStructure
)

const (
	physicalTaskStatusNone int32 = iota
	physicalTaskStatusCreated
)

type (
	// Node is the in-memory representation of a persisted CHASM node.
	//
	// Node and all its methods are NOT meant to be used by CHASM component authors.
	// They are exported for use by the CHASM engine and underlying MutableState implementation only.
	Node struct {
		*nodeBase

		parent   *Node
		children map[string]*Node // child name (path segment) -> child node
		nodeName string           // key of this node in parent's children map, empty string for root node.

		// Type of attributes controls the type of the node.
		serializedNode *persistencespb.ChasmNode // serialized component | data | collection with metadata
		value          any                       // deserialized component | data | map
		valueState     valueState

		// Cached encoded path for this node.
		// DO NOT read this field directly. Always use getEncodedPath() method to retrieve the encoded path.
		//
		// Empty string is a valid encoded path (for root node), so using *string here to differentiate.
		//
		// TODO: Consider using unique package here.
		// Encoded path for different runs of the same Component type are the same.
		encodedPath *string

		// When terminated is true, regardless of the Lifecycle state of the component,
		// the component will be considered as closed.
		//
		// This right now only applies to the root node and used to update MutableState
		// executionState and executionStatus and trigger retention timers.
		// We could consider extending the force terminate concept to sub-components as well.
		terminated bool
	}

	// nodeBase is a set of dependencies and states shared by all nodes in a CHASM tree.
	nodeBase struct {
		registry    *Registry
		timeSource  clock.TimeSource
		backend     NodeBackend
		pathEncoder NodePathEncoder
		logger      log.Logger

		// Following fields are changes accumulated in this transaction,
		// and will get cleaned up after CloseTransaction().

		// mutation field captures all user state changes (those will be replicated)
		mutation NodesMutation
		// systemMutation field captures all cell specific system changes (those will NOT be replicated)
		systemMutation     NodesMutation
		newTasks           map[any][]taskWithAttributes // component value -> task & attributes
		immediatePureTasks map[any][]taskWithAttributes // similar to newTasks, but will be executed at the end of the transaction

		taskValueCache map[*commonpb.DataBlob]reflect.Value

		// isActiveStateDirty is true if any user data is mutated.
		// NOTE: this only captures active cluster's user data mutation.
		// Replication logic (ApplySnapshot/Mutation) will not set this field.
		//
		// This flag in a CHASM tree level, while valueState is on node level.
		// Tracking this flag on tree level avoids traversing the whole tree every time
		// we want to know if something is updated.
		//
		// This flag is equivalent to checking if any node's valueState >= valueStateNeedSerialize
		isActiveStateDirty bool

		// Root component's search attributes and memo at the start of a transaction.
		// They will be updated upon CloseTransaction() if they are changed.
		currentSA   map[string]VisibilityValue
		currentMemo map[string]VisibilityValue

		needsPointerResolution bool
	}

	taskWithAttributes struct {
		task       any
		attributes TaskAttributes
	}

	// NodesMutation is a set of mutations for all nodes rooted at a given node n,
	// including the node n itself.
	//
	// TODO: Return tree size changes in NodesMutation as well. MutateState needs to
	// track the overall size of itself and terminate workflow if it exceeds the limit.
	NodesMutation struct {
		UpdatedNodes map[string]*persistencespb.ChasmNode // encoded node path -> chasm node
		DeletedNodes map[string]struct{}
	}

	// NodesSnapshot is a snapshot for all nodes rooted at a given node n,
	// including the node n itself.
	NodesSnapshot struct {
		Nodes map[string]*persistencespb.ChasmNode // encoded node path -> chasm node
	}

	// NodeBackend is a set of methods needed from MutableState
	//
	// This is for breaking cycle dependency between
	// this package and service/history/workflow package
	// where MutableState is defined.
	NodeBackend interface {
		// TODO: Add methods needed from MutateState here.
		GetExecutionState() *persistencespb.WorkflowExecutionState
		GetExecutionInfo() *persistencespb.WorkflowExecutionInfo
		GetCurrentVersion() int64
		NextTransitionCount() int64
		CurrentVersionedTransition() *persistencespb.VersionedTransition
		GetWorkflowKey() definition.WorkflowKey
		AddTasks(...tasks.Task)
		UpdateWorkflowStateStatus(
			state enumsspb.WorkflowExecutionState,
			status enumspb.WorkflowExecutionStatus,
		) (bool, error)
		IsWorkflow() bool
		GetNexusCompletion(
			ctx context.Context,
			requestID string,
		) (nexusrpc.OperationCompletion, error)
	}

	// NodePathEncoder is an interface for encoding and decoding node paths.
	// Logic outside the chasm package should only work with encoded paths.
	NodePathEncoder interface {
		Encode(node *Node, path []string) (string, error)
		// TODO: Return a iterator on node name instead of []string,
		// so that we can get a node by encoded path without additional
		// allocation for the decoded path.
		Decode(encodedPath string) ([]string, error)
	}

	// NodePureTask is intended to be implemented and used within the CHASM
	// framework only.
	NodePureTask interface {
		ExecutePureTask(baseCtx context.Context, taskAttributes TaskAttributes, taskInstance any) (bool, error)
		ValidatePureTask(baseCtx context.Context, taskAttributes TaskAttributes, taskInstance any) (bool, error)
	}
)

// NewTree creates a new in-memory CHASM tree from a collection of flattened persistence CHASM nodes.
func NewTree(
	serializedNodes map[string]*persistencespb.ChasmNode, // This is coming from MS map[nodePath]ChasmNode.

	registry *Registry,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
	logger log.Logger,
) (*Node, error) {
	if len(serializedNodes) == 0 {
		return NewEmptyTree(registry, timeSource, backend, pathEncoder, logger), nil
	}

	root := newTreeHelper(registry, timeSource, backend, pathEncoder, logger)
	for encodedPath, serializedNode := range serializedNodes {
		nodePath, err := pathEncoder.Decode(encodedPath)
		if err != nil {
			return nil, err
		}
		root.setSerializedNode(nodePath, encodedPath, serializedNode)
	}

	if err := newTreeInitSearchAttributesAndMemo(root); err != nil {
		return nil, err
	}
	return root, nil
}

// NewEmptyTree creates a new empty in-memory CHASM tree.
func NewEmptyTree(
	registry *Registry,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
	logger log.Logger,
) *Node {
	root := newTreeHelper(registry, timeSource, backend, pathEncoder, logger)

	// If serializedNodes is empty, it means that this new tree.
	// Initialize empty serializedNode.
	root.initSerializedNode(fieldTypeComponent)
	// Although both value and serializedNode.Data are nil, they are considered NOT synced
	// because value has no type and serializedNode does.
	// deserialize method should set value when called.
	root.setValueState(valueStateNeedDeserialize)
	return root
}

func newTreeHelper(
	registry *Registry,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
	logger log.Logger,
) *Node {
	base := &nodeBase{
		registry:    registry,
		timeSource:  timeSource,
		backend:     backend,
		pathEncoder: pathEncoder,
		logger:      logger,

		mutation: NodesMutation{
			UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
			DeletedNodes: make(map[string]struct{}),
		},
		systemMutation: NodesMutation{
			UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
			DeletedNodes: make(map[string]struct{}),
		},
		newTasks:               make(map[any][]taskWithAttributes),
		immediatePureTasks:     make(map[any][]taskWithAttributes),
		taskValueCache:         make(map[*commonpb.DataBlob]reflect.Value),
		needsPointerResolution: false,
	}

	return newNode(base, nil, "")
}

func newTreeInitSearchAttributesAndMemo(
	root *Node,
) error {
	immutableContext := NewContext(context.Background(), root)
	rootComponent, err := root.Component(immutableContext, ComponentRef{})
	if err != nil {
		return err
	}

	// Theoritically we should check if the root node has a Visibility component or not.
	// But that doesn't really matter. Even if it doesn't have one, currentSearchAttributes
	// and currentMemo will just never be used.

	if saProvider, ok := rootComponent.(VisibilitySearchAttributesProvider); ok {
		root.currentSA = saProvider.SearchAttributes(immutableContext)
	}
	if memoProvider, ok := rootComponent.(VisibilityMemoProvider); ok {
		root.currentMemo = memoProvider.Memo(immutableContext)
	}

	return nil
}

func (n *Node) SetRootComponent(
	rootComponent Component,
) {
	root := n.root()
	root.value = rootComponent
	root.setValueState(valueStateNeedSyncStructure)
}

func (n *Node) setValueState(state valueState) {
	n.valueState = state
	if state >= valueStateNeedSerialize {
		n.isActiveStateDirty = true
	}
}

// Component retrieves a component from the tree rooted at node n
// using the provided component reference
// It also performs access rule, and task validation checks
// (for task processing requests) before returning the component.
func (n *Node) Component(
	chasmContext Context,
	ref ComponentRef,
) (Component, error) {
	if ref.entityGoType != nil && ref.archetype == "" {
		rootRC, ok := n.registry.componentOf(ref.entityGoType)
		if !ok {
			return nil, errComponentNotFound
		}
		ref.archetype = Archetype(rootRC.fqType())

	}
	if ref.archetype != "" &&
		n.root().serializedNode.GetMetadata().GetComponentAttributes().Type != ref.archetype.String() {
		return nil, errComponentNotFound
	}

	node, ok := n.findNode(ref.componentPath)
	if !ok {
		return nil, errComponentNotFound
	}

	if ref.componentInitialVT != nil && transitionhistory.Compare(
		ref.componentInitialVT,
		node.serializedNode.Metadata.InitialVersionedTransition,
	) != 0 {
		return nil, errComponentNotFound
	}

	if err := node.prepareComponentValue(chasmContext); err != nil {
		return nil, err
	}

	componentValue, ok := node.value.(Component)
	if !ok {
		return nil, softassert.UnexpectedInternalErr(
			n.logger,
			"component value is not of type Component",
			fmt.Errorf("%s", reflect.TypeOf(node.value).String()))
	}

	// Access check always begins on the target node's parent, and ignored for nodes
	// without ancestors.
	if node.parent != nil {
		err := node.parent.validateAccess(chasmContext)
		if err != nil {
			return nil, err
		}
	}

	if ref.validationFn != nil {
		if err := ref.validationFn(node.root().backend, chasmContext, componentValue); err != nil {
			return nil, err
		}
	}

	return componentValue, nil
}

// validateAccess performs the access rule check on a node.
//
// When the context's intent is OperationIntentProgress, This check validates that
// all of a node's ancestors are still in a running state, and can accept writes. In
// the case of a newly-created node, a detached node, or an OperationIntentObserve
// intent, the check is skipped.
func (n *Node) validateAccess(ctx Context) error {
	intent := operationIntentFromContext(ctx.getContext())
	if intent != OperationIntentProgress {
		// Read-only operations are always allowed.
		return nil
	}

	// TODO - check if this is a detached node, operations are always allowed.

	if n.parent != nil {
		err := n.parent.validateAccess(ctx)
		if err != nil {
			return err
		}
	}

	// Only Component nodes need to be validated.
	if n.serializedNode.Metadata.GetComponentAttributes() == nil {
		return nil
	}

	// Hydrate the component so we can access its LifecycleState.
	err := n.prepareComponentValue(ctx)
	if err != nil {
		return err
	}
	componentValue, _ := n.value.(Component) //nolint:revive // unchecked-type-assertion

	if componentValue.LifecycleState(ctx).IsClosed() {
		return errAccessCheckFailed
	}

	if n.terminated {
		// Terminated nodes can never be written to.
		return errAccessCheckFailed
	}

	return nil
}

func (n *Node) prepareComponentValue(
	chasmContext Context,
) error {
	if n.valueState == valueStateNeedDeserialize {
		metadata := n.serializedNode.Metadata
		componentAttr := metadata.GetComponentAttributes()
		if componentAttr == nil {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"expect chasm node to have ComponentAttributes",
				fmt.Errorf("actual attributes: %v", metadata.Attributes))
		}

		registrableComponent, ok := n.registry.component(componentAttr.GetType())
		if !ok {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"component type name not registered",
				fmt.Errorf("%s", componentAttr.GetType()))
		}

		if err := n.deserialize(registrableComponent.goType); err != nil {
			return fmt.Errorf("failed to deserialize component: %w", err)
		}
	}

	// For now, we assume if a node is accessed with a MutableContext,
	// its value will be mutated and no longer in sync with the serializedNode.
	_, componentCanBeMutated := chasmContext.(MutableContext)
	if componentCanBeMutated {
		n.setValueState(valueStateNeedSyncStructure)
	}

	return nil
}

func (n *Node) prepareDataValue(
	chasmContext Context,
	valueT reflect.Type,
) error {
	metadata := n.serializedNode.Metadata
	dataAttr := metadata.GetDataAttributes()
	if dataAttr == nil {
		return softassert.UnexpectedInternalErr(
			n.logger,
			"expect chasm node to have DataAttributes",
			fmt.Errorf("actual attributes: %v", metadata.Attributes))
	}

	if n.valueState == valueStateNeedDeserialize {
		if err := n.deserialize(valueT); err != nil {
			return fmt.Errorf("failed to deserialize data: %w", err)
		}
	}

	// For now, we assume if a node is accessed with a MutableContext,
	// its value will be mutated and no longer in sync with the serializedNode.
	_, componentCanBeMutated := chasmContext.(MutableContext)
	if componentCanBeMutated {
		n.setValueState(valueStateNeedSerialize)
	}

	return nil
}

func (n *Node) preparePointerValue() error {
	metadata := n.serializedNode.Metadata
	pointerAttr := metadata.GetPointerAttributes()
	if pointerAttr == nil {
		return softassert.UnexpectedInternalErr(
			n.logger,
			"expect chasm node to have PointerAttributes",
			fmt.Errorf("actual attributes: %v", metadata.Attributes))
	}

	if n.valueState == valueStateNeedDeserialize {
		if err := n.deserialize(nil); err != nil {
			return fmt.Errorf("failed to deserialize data: %w", err)
		}
	}

	return nil
}

func (n *Node) isComponent() bool {
	return n.serializedNode.GetMetadata().GetComponentAttributes() != nil
}

func (n *Node) fieldType() fieldType {
	if n.serializedNode.GetMetadata().GetComponentAttributes() != nil {
		return fieldTypeComponent
	}

	if n.serializedNode.GetMetadata().GetDataAttributes() != nil {
		return fieldTypeData
	}

	if n.serializedNode.GetMetadata().GetPointerAttributes() != nil {
		return fieldTypePointer
	}

	if n.serializedNode.GetMetadata().GetCollectionAttributes() != nil {
		softassert.Fail(
			n.logger,
			"fieldType can't be called on Collection node because Collection is not a Field")
	}

	return fieldTypeUnspecified
}

func (n *Node) valueFields() iter.Seq[fieldInfo] {
	return fieldsOf(reflect.ValueOf(n.value))
}

func assertStructPointer(t reflect.Type) error {
	if t == nil {
		return nil
	}

	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return serviceerror.NewInternalf("only pointer to struct is supported for tree node value: got %s", t.String())
	}
	return nil
}

func (n *Node) initSerializedNode(ft fieldType) {
	switch ft {
	case fieldTypeData:
		n.serializedNode = &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          n.backend.NextTransitionCount(),
					NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
				},
				Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
					DataAttributes: &persistencespb.ChasmDataAttributes{},
				},
			},
		}
	case fieldTypeComponent:
		n.serializedNode = &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          n.backend.NextTransitionCount(),
					NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
				},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
				},
			},
		}
	case fieldTypePointer, fieldTypeDeferredPointer:
		// A deferred pointer will be resolved to a regular pointer before persistence.
		n.serializedNode = &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          n.backend.NextTransitionCount(),
					NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
				},
				Attributes: &persistencespb.ChasmNodeMetadata_PointerAttributes{
					PointerAttributes: &persistencespb.ChasmPointerAttributes{},
				},
			},
		}
	case fieldTypeUnspecified:
		softassert.Fail(n.logger,
			"initSerializedNode can't be called with unspecified field type")
	}
}

func (n *Node) initSerializedCollectionNode() {
	n.serializedNode = &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          n.backend.NextTransitionCount(),
				NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
			},
			Attributes: &persistencespb.ChasmNodeMetadata_CollectionAttributes{
				CollectionAttributes: &persistencespb.ChasmCollectionAttributes{},
			},
		},
	}
}

func (n *Node) setSerializedNode(
	nodePath []string,
	encodedPath string,
	serializedNode *persistencespb.ChasmNode,
) *Node {
	if len(nodePath) == 0 {
		n.serializedNode = serializedNode
		n.setValueState(valueStateNeedDeserialize)
		n.encodedPath = &encodedPath
		return n
	}

	childName := nodePath[0]
	childNode, ok := n.children[childName]
	if !ok {
		childNode = newNode(n.nodeBase, n, childName)
		n.children[childName] = childNode
	}
	return childNode.setSerializedNode(nodePath[1:], encodedPath, serializedNode)
}

// serialize sets or updates serializedValue field of the node n with serialized value.
// It sets node's valueState to valueStateSynced and updates LastUpdateVersionedTransition.
func (n *Node) serialize() error {
	switch n.serializedNode.GetMetadata().GetAttributes().(type) {
	case *persistencespb.ChasmNodeMetadata_ComponentAttributes:
		return n.serializeComponentNode()
	case *persistencespb.ChasmNodeMetadata_DataAttributes:
		return n.serializeDataNode()
	case *persistencespb.ChasmNodeMetadata_CollectionAttributes:
		return n.serializeCollectionNode()
	case *persistencespb.ChasmNodeMetadata_PointerAttributes:
		return n.serializePointerNode()
	default:
		return serviceerror.NewInternal("unknown node type")
	}
}

func (n *Node) serializeComponentNode() error {
	for field := range n.valueFields() {
		if field.err != nil {
			return field.err
		}

		if field.kind != fieldKindData {
			continue
		}

		var blob *commonpb.DataBlob
		if !field.val.IsNil() {
			var err error
			if blob, err = serialization.ProtoEncode(field.val.Interface().(proto.Message)); err != nil {
				return err
			}
		}

		rc, ok := n.registry.componentFor(n.value)
		if !ok {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"component type is not registered",
				fmt.Errorf("%s", reflect.TypeOf(n.value).String()))
		}

		n.serializedNode.Data = blob
		n.serializedNode.GetMetadata().GetComponentAttributes().Type = rc.fqType()
		n.updateLastUpdateVersionedTransition()
		n.setValueState(valueStateSynced)

		// continue to iterate over fields to validate that there is only one proto field in the component.
	}
	return nil
}

// syncSubComponents syncs the entire tree recursively (starting from the root node n) from the underlining component value:
//   - Create:
//     -- if child node is nil but subcomponent is not empty or key present in the collection,
//     a new node with subcomponent/collection_item value is created.
//   - Delete:
//     -- if subcomponent is empty, the corresponding child is removed from the tree,
//     -- if subcomponent is no longer in a component, the corresponding child is removed from the tree,
//     -- if collection item is not in the collection, the corresponding child is removed from the tree,
//     -- when a child is removed, all its children are removed too.
//
// All removed paths are added to mutation.DeletedNodes (which is shared between all nodes in the tree).
//
// True is returned when CHASM must perform deferred pointer resolution.
//
// nolint:revive,cognitive-complexity
func (n *Node) syncSubComponents() error {
	if n.valueState < valueStateNeedSyncStructure {
		for _, childNode := range n.children {
			err := childNode.syncSubComponents()
			if err != nil {
				return err
			}
		}
		return nil
	}

	childrenToKeep := make(map[string]struct{})
	for field := range n.valueFields() {
		if field.err != nil {
			return field.err
		}

		switch field.kind {
		case fieldKindUnspecified:
			softassert.Fail(n.logger,
				"field.kind can be unspecified only if err is not nil, and there is a check for it above")
		case fieldKindData:
			// Nothing to sync.
		case fieldKindSubField:
			keepChild, updatedFieldV, err := n.syncSubField(field.val, field.name)
			if err != nil {
				return err
			}
			if updatedFieldV.IsValid() {
				field.val.Set(updatedFieldV)
			}
			if keepChild {
				childrenToKeep[field.name] = struct{}{}
			}
		case fieldKindSubMap:
			if field.val.IsNil() {
				// If Map field is nil then delete all collection items nodes and collection node itself.
				continue
			}

			collectionNode := n.children[field.name]
			if collectionNode == nil {
				collectionNode = newNode(n.nodeBase, n, field.name)
				collectionNode.initSerializedCollectionNode()
				collectionNode.setValueState(valueStateNeedSyncStructure)
				n.children[field.name] = collectionNode
			}

			// Validate map type.
			if field.val.Kind() != reflect.Map {
				return softassert.UnexpectedInternalErr(
					n.logger,
					"CHASM map must be of map type",
					fmt.Errorf("node %s", n.nodeName))
			}

			if len(field.val.MapKeys()) == 0 {
				// If Map field is empty then delete all collection items nodes and collection node itself.
				continue
			}

			mapValT := field.typ.Elem()
			if mapValT.Kind() != reflect.Struct || genericTypePrefix(mapValT) != chasmFieldTypePrefix {
				return softassert.UnexpectedInternalErr(
					n.logger,
					"CHASM map value must be of Field[T] type",
					fmt.Errorf("node %s got %s", n.nodeName, mapValT))
			}

			collectionItemsToKeep := make(map[string]struct{})
			for _, mapKeyV := range field.val.MapKeys() {
				mapItemV := field.val.MapIndex(mapKeyV)
				collectionKey, err := n.mapKeyToString(mapKeyV)
				if err != nil {
					return err
				}
				keepItem, updatedMapItemV, err := collectionNode.syncSubField(mapItemV, collectionKey)
				if err != nil {
					return err
				}
				if updatedMapItemV.IsValid() {
					// The only way to update item in the map is to set it back.
					field.val.SetMapIndex(mapKeyV, updatedMapItemV)
				}
				if keepItem {
					collectionItemsToKeep[collectionKey] = struct{}{}
				}
			}
			if err := collectionNode.deleteChildren(collectionItemsToKeep); err != nil {
				return err
			}
			collectionNode.setValueState(min(valueStateNeedSerialize, collectionNode.valueState))
			childrenToKeep[field.name] = struct{}{}
		}
	}

	err := n.deleteChildren(childrenToKeep)
	n.setValueState(valueStateNeedSerialize)

	return err
}

func (n *Node) mapKeyToString(keyV reflect.Value) (string, error) {
	switch keyV.Kind() {
	case reflect.String:
		return keyV.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(keyV.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(keyV.Uint(), 10), nil
	case reflect.Bool:
		return strconv.FormatBool(keyV.Bool()), nil
	default:
		return "", softassert.UnexpectedInternalErr(
			n.logger,
			"CHASM map key type is not supported",
			fmt.Errorf("node %s must be one of [%s], got %s", n.nodeName, mapKeyTypes, keyV.Type().String()))
	}
}

func (n *Node) stringToMapKey(nodeName string, key string, keyT reflect.Type) (reflect.Value, error) {
	var (
		keyV reflect.Value
		err  error
	)
	switch keyT.Kind() {
	case reflect.String:
		keyV = reflect.ValueOf(key)
	case reflect.Int:
		var x int64
		x, err = strconv.ParseInt(key, 10, 0)
		keyV = reflect.ValueOf(int(x))
	case reflect.Int8:
		var x int64
		x, err = strconv.ParseInt(key, 10, 8)
		keyV = reflect.ValueOf(int8(x))
	case reflect.Int16:
		var x int64
		x, err = strconv.ParseInt(key, 10, 16)
		keyV = reflect.ValueOf(int16(x))
	case reflect.Int32:
		var x int64
		x, err = strconv.ParseInt(key, 10, 32)
		keyV = reflect.ValueOf(int32(x))
	case reflect.Int64:
		var x int64
		x, err = strconv.ParseInt(key, 10, 64)
		keyV = reflect.ValueOf(x)
	case reflect.Uint:
		var x uint64
		x, err = strconv.ParseUint(key, 10, 0)
		keyV = reflect.ValueOf(uint(x))
	case reflect.Uint8:
		var x uint64
		x, err = strconv.ParseUint(key, 10, 8)
		keyV = reflect.ValueOf(uint8(x))
	case reflect.Uint16:
		var x uint64
		x, err = strconv.ParseUint(key, 10, 16)
		keyV = reflect.ValueOf(uint16(x))
	case reflect.Uint32:
		var x uint64
		x, err = strconv.ParseUint(key, 10, 32)
		keyV = reflect.ValueOf(uint32(x))
	case reflect.Uint64:
		var x uint64
		x, err = strconv.ParseUint(key, 10, 64)
		keyV = reflect.ValueOf(x)
	case reflect.Bool:
		var b bool
		b, err = strconv.ParseBool(key)
		keyV = reflect.ValueOf(b)
	default:
		// Use softassert only here because this is the only case that indicates "compile" time error.
		// The other errors below can come from data type mismatch between a component and persisted data.
		err = softassert.UnexpectedInternalErr(
			n.logger,
			"unsupported CHASM map key type",
			fmt.Errorf("unsupported type %s of kind %s: supported key types: %s", keyT.String(), keyT.Kind().String(), mapKeyTypes),
			tag.Error(err))
	}

	if err == nil && !keyV.IsValid() {
		err = fmt.Errorf("value %s is not valid of type %s of kind %s", key, keyT.String(), keyT.Kind().String())
	}

	if err != nil {
		err = softassert.UnexpectedInternalErr(
			n.logger,
			"serialized map key value can't be parsed to CHASM map key type",
			fmt.Errorf("nodeName: %s, key: %s, keyType: %s, error: %s", nodeName, key, keyT.String(), err.Error()))
	}

	return keyV, err
}

// syncSubField syncs node n with value from fieldV parameter.
// If fieldV is a component, then it will sync all subcomponents recursively.
// It returns:
//   - bool keepNode indicates if node needs to be removed from parent's children map.
//   - updatedFieldV if fieldV needs to be updated with new value.
//     If updatedFieldV is invalid, then fieldV doesn't need to be updated.
//     NOTE: this function doesn't update fieldV because it might come from the map which is not addressable.
//   - error.
func (n *Node) syncSubField(
	fieldV reflect.Value,
	fieldN string,
) (
	keepNode bool,
	updatedFieldV reflect.Value,
	err error,
) {
	internalV := fieldV.FieldByName(internalFieldName)
	//nolint:revive // Internal field is guaranteed to be of type fieldInternal.
	internal := internalV.Interface().(fieldInternal)
	if internal.isEmpty() {
		// Internal is empty only when Field was explicitly set to NewEmptyField[T] which is a way to clear its value.
		// In this case, return keepNode=false and this node (and all it children) will be added to DeletedNodes map.
		return
	}
	if internal.node == nil && internal.value() != nil {
		// Field is not empty but tree node is not set. It means this is a new field, and a node must be created.
		childNode := newNode(n.nodeBase, n, fieldN)

		switch internal.fieldType() {
		case fieldTypePointer:
			if _, ok := internal.value().([]string); !ok {
				err = softassert.UnexpectedInternalErr(
					n.logger,
					"value must be of type []string for the field of pointer type",
					fmt.Errorf("got %T", internal.value()))
				return
			}
		case fieldTypeData, fieldTypeComponent:
			if err = assertStructPointer(reflect.TypeOf(internal.value())); err != nil {
				return
			}
		case fieldTypeDeferredPointer:
			// No-op, validation happens when the pointer is resolved.
			n.needsPointerResolution = true
		default:
			err = softassert.UnexpectedInternalErr(
				n.logger,
				"unexpected field type",
				fmt.Errorf("%d", internal.fieldType()))
			return
		}
		childNode.value = internal.value()
		childNode.initSerializedNode(internal.fieldType())
		if internal.fieldType() == fieldTypeComponent {
			childNode.setValueState(valueStateNeedSyncStructure)
		} else {
			childNode.setValueState(valueStateNeedSerialize)
		}

		n.children[fieldN] = childNode
		internal.node = childNode

		updatedFieldV = reflect.New(fieldV.Type()).Elem()
		updatedFieldV.FieldByName(internalFieldName).Set(reflect.ValueOf(internal))
	}
	if internal.fieldType() == fieldTypeComponent && internal.value() != nil {
		err = internal.node.syncSubComponents()
		if err != nil {
			return
		}
	}
	return true, updatedFieldV, nil
}

func (n *Node) deleteChildren(
	childrenToKeep map[string]struct{},
) error {
	for childName, childNode := range n.children {
		if _, childToKeep := childrenToKeep[childName]; !childToKeep {
			if err := childNode.delete(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) serializeDataNode() error {
	protoValue, ok := n.value.(proto.Message)
	if !ok {
		return serviceerror.NewInternal("only support proto.Message as chasm data")
	}

	var blob *commonpb.DataBlob
	if protoValue != nil {
		var err error
		if blob, err = serialization.ProtoEncode(protoValue); err != nil {
			return err
		}
	}
	n.serializedNode.Data = blob
	n.updateLastUpdateVersionedTransition()
	n.setValueState(valueStateSynced)

	return nil
}

func (n *Node) serializeCollectionNode() error {
	// The collection node has no data; therefore, only metadata needs to be updated.
	n.updateLastUpdateVersionedTransition()
	n.setValueState(valueStateSynced)
	return nil
}

// serializePointerNode doesn't serialize anything but named this way for consistency.
func (n *Node) serializePointerNode() error {
	path, isPathValid := n.value.([]string)
	if !isPathValid {
		return softassert.UnexpectedInternalErr(
			n.logger,
			"pointer path is not []string",
			fmt.Errorf("got %T for node %s", n.value, n.nodeName))
	}

	n.serializedNode.GetMetadata().GetPointerAttributes().NodePath = path
	n.updateLastUpdateVersionedTransition()
	n.setValueState(valueStateSynced)

	return nil
}

func (n *Node) updateLastUpdateVersionedTransition() {
	if n.serializedNode.GetMetadata().GetLastUpdateVersionedTransition() == nil {
		n.serializedNode.GetMetadata().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{}
	}
	n.serializedNode.GetMetadata().GetLastUpdateVersionedTransition().TransitionCount = n.backend.NextTransitionCount()
	n.serializedNode.GetMetadata().GetLastUpdateVersionedTransition().NamespaceFailoverVersion = n.backend.GetCurrentVersion()
}

// deserialize initializes the node's value from its serializedNode.
// If a value is of the component type, it initializes every chasm.Field of it and sets serializedNode field but not value field,
// i.e., it doesn't deserialize recursively and must be called on every node separately.
// valueT must be a pointer to a concrete type (not interface). To support deserialization of a component to interface,
// a registry lookup must be done outside the deserialize method.
func (n *Node) deserialize(
	valueT reflect.Type,
) error {
	if err := assertStructPointer(valueT); err != nil {
		return err
	}

	if n.valueState != valueStateNeedDeserialize && reflect.TypeOf(n.value) == valueT {
		return nil
	}

	switch n.serializedNode.GetMetadata().GetAttributes().(type) {
	case *persistencespb.ChasmNodeMetadata_ComponentAttributes:
		return n.deserializeComponentNode(valueT)
	case *persistencespb.ChasmNodeMetadata_DataAttributes:
		return n.deserializeDataNode(valueT)
	case *persistencespb.ChasmNodeMetadata_CollectionAttributes:
		softassert.Fail(
			n.logger,
			"deserialize shouldn't be called on the collection node because it is deserialized with the parent component.")
	case *persistencespb.ChasmNodeMetadata_PointerAttributes:
		return n.deserializePointerNode()
	}
	return nil
}

func (n *Node) deserializeComponentNode(
	valueT reflect.Type,
) error {
	// valueT is guaranteed to be a pointer to the struct because it was already validated by the assertStructPointer method.
	valueV := reflect.New(valueT.Elem())

	for field := range fieldsOf(valueV) {
		if field.err != nil {
			return field.err
		}

		switch field.kind {
		case fieldKindUnspecified:
			softassert.Fail(
				n.logger,
				"field.kind can be unspecified only if err is not nil, and there is a check for it above",
				tag.NewStringTag("node name", n.nodeName))
		case fieldKindData:
			value, err := unmarshalProto(n.serializedNode.GetData(), field.typ)
			if err != nil {
				return err
			}
			field.val.Set(value)
		case fieldKindSubField:
			if childNode, found := n.children[field.name]; found {
				chasmFieldV := reflect.New(field.typ).Elem()
				internalValue := reflect.ValueOf(newFieldInternalWithNode(childNode))
				chasmFieldV.FieldByName(internalFieldName).Set(internalValue)
				field.val.Set(chasmFieldV)
			}
		case fieldKindSubMap:
			if collectionNode, found := n.children[field.name]; found {
				mapFieldV := field.val
				if mapFieldV.IsNil() {
					mapFieldV = reflect.MakeMapWithSize(field.typ, field.val.Len())
					field.val.Set(mapFieldV)
				}

				for collectionItemName, collectionItemNode := range collectionNode.children {
					// field.typ.Elem() is a go type of map item: Field[T]
					chasmFieldV := reflect.New(field.typ.Elem()).Elem()
					internalValue := reflect.ValueOf(newFieldInternalWithNode(collectionItemNode))
					chasmFieldV.FieldByName(internalFieldName).Set(internalValue)
					mapKeyV, err := n.stringToMapKey(field.name, collectionItemName, mapFieldV.Type().Key())
					if err != nil {
						return err
					}
					mapFieldV.SetMapIndex(mapKeyV, chasmFieldV)
				}
			}
		case fieldKindMutableState:
			field.val.Set(reflect.ValueOf(n.backend))
		}
	}

	n.value = valueV.Interface()
	n.setValueState(valueStateSynced)
	return nil
}

func (n *Node) deserializeDataNode(
	valueT reflect.Type,
) error {
	value, err := unmarshalProto(n.serializedNode.GetData(), valueT)
	if err != nil {
		return err
	}

	n.value = value.Interface()
	n.setValueState(valueStateSynced)
	return nil
}

// deserializePointerNode doesn't deserialize anything but named this way for consistency.
func (n *Node) deserializePointerNode() error {
	n.value = n.serializedNode.GetMetadata().GetPointerAttributes().GetNodePath()
	n.setValueState(valueStateSynced)
	return nil
}

func unmarshalProto(
	dataBlob *commonpb.DataBlob,
	valueT reflect.Type,
) (reflect.Value, error) {
	if !valueT.AssignableTo(protoMessageT) {
		return reflect.Value{}, serviceerror.NewInternal("only support proto.Message as chasm data")
	}

	value := reflect.New(valueT.Elem())

	if dataBlob == nil {
		// If the original data is the zero value of its type, the dataBlob loaded from persistence layer will be nil.
		// But we know for component & data nodes, they won't get persisted in the first place if there's no data,
		// so it must be a zero value.
		dataBlob = &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte{},
		}
	}

	if err := serialization.Decode(dataBlob, value.Interface().(proto.Message)); err != nil {
		return reflect.Value{}, err
	}

	return value, nil
}

// Ref implements the CHASM Context interface
func (n *Node) Ref(
	component Component,
) ([]byte, error) {
	// No need to update tree structure here. If a Component can only be found after
	// syncSubComponents() is called, it means the component is created in the
	// current transition and don't have a reference yet.

	for path, node := range n.andAllChildren() {
		if node.value == component {
			workflowKey := node.backend.GetWorkflowKey()
			ref := ComponentRef{
				EntityKey: EntityKey{
					NamespaceID: workflowKey.NamespaceID,
					BusinessID:  workflowKey.WorkflowID,
					EntityID:    workflowKey.RunID,
				},
				archetype: n.Archetype(),
				// TODO: Consider using node's LastUpdateVersionedTransition for checking staleness here.
				// Using VersionedTransition of the entire tree might be too strict.
				entityLastUpdateVT: transitionhistory.CopyVersionedTransition(node.backend.CurrentVersionedTransition()),
				componentPath:      path,
				componentInitialVT: node.serializedNode.GetMetadata().GetInitialVersionedTransition(),
			}
			return ref.Serialize(n.registry)
		}
	}
	return nil, errComponentNotFound
}

// componentNodePath implements the CHASM Context interface
func (n *Node) componentNodePath(
	component Component,
) ([]string, error) {
	// It's unnecessary to deserialize entire tree as calling this method means
	// caller already have the deserialized value.
	for path, node := range n.andAllChildren() {
		if !node.isComponent() {
			continue
		}

		if node.value == component {
			return path, nil
		}
	}
	return nil, errComponentNotFound
}

// dataNodePath implements the CHASM Context interface
func (n *Node) dataNodePath(
	data proto.Message,
) ([]string, error) {
	// It's unnecessary to deserialize entire tree as calling this method means
	// caller already have the deserialized value.
	for path, node := range n.andAllChildren() {
		if node.serializedNode.GetMetadata().GetDataAttributes() == nil {
			continue
		}

		if node.value == data {
			return path, nil
		}
	}
	return nil, errComponentNotFound
}

// Now implements the CHASM Context interface
func (n *Node) Now(
	_ Component,
) time.Time {
	// TODO: Now() could be different for components after we support Pause for CHASM components.
	return n.timeSource.Now()
}

// AddTask implements the CHASM MutableContext interface
func (n *Node) AddTask(
	component Component,
	taskAttributes TaskAttributes,
	task any,
) {
	rt, ok := n.registry.taskFor(task)
	// TODO: remove the task type check after scheduler unit tests are fixed.
	if ok && rt.isPureTask && taskAttributes.IsImmediate() && rt.fqType() == "TestLibrary.test_pure_task" {
		// Those tasks will be executed in the current transaction.
		n.immediatePureTasks[component] = append(n.immediatePureTasks[component], taskWithAttributes{
			task:       task,
			attributes: taskAttributes,
		})
		return
	}

	n.nodeBase.newTasks[component] = append(n.nodeBase.newTasks[component], taskWithAttributes{
		task:       task,
		attributes: taskAttributes,
	})
}

// CloseTransaction is used by MutableState to close the transaction and
// track changes made in the current transaction.
func (n *Node) CloseTransaction() (NodesMutation, error) {
	defer n.cleanupTransaction()

	if err := n.executeImmediatePureTasks(); err != nil {
		return NodesMutation{}, err
	}

	if err := n.syncSubComponents(); err != nil {
		return NodesMutation{}, err
	}

	if n.needsPointerResolution {
		if err := n.resolveDeferredPointers(); err != nil {
			return NodesMutation{}, err
		}
	}

	nextVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
		TransitionCount:          n.backend.NextTransitionCount(),
	}

	rootLifecycleChanged, err := n.closeTransactionHandleRootLifecycleChange()
	if err != nil {
		return NodesMutation{}, err
	}

	if n.isActiveStateDirty {
		if err := n.closeTransactionForceUpdateVisibility(rootLifecycleChanged); err != nil {
			return NodesMutation{}, err
		}
	}

	if err := n.closeTransactionSerializeNodes(); err != nil {
		return NodesMutation{}, err
	}

	if err := n.closeTransactionUpdateComponentTasks(nextVersionedTransition); err != nil {
		return NodesMutation{}, err
	}

	// Both user & system data mutation need to be returned and persisted.
	maps.Copy(n.mutation.UpdatedNodes, n.systemMutation.UpdatedNodes)
	maps.Copy(n.mutation.DeletedNodes, n.systemMutation.DeletedNodes)

	return n.mutation, nil
}

func (n *Node) executeImmediatePureTasks() error {

	if len(n.immediatePureTasks) == 0 {
		return nil
	}

	// We must sync structure before running any tasks here because,
	// those tasks might be for a newly created component which doesn't even have a node yet.
	// And we want to make sure we only run tasks for components that are still part of the tree.
	if err := n.syncSubComponents(); err != nil {
		return err
	}

	// TODO: Maintain a mapping from deserialized component value to node
	// and avoid this look up.
	componentValueToNode := make(map[any]*Node)
	for _, node := range n.andAllChildren() {
		if !node.isComponent() {
			continue
		}
		if _, ok := n.immediatePureTasks[node.value]; ok {
			componentValueToNode[node.value] = node
		}
	}

	for componentValue, componentTasks := range n.immediatePureTasks {

		taskNode := componentValueToNode[componentValue]
		if taskNode == nil || len(componentTasks) == 0 {
			// NOTE: taskNode being nil is not necessarily an error because this function is executed at the end of a transaction
			// which could contain multiple transitions. So it's possible that a task added for a component in one
			// transition and in a later transition that component get removed.
			continue
		}

		// TODO: sync structure after every task execution instead of once per node to handle the case
		// where a task deletes the component it is executing on.
		//
		// This is possible if component as a pointer to it's ancestors and that ancestor deletes this component.
		// For example, a child activity fires a timeout timer, it notifies the parent node which is a workflow,
		// and workflow deletes the activity from it's activities map.
		syncStructure := true
		for _, task := range componentTasks {
			executed, err := taskNode.ExecutePureTask(context.Background(), task.attributes, task.task)
			if err != nil {
				return err
			}
			syncStructure = syncStructure || executed
		}

		if syncStructure {
			if err := n.syncSubComponents(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Node) closeTransactionHandleRootLifecycleChange() (bool, error) {
	if n.backend.IsWorkflow() {
		// Workflow manages its lifecycle directly in mutable state.
		return false, nil
	}

	if n.valueState != valueStateNeedSerialize {
		return false, nil
	}

	if n.terminated {
		return n.backend.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
	}

	chasmContext := NewContext(context.Background(), n)
	rootComponent, err := n.Component(chasmContext, ComponentRef{})
	if err != nil {
		return false, err
	}
	lifecycleState := rootComponent.LifecycleState(chasmContext)

	var newState enumsspb.WorkflowExecutionState
	var newStatus enumspb.WorkflowExecutionStatus
	switch lifecycleState {
	case LifecycleStateRunning:
		newState = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
		newStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	case LifecycleStateCompleted:
		newState = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		newStatus = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	case LifecycleStateFailed:
		newState = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		newStatus = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	default:
		return false, softassert.UnexpectedInternalErr(
			n.logger,
			"unknown component lifecycle state",
			fmt.Errorf("%v", lifecycleState))
	}

	return n.backend.UpdateWorkflowStateStatus(newState, newStatus)
}

func (n *Node) closeTransactionForceUpdateVisibility(
	rootLifecycleChanged bool,
) error {

	immutableContext := NewContext(context.TODO(), n)
	needUpdate := rootLifecycleChanged

	rootComponent, err := n.Component(immutableContext, ComponentRef{})
	if err != nil {
		return err
	}

	saProvider, ok := rootComponent.(VisibilitySearchAttributesProvider)
	if ok {
		newSA := saProvider.SearchAttributes(immutableContext)
		if !maps.EqualFunc(n.currentSA, newSA, isVisibilityValueEqual) {
			needUpdate = true
		}
		n.currentSA = newSA
	}

	memoProvider, ok := rootComponent.(VisibilityMemoProvider)
	if ok {
		newMemo := memoProvider.Memo(immutableContext)
		if !maps.EqualFunc(n.currentMemo, newMemo, isVisibilityValueEqual) {
			needUpdate = true
		}
		n.currentMemo = newMemo
	}

	if !needUpdate {
		return nil
	}

	var visibilityNode *Node
	for _, child := range n.children {
		if !child.isComponent() {
			continue
		}

		if child.valueState == valueStateNeedSerialize {
			if rc, ok := n.registry.componentFor(child.value); ok && rc.fqType() == visibilityComponentFqType {
				visibilityNode = child
				break
			}
		} else if child.serializedNode.Metadata.GetComponentAttributes().Type == visibilityComponentFqType {
			visibilityNode = child
			break
		}
	}

	if visibilityNode == nil {
		return nil
	}

	visComponent, err := visibilityNode.Component(immutableContext, ComponentRef{})
	if err != nil {
		return err
	}

	visibility, ok := visComponent.(*Visibility)
	if !ok {
		return softassert.UnexpectedInternalErr(
			n.logger,
			"expected visibility component for component type",
			fmt.Errorf("type: %s, but got %T", visibilityComponentFqType, visComponent))
	}

	// Generate a task and mark the node as dirty.
	//
	// NOTE: generateTask() will create a new logical task for the visibility component. But it also
	// invalidates all previous logical tasks at the end of the transaction, and only one physical task
	// will be created in the visibility queue.
	mutableContext := NewMutableContext(context.TODO(), n)
	visibility.generateTask(mutableContext)
	visibilityNode.setValueState(valueStateNeedSerialize)

	// We don't need to sync tree structure here for the visiblity node because we only generated a task without
	// changing any component fields.
	return nil
}

func (n *Node) closeTransactionSerializeNodes() error {
	for nodePath, node := range n.andAllChildren() {
		if node.valueState > valueStateNeedSerialize {
			return serviceerror.NewInternalf("invalid valueState for serializing: %v", node.valueState)
		}

		if node.valueState < valueStateNeedSerialize {
			continue
		}

		if err := node.serialize(); err != nil {
			return err
		}

		if componentAttr := node.serializedNode.GetMetadata().GetComponentAttributes(); componentAttr != nil &&
			componentAttr.Type == visibilityComponentFqType &&
			len(nodePath) != 1 {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"CHASM visibility component must be immediate child of the root node",
				fmt.Errorf("found at path %s", nodePath))
		}

		encodedPath, err := node.getEncodedPath()
		if err != nil {
			return err
		}
		n.mutation.UpdatedNodes[encodedPath] = node.serializedNode
		// DeletedNodes map is populated when syncing tree structure. However, since we may sync tree structure
		// multiple times in one transaction, if node at the same path was previously deleted, have structure synced,
		// then get re-created, the same encoded path will exists in both UpdatedNodes and DeletedNodes maps.
		//
		// serializeNode only happens once at the end of a transaction, and here we know the node at this encoded path exists,
		// remove it from the DeletedNodes map.
		delete(n.mutation.DeletedNodes, encodedPath)
	}

	return nil
}

func (n *Node) closeTransactionUpdateComponentTasks(
	nextVersionedTransition *persistencespb.VersionedTransition,
) error {
	taskOffset := int64(1)
	validateContext := NewContext(context.Background(), n)

	var firstPureTask *persistencespb.ChasmComponentAttributes_Task
	var firstPureTaskNode *Node

	for nodePath, node := range n.andAllChildren() {
		// no-op if node is not a component
		componentAttr := node.serializedNode.Metadata.GetComponentAttributes()
		if componentAttr == nil {
			continue
		}

		// First update component logical tasks.
		//
		// The conditions excludes replication logic (applyMutation/Snapshot) which sets
		// valueState to valueStateNeedDeserialize.
		//
		// Do NOT use condition node.valueState == valueStateNeedSerialize.
		// This method is called after the closeTransactionSerializeNodes which sets valueState
		// to valueStateSynced.
		if transitionhistory.Compare(
			node.serializedNode.GetMetadata().LastUpdateVersionedTransition,
			nextVersionedTransition,
		) == 0 && node.valueState != valueStateNeedDeserialize {
			if err := node.closeTransactionCleanupInvalidTasks(validateContext); err != nil {
				return err
			}

			if err := node.closeTransactionHandleNewTasks(
				nextVersionedTransition,
				validateContext,
				&taskOffset,
			); err != nil {
				return err
			}
		}

		sideEffectTasks := componentAttr.GetSideEffectTasks()
		for idx := len(sideEffectTasks) - 1; idx >= 0; idx-- {
			sideEffectTask := sideEffectTasks[idx]
			if sideEffectTask.PhysicalTaskStatus == physicalTaskStatusCreated {
				break
			}

			node.closeTransactionGeneratePhysicalSideEffectTask(
				sideEffectTask,
				nodePath,
			)
		}

		// Find the first pure task in the entire tree,
		// regardless if the pure task is newly added or existing.
		pureTasks := componentAttr.GetPureTasks()
		if len(pureTasks) == 0 {
			continue
		}

		if firstPureTask == nil ||
			comparePureTasks(pureTasks[0], firstPureTask) < 0 {
			firstPureTask = pureTasks[0]
			firstPureTaskNode = node
		}
	}

	// TODO: We cannot simply assert that all tasks in n.nodeBase.newTasks are processed.
	// That should be the case when only one transition for each transaction.
	// However, when processing pure tasks, we run multiple pure tasks, thus multiple transitions
	// in one transaction. This means it's possible that task generated for a component in the first
	// task, and that component get deleted by the second task.

	return n.closeTransactionGeneratePhysicalPureTask(
		firstPureTask,
		firstPureTaskNode,
	)
}

func (n *Node) deserializeComponentTask(
	componentTask *persistencespb.ChasmComponentAttributes_Task,
) (any, error) {
	registableTask, ok := n.registry.task(componentTask.Type)
	if !ok {
		return nil, softassert.UnexpectedInternalErr(
			n.logger,
			"task type is not registered",
			fmt.Errorf("%s", componentTask.Type))
	}

	taskValue, err := n.deserializeTaskWithCache(registableTask, componentTask.Data)
	if err != nil {
		return nil, err
	}

	return taskValue.Interface(), nil
}

// validateTask runs taskInstance's registered validation handler.
func (n *Node) validateTask(
	validateContext Context,
	taskAttributes TaskAttributes,
	taskInstance any,
) (bool, error) {
	registableTask, ok := n.registry.taskFor(taskInstance)
	if !ok {
		return false, softassert.UnexpectedInternalErr(
			n.logger,
			"task type for goType is not registered",
			fmt.Errorf("%s", reflect.TypeOf(taskInstance).Name()))
	}

	retValues := registableTask.validateFn.Call([]reflect.Value{
		reflect.ValueOf(validateContext),
		reflect.ValueOf(n.value),
		reflect.ValueOf(taskAttributes),
		reflect.ValueOf(taskInstance),
	})
	if !retValues[1].IsNil() {
		//revive:disable-next-line:unchecked-type-assertion
		return false, retValues[1].Interface().(error)
	}
	//revive:disable-next-line:unchecked-type-assertion
	return retValues[0].Interface().(bool), nil
}

func (n *Node) closeTransactionCleanupInvalidTasks(
	validateContext Context,
) error {
	// Validate existing tasks and remove invalid ones.
	var validationErr error
	deleteFunc := func(existingTask *persistencespb.ChasmComponentAttributes_Task) bool {
		existingTaskInstance, err := n.deserializeComponentTask(existingTask)
		if err != nil {
			validationErr = err
			return false
		}

		valid, err := n.validateTask(
			validateContext,
			TaskAttributes{
				ScheduledTime: existingTask.ScheduledTime.AsTime(),
				Destination:   existingTask.Destination,
			},
			existingTaskInstance,
		)
		if err != nil {
			validationErr = err
			return false
		}
		if !valid {
			delete(n.taskValueCache, existingTask.Data)
		}
		return !valid
	}

	componentAttr := n.serializedNode.Metadata.GetComponentAttributes()
	componentAttr.SideEffectTasks = slices.DeleteFunc(componentAttr.SideEffectTasks, deleteFunc)
	if validationErr != nil {
		return validationErr
	}
	componentAttr.PureTasks = slices.DeleteFunc(componentAttr.PureTasks, deleteFunc)
	if validationErr != nil {
		return validationErr
	}
	return nil
}

func (n *Node) closeTransactionHandleNewTasks(
	nextVersionedTransition *persistencespb.VersionedTransition,
	validateContext Context,
	taskOffset *int64,
) error {
	newTasks, ok := n.newTasks[n.value]
	if !ok {
		return nil
	}

	componentAttr := n.serializedNode.Metadata.GetComponentAttributes()
	sortPureTasks := false

	for _, newTask := range newTasks {
		if !newTask.attributes.IsValid() {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"task attributes cannot have both destination and scheduled specified",
				fmt.Errorf("attributes: %v", newTask.attributes))
		}

		valid, err := n.validateTask(
			validateContext,
			newTask.attributes,
			newTask.task,
		)
		if err != nil {
			return err
		}
		if !valid {
			continue
		}

		registrableTask, ok := n.registry.taskFor(newTask.task)
		if !ok {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"task type is not registered",
				fmt.Errorf("%s", reflect.TypeOf(newTask.task).String()))
		}

		taskBlob, err := n.serializeTaskWithCache(registrableTask, reflect.ValueOf(newTask.task))
		if err != nil {
			return err
		}

		componentTask := &persistencespb.ChasmComponentAttributes_Task{
			Type:                      registrableTask.fqType(),
			Destination:               newTask.attributes.Destination,
			ScheduledTime:             timestamppb.New(newTask.attributes.ScheduledTime),
			Data:                      taskBlob,
			VersionedTransition:       nextVersionedTransition,
			VersionedTransitionOffset: *taskOffset,
			PhysicalTaskStatus:        physicalTaskStatusNone,
		}

		if registrableTask.isPureTask {
			componentAttr.PureTasks = append(componentAttr.PureTasks, componentTask)
			sortPureTasks = true
		} else {
			componentAttr.SideEffectTasks = append(componentAttr.SideEffectTasks, componentTask)
		}

		*taskOffset++
	}

	if sortPureTasks {
		// pure tasks are sorted by scheduled time.
		slices.SortFunc(componentAttr.PureTasks, comparePureTasks)
	}

	return nil
}

func (n *Node) closeTransactionGeneratePhysicalSideEffectTask(
	sideEffectTask *persistencespb.ChasmComponentAttributes_Task,
	nodePath []string,
) {
	n.backend.AddTasks(&tasks.ChasmTask{
		WorkflowKey:         n.backend.GetWorkflowKey(),
		VisibilityTimestamp: sideEffectTask.ScheduledTime.AsTime(),
		Destination:         sideEffectTask.Destination,
		Category:            taskCategory(sideEffectTask),
		Info: &persistencespb.ChasmTaskInfo{
			ComponentInitialVersionedTransition:    n.serializedNode.Metadata.InitialVersionedTransition,
			ComponentLastUpdateVersionedTransition: n.serializedNode.Metadata.LastUpdateVersionedTransition,
			Path:                                   nodePath,
			Type:                                   sideEffectTask.Type,
			Data:                                   sideEffectTask.Data,
		},
	})
	sideEffectTask.PhysicalTaskStatus = physicalTaskStatusCreated
}

func (n *Node) closeTransactionGeneratePhysicalPureTask(
	firstPureTask *persistencespb.ChasmComponentAttributes_Task,
	firstTaskNode *Node,
) error {
	if firstPureTask == nil || firstPureTask.PhysicalTaskStatus == physicalTaskStatusCreated {
		return nil
	}

	n.backend.AddTasks(&tasks.ChasmTaskPure{
		WorkflowKey:         n.backend.GetWorkflowKey(),
		VisibilityTimestamp: firstPureTask.ScheduledTime.AsTime(),
		Category:            tasks.CategoryTimer,
	})

	// We need to persist the task status change as well, so add the node
	// to the list of updated nodes.
	// However, since task status is a cluster local field, we don't really
	// update LastUpdateVersionedTransition for this node, and the change won't be replicated.
	firstPureTask.PhysicalTaskStatus = physicalTaskStatusCreated
	encodedPath, err := firstTaskNode.getEncodedPath()
	if err != nil {
		return err
	}
	n.systemMutation.UpdatedNodes[encodedPath] = firstTaskNode.serializedNode
	return nil
}

// resolveDeferredPointers resolves all deferred pointers in the tree.
// Returns error if any deferred pointer cannot be resolved, as deferred pointers
// cannot be persisted after transaction close.
func (n *Node) resolveDeferredPointers() error {
	for _, node := range n.andAllChildren() {
		if node.value == nil || !node.isComponent() {
			continue
		}

		for field := range node.valueFields() {
			if field.err != nil {
				return field.err
			}

			if field.kind != fieldKindSubField {
				continue
			}

			internalV := field.val.FieldByName(internalFieldName)
			internal, _ := internalV.Interface().(fieldInternal) //nolint:revive

			if internal.fieldType() == fieldTypeDeferredPointer && internal.value() != nil {
				// Must resolve the deferred pointer or fail the transaction.
				var resolvedPath []string
				var err error

				switch value := internal.value().(type) {
				case Component:
					resolvedPath, err = n.componentNodePath(value)
				case proto.Message:
					resolvedPath, err = n.dataNodePath(value)
				default:
					err = softassert.UnexpectedInternalErr(
						n.logger,
						"unable to create a deferred pointer for values of type",
						fmt.Errorf("%T", value))
				}
				if err != nil {
					return softassert.UnexpectedInternalErr(
						n.logger,
						"failed to resolve deferred pointer during transaction close",
						err)
				}

				// Update the field to be a regular pointer, reusing the existing serializedNode,
				// and update the serializedNode's value.
				newInternal := newFieldInternalWithValue(fieldTypePointer, resolvedPath)
				newInternal.node = internal.node
				newInternal.node.value = resolvedPath
				internalV.Set(reflect.ValueOf(newInternal))
			}
		}
	}
	return nil
}

// andAllChildren returns a sequence of all nodes in the tree starting from n, including n itself.
// The sequence is depth-first, pre-order traversal.
func (n *Node) andAllChildren() iter.Seq2[[]string, *Node] {
	return func(yield func([]string, *Node) bool) {
		var walk func([]string, *Node) bool
		walk = func(path []string, node *Node) bool {
			if node == nil {
				return true
			}
			if !yield(path, node) {
				return false
			}
			for _, child := range node.children {
				if !walk(append(path, child.nodeName), child) {
					return false
				}
			}
			return true
		}
		walk(nil, n)
	}
}

func (n *Node) cleanupTransaction() {
	n.mutation = NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}

	// System mutation are most likely to be empty, so we reuse existing ones if possible.
	if len(n.systemMutation.UpdatedNodes) != 0 {
		n.systemMutation.UpdatedNodes = make(map[string]*persistencespb.ChasmNode)
	}
	if len(n.systemMutation.DeletedNodes) != 0 {
		n.systemMutation.DeletedNodes = make(map[string]struct{})
	}

	n.newTasks = make(map[any][]taskWithAttributes)

	n.isActiveStateDirty = false
	n.immediatePureTasks = make(map[any][]taskWithAttributes)

	n.needsPointerResolution = false
}

// Snapshot returns all nodes in the tree that have been modified after the given min versioned transition.
// A nil exclusiveMinVT will be treated as the same as the zero versioned transition and returns all nodes in the tree.
// This method should only be invoked on root CHASM node when IsDirty() is false.
func (n *Node) Snapshot(
	exclusiveMinVT *persistencespb.VersionedTransition,
) NodesSnapshot {
	if !softassert.That(n.logger, n.parent == nil, "chasm.Snapshot() should only be called on the root node") {
		panic(fmt.Sprintf("chasm.Snapshot() called on child node: %+v", n))
	}

	// TODO: add assertion on IsDirty() once implemented

	nodes := make(map[string]*persistencespb.ChasmNode)
	n.snapshotInternal(exclusiveMinVT, nodes)

	return NodesSnapshot{
		Nodes: nodes,
	}
}

func (n *Node) snapshotInternal(
	exclusiveMinVT *persistencespb.VersionedTransition,
	nodes map[string]*persistencespb.ChasmNode,
) {
	if n == nil {
		return
	}

	if transitionhistory.Compare(n.serializedNode.Metadata.LastUpdateVersionedTransition, exclusiveMinVT) > 0 {
		encodedPath, err := n.getEncodedPath()
		if !softassert.That(n.logger, err == nil, "chasm path encoding should always succeed on clean tree") {
			panic(fmt.Sprintf("failed to encode chasm path on clean tree: %v", err))
		}
		nodes[encodedPath] = n.serializedNode
	}

	for _, childNode := range n.children {
		childNode.snapshotInternal(
			exclusiveMinVT,
			nodes,
		)
	}
}

// ApplyMutation is used by replication stack to apply node
// mutations from the source cluster.
//
// NOTE: It will be an error if UpdatedNodes and DeletedNodes have overlapping keys,
// as the CHASM tree does not have enough information to tell if the deletion happens
// before or after the update.
func (n *Node) ApplyMutation(
	mutation NodesMutation,
) error {
	if err := n.applyDeletions(mutation.DeletedNodes); err != nil {
		return err
	}

	if err := n.applyUpdates(mutation.UpdatedNodes); err != nil {
		return err
	}

	// For replication case, we only update the search attributes and memo
	// but not force updating the visibility component itself to generate a task.
	//
	// This is because the visibility component is already force updated in the active
	// cluster and that forced update will be replicated as well. Standby cluster
	// only needs to track the current SA and memo to prevent generating an unnecessary
	// visibility component update & task if there is a failover.
	//
	// TODO: combine this with the logic in CloseTransactionForceUpdateVisibility
	// right that force update logic only applies to the active cluster.
	immutableContext := NewContext(context.Background(), n)
	rootComponent, err := n.root().Component(immutableContext, ComponentRef{})
	if err != nil {
		return err
	}
	saProvider, ok := rootComponent.(VisibilitySearchAttributesProvider)
	if ok {
		n.currentSA = saProvider.SearchAttributes(immutableContext)
	}
	memoProvider, ok := rootComponent.(VisibilityMemoProvider)
	if ok {
		n.currentMemo = memoProvider.Memo(immutableContext)
	}

	return nil
}

// ApplySnapshot is used by replication stack to apply node
// snapshot from the source cluster.
//
// If we simply substituting the entire CHASM tree, we will be
// forced to close the transaction as snapshot and potentially
// write extra data to persistence.
// This method will instead figure out the mutations needed to
// bring the current tree to the be the same as the snapshot,
// thus allowing us to close the transaction as mutation.
func (n *Node) ApplySnapshot(
	incomingSnapshot NodesSnapshot,
) error {
	currentSnapshot := n.Snapshot(nil)

	mutation := NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}

	for encodedPath := range currentSnapshot.Nodes {
		if _, ok := incomingSnapshot.Nodes[encodedPath]; !ok {
			mutation.DeletedNodes[encodedPath] = struct{}{}
		}
	}

	for encodedPath, incomingNode := range incomingSnapshot.Nodes {
		currentNode, ok := currentSnapshot.Nodes[encodedPath]
		if !ok {
			mutation.UpdatedNodes[encodedPath] = incomingNode
			continue
		}

		if transitionhistory.Compare(
			currentNode.Metadata.LastUpdateVersionedTransition,
			incomingNode.Metadata.LastUpdateVersionedTransition,
		) != 0 {
			mutation.UpdatedNodes[encodedPath] = incomingNode
		}
	}

	return n.ApplyMutation(mutation)
}

func (n *Node) applyDeletions(
	deletedNodes map[string]struct{},
) error {
	for encodedPath := range deletedNodes {
		path, err := n.pathEncoder.Decode(encodedPath)
		if err != nil {
			return err
		}

		node, ok := n.findNode(path)
		if !ok {
			// Already deleted.
			// This could happen when:
			// - If the mutations passed in include changes
			// older than the current state of the tree.
			// - We are already applied the deletion on a parent node.
			continue
		}

		if err := node.delete(); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) applyUpdates(
	updatedNodes map[string]*persistencespb.ChasmNode,
) error {
	for encodedPath, updatedNode := range updatedNodes {
		path, err := n.pathEncoder.Decode(encodedPath)
		if err != nil {
			return err
		}

		node, ok := n.findNode(path)
		if !ok {
			// Node doesn't exist, we need to create it.
			newNode := n.setSerializedNode(path, encodedPath, updatedNode)
			newNode.resetTaskStatus()
			n.mutation.UpdatedNodes[encodedPath] = newNode.serializedNode
			continue
		}

		// An empty node may be created when child update is applied before the parent,
		// in which case node.serializedNode will be nil.
		if node.serializedNode == nil || transitionhistory.Compare(
			node.serializedNode.Metadata.LastUpdateVersionedTransition,
			updatedNode.Metadata.LastUpdateVersionedTransition,
		) != 0 {
			localComponentAttr := node.serializedNode.GetMetadata().GetComponentAttributes()
			updatedComponentAttr := updatedNode.GetMetadata().GetComponentAttributes()
			if localComponentAttr != nil && updatedComponentAttr != nil {
				n.carryOverTaskStatus(
					localComponentAttr.SideEffectTasks,
					updatedComponentAttr.SideEffectTasks,
					compareSideEffectTasks,
				)
				n.carryOverTaskStatus(
					localComponentAttr.PureTasks,
					updatedComponentAttr.PureTasks,
					comparePureTasks,
				)
			}

			n.mutation.UpdatedNodes[encodedPath] = updatedNode
			node.serializedNode = updatedNode
			node.value = nil
			node.setValueState(valueStateNeedDeserialize)

			// Clearing decoded value for ancestor nodes is not necessary because the value field is not referenced directly.
			// Parent node is pointing to the Node struct.
		}
	}

	return nil
}

func (n *Node) RefreshTasks() error {
	for _, node := range n.andAllChildren() {
		// Only reset task status here, the actual task generation will be done when
		// CloseTransaction() is called to persist the changes.
		if reset := node.resetTaskStatus(); !reset {
			continue
		}

		encodedPath, err := node.getEncodedPath()
		if err != nil {
			return err
		}

		// Task status is a cluster local field and changes to it doesn't need to be replicated.
		// Recording changes in systemMutation so that:
		// 1. it can be persisted.
		// 2. n.IsStateDirty() can still return false so that mutable state's transition history
		// won't be updated.
		n.systemMutation.UpdatedNodes[encodedPath] = node.serializedNode
	}

	return nil
}

func (n *Node) resetTaskStatus() bool {
	if n.serializedNode == nil || n.serializedNode.GetMetadata() == nil {
		return false
	}

	componentAttr := n.serializedNode.GetMetadata().GetComponentAttributes()
	if componentAttr == nil {
		return false
	}

	reset := false
	for _, componentTasks := range [][]*persistencespb.ChasmComponentAttributes_Task{
		componentAttr.PureTasks,
		componentAttr.SideEffectTasks,
	} {
		for _, t := range componentTasks {
			if !reset && t.PhysicalTaskStatus == physicalTaskStatusCreated {
				reset = true
			}
			t.PhysicalTaskStatus = physicalTaskStatusNone
		}
	}

	return reset
}

func (n *Node) getEncodedPath() (string, error) {
	if n.encodedPath != nil {
		return *n.encodedPath, nil
	}
	encodePath, err := n.pathEncoder.Encode(n, n.path())
	if err == nil {
		n.encodedPath = &encodePath
	}
	return encodePath, err
}

func (n *Node) path() []string {
	if n.parent == nil {
		return []string{}
	}

	return append(n.parent.path(), n.nodeName)
}

func (n *Node) findNode(
	path []string,
) (*Node, bool) {
	if len(path) == 0 {
		return n, true
	}

	childName := path[0]
	childNode, ok := n.children[childName]
	if !ok {
		return nil, false
	}
	return childNode.findNode(path[1:])
}

func (n *Node) delete() error {
	for _, childNode := range n.children {
		if err := childNode.delete(); err != nil {
			return err
		}
	}

	// If a parent is about to be removed, it must not have any children.
	softassert.That(n.logger, len(n.children) == 0, "children must be empty when node is removed")

	if n.parent != nil {
		delete(n.parent.children, n.nodeName)
	}

	encodedPath, err := n.getEncodedPath()
	if err != nil {
		return err
	}

	n.mutation.DeletedNodes[encodedPath] = struct{}{}

	n.cleanupCachedTasks()

	return nil
}

func (n *Node) cleanupCachedTasks() {
	if !n.isComponent() {
		return
	}

	componentAttr := n.serializedNode.GetMetadata().GetComponentAttributes()
	for _, task := range componentAttr.GetPureTasks() {
		delete(n.taskValueCache, task.Data)
	}
	for _, task := range componentAttr.GetSideEffectTasks() {
		delete(n.taskValueCache, task.Data)
	}
}

// IsDirty returns true if any node in the tree has been modified,
// and need to be persisted in DB.
// The result will be reset to false after a call to CloseTransaction().
func (n *Node) IsDirty() bool {
	if n.IsStateDirty() {
		return true
	}

	return len(n.systemMutation.UpdatedNodes) > 0 || len(n.systemMutation.DeletedNodes) > 0
}

// IsStateDirty returns true if any node in the tree has USER DATA modified,
// which need to be persisted to DB AND replicated to other clusters.
// The result will be reset to false after a call to CloseTransaction().
func (n *Node) IsStateDirty() bool {
	return n.isActiveStateDirty || len(n.mutation.UpdatedNodes) > 0 || len(n.mutation.DeletedNodes) > 0
}

func (n *Node) IsStale(
	ref ComponentRef,
) error {
	// The point of this method to access the private entityLastUpdateVT field in componentRef,
	// and avoid exposing it in the public CHASM interface.
	if ref.entityLastUpdateVT == nil {
		return nil
	}

	return transitionhistory.StalenessCheck(
		n.backend.GetExecutionInfo().TransitionHistory,
		ref.entityLastUpdateVT,
	)
}

func (n *Node) Terminate(
	request TerminateComponentRequest,
) error {
	mutableContext := NewMutableContext(context.Background(), n.root())
	component, err := n.Component(mutableContext, ComponentRef{})
	if err != nil {
		return err
	}

	_, err = component.Terminate(mutableContext, request)
	if err != nil {
		return err
	}

	n.terminated = true
	return nil
}

func (n *Node) Archetype() Archetype {
	root := n.root()
	if root.serializedNode == nil {
		// Empty tree
		return ""
	}

	// Root must have be a component.
	return Archetype(root.serializedNode.Metadata.GetComponentAttributes().Type)
}

func (n *Node) root() *Node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// isComponentTaskExpired returns true when the task's scheduled time is equal
// or before the reference time. The caller should also make sure to account
// for skew between the physical task queue and the database by adjusting
// referenceTime in advance.
func isComponentTaskExpired(
	referenceTime time.Time,
	task *persistencespb.ChasmComponentAttributes_Task,
) bool {
	if task.ScheduledTime == nil {
		return false
	}

	scheduledTime := task.ScheduledTime.AsTime().Truncate(persistence.ScheduledTaskMinPrecision)
	referenceTime = referenceTime.Truncate(persistence.ScheduledTaskMinPrecision)

	return !scheduledTime.After(referenceTime)
}

// EachPureTask runs the callback for all expired/runnable pure tasks within the
// CHASM tree (including invalid tasks). The CHASM tree is left untouched, even
// if invalid tasks are detected (these are cleaned up as part of transaction
// close).
func (n *Node) EachPureTask(
	referenceTime time.Time,
	callback func(executor NodePureTask, taskAttributes TaskAttributes, taskInstance any) (bool, error),
) error {
	ctx := NewContext(context.Background(), n)

	// TODO: instead of tracking processed nodes,
	// consider removing processed pure tasks from the componentAttr.PureTasks slice
	// which also addresses the concern of user provided task validator doesn't invalidate
	// a task even if it's processed.
	processedNodes := make(map[*Node]struct{})

TreeLoop:
	for {
	NodeLoop:
		for _, node := range n.andAllChildren() {
			if _, processed := processedNodes[node]; processed {
				continue NodeLoop
			}
			processedNodes[node] = struct{}{}

			needSyncComponents, err := node.eachNodePureTask(
				ctx,
				referenceTime,
				callback,
			)
			if err != nil {
				return err
			}

			if needSyncComponents {
				// If any pure task is executed, the tree structure may change.
				// Can not continue using the current node iterator to find the next node,
				// need to sync tree structure and start over.
				if err := n.syncSubComponents(); err != nil {
					return err
				}
				continue TreeLoop
			}
		}

		// If code reaches here, it means all tasks have been processed.
		return nil
	}
}

// eachNodePureTask runs the callback on all expired pure tasks for a single node.
// This is a helper function for EachPureTask() which runs the callback for pure tasks
// in all nodes in the tree.
//
// Returns a boolean indicating if any pure task in the node is actually executed and error if any.
func (n *Node) eachNodePureTask(
	chasmContext Context,
	referenceTime time.Time,
	callback func(executor NodePureTask, taskAttributes TaskAttributes, taskInstance any) (bool, error),
) (bool, error) {
	// Skip nodes that aren't serialized yet.
	if n.serializedNode == nil || n.serializedNode.Metadata == nil {
		return false, nil
	}

	componentAttr := n.serializedNode.Metadata.GetComponentAttributes()
	// Skip nodes that aren't components.
	if componentAttr == nil {
		return false, nil
	}

	// Hydrate nodes before the task validator is called.
	err := n.prepareComponentValue(chasmContext)
	if err != nil {
		return false, err
	}

	taskExecuted := false
	for _, task := range componentAttr.GetPureTasks() {
		if !isComponentTaskExpired(referenceTime, task) {
			// Pure tasks are stored in-order, so we can skip scanning the rest once we hit
			// an unexpired task deadline.
			return taskExecuted, nil
		}

		taskInstance, err := n.deserializeComponentTask(task)
		if err != nil {
			return false, err
		}

		taskAttributes := TaskAttributes{
			ScheduledTime: task.ScheduledTime.AsTime(),
			Destination:   task.Destination,
		}

		executed, err := callback(n, taskAttributes, taskInstance)
		if err != nil {
			return false, err
		}
		taskExecuted = taskExecuted || executed

		// TODO: sync structure after each task since it's possible for a task to delete the node generated it
		// when pointers are involved. E.g. Component call a method on its parent which deletes the component.
		//
		// This requires either deleting processed pure tasks or tracking which tasks get processed.
	}
	return taskExecuted, nil
}

func newNode(
	base *nodeBase,
	parent *Node,
	nodeName string,
) *Node {
	return &Node{
		nodeBase: base,
		parent:   parent,
		children: make(map[string]*Node),
		nodeName: nodeName,
	}
}

func compareSideEffectTasks(a, b *persistencespb.ChasmComponentAttributes_Task) int {
	if cmpResult := transitionhistory.Compare(a.VersionedTransition, b.VersionedTransition); cmpResult != 0 {
		return cmpResult
	}
	return cmp.Compare(a.VersionedTransitionOffset, b.VersionedTransitionOffset)
}

func comparePureTasks(a, b *persistencespb.ChasmComponentAttributes_Task) int {
	if cmpResult := a.ScheduledTime.AsTime().Compare(b.ScheduledTime.AsTime()); cmpResult != 0 {
		return cmpResult
	}

	return compareSideEffectTasks(a, b)
}

func (n *Node) carryOverTaskStatus(
	sourceTasks, targetTasks []*persistencespb.ChasmComponentAttributes_Task,
	compareFn func(a, b *persistencespb.ChasmComponentAttributes_Task) int,
) {
	sourceIdx, targetIdx := 0, 0
	for sourceIdx < len(sourceTasks) && targetIdx < len(targetTasks) {
		sourceTask := sourceTasks[sourceIdx]
		targetTask := targetTasks[targetIdx]

		switch compareFn(sourceTask, targetTask) {
		case 0:
			// Task match, carry over status.
			targetTask.PhysicalTaskStatus = sourceTask.PhysicalTaskStatus
			// Use existing task data to avoid taskValueCache miss, since the cache uses
			// *DataBlob as the key.
			// Otherwise we have to clear cache for all tasks in the node, and re-deserialize
			// tasks later.
			targetTask.Data = sourceTask.Data
			sourceIdx++
			targetIdx++
		case -1:
			// Source task has a smaller key, meaning the task has been deleted.
			// Move on to the next source task.
			sourceIdx++
			delete(n.taskValueCache, sourceTask.Data)
		case 1:
			// Source task has a larger key, meaning there's a new task inserted.
			// Sanitize incoming task status.
			targetTask.PhysicalTaskStatus = physicalTaskStatusNone
			targetIdx++
		}
	}

	// Sanitize incoming task status for remaining tasks.
	for ; targetIdx < len(targetTasks); targetIdx++ {
		targetTasks[targetIdx].PhysicalTaskStatus = physicalTaskStatusNone
	}
	for ; sourceIdx < len(sourceTasks); sourceIdx++ {
		delete(n.taskValueCache, sourceTasks[sourceIdx].Data)
	}
}

func taskCategory(
	task *persistencespb.ChasmComponentAttributes_Task,
) tasks.Category {
	if task.Type == visibilityTaskFqType {
		return tasks.CategoryVisibility
	}

	if task.Destination != "" {
		return tasks.CategoryOutbound
	}

	if task.ScheduledTime == nil ||
		task.ScheduledTime.AsTime().Equal(TaskScheduledTimeImmediate) {
		return tasks.CategoryTransfer
	}
	return tasks.CategoryTimer
}

func (n *Node) deserializeTaskWithCache(
	registrableTask *RegistrableTask,
	taskBlob *commonpb.DataBlob,
) (taskValue reflect.Value, retErr error) {
	if cachedValue, ok := n.taskValueCache[taskBlob]; ok {
		return cachedValue, nil
	}

	taskValue, err := deserializeTask(registrableTask, taskBlob)
	if err != nil {
		return reflect.Value{}, err
	}

	n.taskValueCache[taskBlob] = taskValue
	return taskValue, nil
}

func (n *Node) serializeTaskWithCache(
	registrableTask *RegistrableTask,
	taskValue reflect.Value,
) (*commonpb.DataBlob, error) {
	taskBlob, err := serializeTask(registrableTask, taskValue)
	if err != nil {
		return nil, err
	}

	n.taskValueCache[taskBlob] = taskValue
	return taskBlob, nil
}

func deserializeTask(
	registrableTask *RegistrableTask,
	taskBlob *commonpb.DataBlob,
) (taskValue reflect.Value, retErr error) {
	if registrableTask.goType.AssignableTo(protoMessageT) {
		taskValue, err := unmarshalProto(taskBlob, registrableTask.goType)
		if err != nil {
			return reflect.Value{}, err
		}
		return taskValue, nil
	}

	taskGoType := registrableTask.goType
	if taskGoType.Kind() == reflect.Ptr {
		taskGoType = taskGoType.Elem()
	}
	taskValue = reflect.New(taskGoType)

	// At this point taskGoType is guaranteed to be a struct and
	// taskValue is a pointer to struct.

	defer func() {
		if retErr == nil && registrableTask.goType.Kind() == reflect.Struct {
			taskValue = taskValue.Elem()
		}
	}()

	if taskGoType.NumField() == 0 {
		return taskValue, nil
	}

	// TODO: consider pre-calculating the proto field num when registring the task type.

	protoMessageFound := false
	for i := 0; i < taskGoType.NumField(); i++ {
		fieldV := taskValue.Elem().Field(i)
		fieldT := taskGoType.Field(i).Type
		if !fieldT.AssignableTo(protoMessageT) {
			continue
		}

		if protoMessageFound {
			return reflect.Value{}, serviceerror.NewInternal("only one proto field allowed in task struct")
		}
		protoMessageFound = true

		value, err := unmarshalProto(taskBlob, fieldT)
		if err != nil {
			return reflect.Value{}, err
		}

		fieldV.Set(value)
	}

	return taskValue, nil
}

func serializeTask(
	registrableTask *RegistrableTask,
	taskValue reflect.Value,
) (*commonpb.DataBlob, error) {
	protoValue, ok := taskValue.Interface().(proto.Message)
	if ok {
		return serialization.ProtoEncode(protoValue)
	}

	taskGoType := registrableTask.goType

	// Handle pointer to struct.
	if taskGoType.Kind() == reflect.Ptr {
		taskGoType = taskGoType.Elem()
		taskValue = taskValue.Elem()
	}

	// Handle empty task struct.
	if taskGoType.NumField() == 0 {
		return &commonpb.DataBlob{
			Data:         nil,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}, nil
	}

	// TODO: consider pre-calculating the proto field num when registring the task type.

	var blob *commonpb.DataBlob
	protoMessageFound := false
	for i := 0; i < taskGoType.NumField(); i++ {
		fieldV := taskValue.Field(i)
		if !fieldV.Type().AssignableTo(protoMessageT) {
			continue
		}

		if protoMessageFound {
			return nil, serviceerror.NewInternalf("only one proto field allowed in task struct of type: %v", taskGoType.String())
		}
		protoMessageFound = true

		var err error
		blob, err = serialization.ProtoEncode(fieldV.Interface().(proto.Message))
		if err != nil {
			return nil, err
		}
	}

	if !protoMessageFound {
		return nil, serviceerror.NewInternal("no proto field found in task struct")
	}

	return blob, nil
}

// ExecutePureTask validates and then executes the given taskInstance against the
// node's component. Executing an invalid task is a no-op (no error returned).
func (n *Node) ExecutePureTask(
	baseCtx context.Context,
	taskAttributes TaskAttributes,
	taskInstance any,
) (bool, error) {
	registrableTask, ok := n.registry.taskFor(taskInstance)
	if !ok {
		return false, fmt.Errorf("unknown task type for task instance goType '%s'", reflect.TypeOf(taskInstance).Name())
	}

	if !registrableTask.isPureTask {
		return false, fmt.Errorf("ExecutePureTask called on a SideEffect task '%s'", registrableTask.fqType())
	}

	ctx := NewMutableContext(
		newContextWithOperationIntent(baseCtx, OperationIntentProgress),
		n,
	)

	// Ensure this node's component value is hydrated before execution. Component
	// will also check access rules.
	component, err := n.Component(ctx, ComponentRef{})
	if err != nil {
		// NotFound errors are expected here and we can safely skip the task execution.
		if errors.As(err, new(*serviceerror.NotFound)) {
			return false, nil
		}
		return false, err
	}

	// Run the task's registered value before execution.
	valid, err := n.validateTask(ctx, taskAttributes, taskInstance)
	if err != nil {
		return false, err
	}
	if !valid {
		return false, nil
	}

	result := registrableTask.executeFn.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(component),
		reflect.ValueOf(taskAttributes),
		reflect.ValueOf(taskInstance),
	})
	if !result[0].IsNil() {
		//nolint:revive // type cast result is unchecked
		return true, result[0].Interface().(error)
	}

	// TODO - a task validator must succeed validation after a task executes
	// successfully (without error), otherwise it will generate an infinite loop.
	// Check for this case by marking the in-memory task as having executed, which the
	// CloseTransaction method will check against.
	//
	// See: https://github.com/temporalio/temporal/pull/7701#discussion_r2072026993

	return true, nil
}

// ValidatePureTask runs a pure task's associated validator, returning true
// if the task is valid. Intended for use by standby executors as part of
// EachPureTask's callback.
// This method assumes the node's value has already been prepared (hydrated).
func (n *Node) ValidatePureTask(
	ctx context.Context,
	taskAttributes TaskAttributes,
	taskInstance any,
) (bool, error) {
	validateCtx := NewContext(ctx, n)
	return n.validateTask(validateCtx, taskAttributes, taskInstance)
}

// ValidateSideEffectTask runs a side effect task's associated validator,
// returning the deserialized task instance if the task is valid. Intended for
// use by standby executors.
//
// If validation succeeds but the task is invalid, nil is returned to signify the
// task can be skipped/deleted.
//
// If validation fails, that error is returned.
func (n *Node) ValidateSideEffectTask(
	ctx context.Context,
	chasmTask *tasks.ChasmTask,
) (isValid bool, retErr error) {

	taskInfo := chasmTask.Info
	taskType := taskInfo.Type
	registrableTask, ok := n.registry.task(taskType)
	if !ok {
		return false, softassert.UnexpectedInternalErr(
			n.logger,
			"unknown task type",
			fmt.Errorf("%s", taskType))
	}

	if registrableTask.isPureTask {
		return false, softassert.UnexpectedInternalErr(
			n.logger,
			"ValidateSideEffectTask called on a Pure task",
			fmt.Errorf("%s", taskType))
	}

	node, ok := n.findNode(taskInfo.Path)
	if !ok {
		return false, nil
	}

	// node.serializedNode should always be available when running a side effect task.
	if transitionhistory.Compare(
		taskInfo.ComponentInitialVersionedTransition,
		node.serializedNode.Metadata.InitialVersionedTransition,
	) != 0 {
		return false, nil
	}

	// Component must be hydrated before the task's validator is called.
	validateCtx := NewContext(ctx, n)
	if err := node.prepareComponentValue(validateCtx); err != nil {
		return false, err
	}

	defer func() {
		if rec := recover(); rec != nil {
			chasmTask.DeserializedTask = reflect.Value{}
			panic(rec) //nolint:forbidigo
		}
		if retErr != nil {
			chasmTask.DeserializedTask = reflect.Value{}
		}
	}()

	if !chasmTask.DeserializedTask.IsValid() {
		// TODO: Change physical side effect task to reference logical task and
		// then use deserializeTaskWithCache as well.
		var err error
		chasmTask.DeserializedTask, err = deserializeTask(registrableTask, taskInfo.Data)
		if err != nil {
			return false, err
		}
	}

	return node.validateTask(
		validateCtx, TaskAttributes{
			ScheduledTime: chasmTask.GetVisibilityTime(),
			Destination:   chasmTask.Destination,
		},
		chasmTask.DeserializedTask.Interface(),
	)
}

// ExecuteSideEffectTask executes the given ChasmTask on its associated node
// without holding the entity lock.
//
// WARNING: This method *must not* access the node's properties without first
// locking the entity.
//
// ctx should have a CHASM engine already set.
func (n *Node) ExecuteSideEffectTask(
	ctx context.Context,
	registry *Registry,
	entityKey EntityKey,
	chasmTask *tasks.ChasmTask,
	validate func(NodeBackend, Context, Component) error,
) (retErr error) {

	if engineFromContext(ctx) == nil {
		return serviceerror.NewInternal("no CHASM engine set on context")
	}

	taskInfo := chasmTask.Info
	taskType := taskInfo.Type
	registrableTask, ok := registry.task(taskType)
	if !ok {
		return softassert.UnexpectedInternalErr(
			n.logger,
			"unknown task type",
			fmt.Errorf("%s", taskType))
	}
	if registrableTask.isPureTask {
		return softassert.UnexpectedInternalErr(
			n.logger,
			"ExecuteSideEffectTask called on a Pure task",
			fmt.Errorf("%s", taskType))
	}

	defer func() {
		if rec := recover(); rec != nil {
			chasmTask.DeserializedTask = reflect.Value{}
			panic(rec) //nolint:forbidigo
		}
		if retErr != nil && !errors.As(retErr, new(*serviceerror.NotFound)) {
			chasmTask.DeserializedTask = reflect.Value{}
		}
	}()

	if !chasmTask.DeserializedTask.IsValid() {
		var err error
		// TODO: Change physical side effect task to reference logical task and
		// then use deserializeTaskWithCache as well.
		chasmTask.DeserializedTask, err = deserializeTask(registrableTask, taskInfo.Data)
		if err != nil {
			return err
		}
	}
	taskValue := chasmTask.DeserializedTask

	taskAttributes := TaskAttributes{
		ScheduledTime: chasmTask.GetVisibilityTime(),
		Destination:   chasmTask.Destination,
	}

	ref := ComponentRef{
		EntityKey:          entityKey,
		archetype:          n.Archetype(),
		entityLastUpdateVT: taskInfo.ComponentLastUpdateVersionedTransition,
		componentPath:      taskInfo.Path,
		componentInitialVT: taskInfo.ComponentInitialVersionedTransition,

		// Validate the Ref only once it is accessed by the task's executor.
		validationFn: makeValidationFn(registrableTask, validate, taskAttributes, taskValue),
	}

	ctx = newContextWithOperationIntent(ctx, OperationIntentProgress)

	result := registrableTask.executeFn.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(ref),
		reflect.ValueOf(taskAttributes),
		taskValue,
	})
	if !result[0].IsNil() {
		//nolint:revive // type cast result is unchecked
		return result[0].Interface().(error)
	}

	return nil
}

func (n *Node) ComponentByPath(
	chasmContext Context,
	path []string,
) (Component, error) {
	node, ok := n.findNode(path)
	if !ok {
		return nil, errComponentNotFound
	}

	if err := node.prepareComponentValue(chasmContext); err != nil {
		return nil, err
	}

	componentValue, ok := node.value.(Component)
	if !ok {
		return nil, softassert.UnexpectedInternalErr(
			n.logger,
			"component value is not of type Component",
			fmt.Errorf("%s", reflect.TypeOf(node.value).String()))
	}

	return componentValue, nil
}

// makeValidationFn adapts the TaskValidator interface to the ComponentRef's
// validation callback format. Returns a validation function that wraps the
// given validation callback to be called before the RegistrableTask's registered
// validator callback. Intended for use to validate mutable state at access time.
func makeValidationFn(
	registrableTask *RegistrableTask,
	validate func(NodeBackend, Context, Component) error,
	taskAttributes TaskAttributes,
	taskValue reflect.Value,
) func(NodeBackend, Context, Component) error {
	return func(backend NodeBackend, ctx Context, component Component) error {
		// Call the provided validation callback.
		err := validate(backend, ctx, component)
		if err != nil {
			return err
		}

		// Call the TaskValidator interface.
		result := registrableTask.validateFn.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(component),
			reflect.ValueOf(taskAttributes),
			taskValue,
		})

		// Handle err.
		if !result[1].IsNil() {
			//nolint:revive // type cast result is unchecked
			return result[1].Interface().(error)
		}

		// Handle bool result.
		if !result[0].Bool() {
			return errTaskNotValid
		}

		return nil
	}
}
