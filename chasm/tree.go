//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination tree_mock.go

package chasm

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	protoMessageT = reflect.TypeFor[proto.Message]()
)

var (
	errComponentNotFound = serviceerror.NewNotFound("component not found")
)

type valueState uint8

const (
	valueStateUndefined valueState = iota
	valueStateSynced
	valueStateNeedDeserialize
	valueStateNeedSerialize
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
		nodeName string           // key of this node in parent's children map.

		// Type of attributes controls the type of the node.
		serializedNode *persistencespb.ChasmNode // serialized component | data | collection with metadata
		value          any                       // deserialized component | data | collection

		// valueState indicates if the value field and the persistence field serializedNode are in sync.
		// If new value might be changed since it was deserialized and serialize method wasn't called yet, then valueState is valueStateNeedSerialize.
		// If a node is constructed from the database, then valueState is valueStateNeedDeserialize.
		// If serialize or deserialize method were called, then valueState is valueStateSynced, and next calls to them would be no-op.
		// NOTE: This is a different concept from the IsDirty() method needed by MutableState which means
		// if the state in memory matches the state in DB.
		valueState valueState

		// TODO: Consider storing encoded path for the node.
		// Consider using unique package as well.
		// Encoded path for different runs of the same Component type are the same.
		//
		// encodedPath string
	}

	// nodeBase is a set of dependencies and states shared by all nodes in a CHASM tree.
	nodeBase struct {
		registry    *Registry
		timeSource  clock.TimeSource
		backend     NodeBackend
		pathEncoder NodePathEncoder
		logger      log.Logger

		// Following fields are per transaction states, will get cleaned up
		// during CloseTransaction().

		// Mutations accumulated so far in this transaction.
		mutation NodesMutation
		newTasks map[any][]taskWithAttributes // component value -> task & attributes
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
		GetCurrentVersion() int64
		NextTransitionCount() int64
		GetWorkflowKey() definition.WorkflowKey
		AddTasks(...tasks.Task)
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
		newTasks: make(map[any][]taskWithAttributes),
	}

	root := newNode(base, nil, "")
	if len(serializedNodes) == 0 {
		// If serializedNodes is empty, it means that this new tree.
		// Initialize empty serializedNode.
		root.initSerializedNode(fieldTypeComponent)
		// Although both value and serializedNode.Data are nil, they are considered NOT synced
		// because value has no type and serializedNode does.
		// deserialize method should set value when called.
		root.valueState = valueStateNeedDeserialize
		return root, nil
	}

	for encodedPath, serializedNode := range serializedNodes {
		nodePath, err := pathEncoder.Decode(encodedPath)
		if err != nil {
			return nil, err
		}
		root.setSerializedNode(nodePath, serializedNode)
	}

	return root, nil
}

// NewEmptyTree creates a new empty in-memory CHASM tree.
func NewEmptyTree(
	registry *Registry,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
) *Node {
	base := &nodeBase{
		registry:    registry,
		timeSource:  timeSource,
		backend:     backend,
		pathEncoder: pathEncoder,
		mutation: NodesMutation{
			UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
			DeletedNodes: make(map[string]struct{}),
		},
		newTasks: make(map[any][]taskWithAttributes),
	}
	root := newNode(base, nil, "")
	return root
}

// Component retrieves a component from the tree rooted at node n
// using the provided component reference
// It also performs access rule, and task validation checks
// (for task processing requests) before returning the component.
func (n *Node) Component(
	chasmContext Context,
	ref ComponentRef,
) (Component, error) {
	node, ok := n.getNodeByPath(ref.componentPath)
	if !ok {
		return nil, errComponentNotFound
	}

	metadata := node.serializedNode.Metadata
	if ref.componentInitialVT != nil && transitionhistory.Compare(
		ref.componentInitialVT,
		metadata.InitialVersionedTransition,
	) != 0 {
		return nil, errComponentNotFound
	}

	if err := node.prepareComponentValue(chasmContext); err != nil {
		return nil, err
	}

	componentValue, ok := node.value.(Component)
	if !ok {
		return nil, serviceerror.NewInternal(
			fmt.Sprintf("component value is not of type Component: %v", reflect.TypeOf(node.value)),
		)
	}

	// TODO: perform access rule check based on the operation intent
	// and lifecycle state of all ancestor nodes.
	//
	// intent := operationIntentFromContext(chasmContext.getContext())
	// if intent != OperationIntentUnspecified {
	// 	...
	// }

	if ref.validationFn != nil {
		if err := ref.validationFn(chasmContext, componentValue); err != nil {
			return nil, err
		}
	}

	return componentValue, nil
}

func (n *Node) prepareComponentValue(
	chasmContext Context,
) error {
	metadata := n.serializedNode.Metadata
	componentAttr := metadata.GetComponentAttributes()
	if componentAttr == nil {
		return serviceerror.NewInternal(
			fmt.Sprintf("expect chasm node to have ComponentAttributes, actual attributes: %v", metadata.Attributes),
		)
	}

	if n.valueState == valueStateNeedDeserialize {
		registrableComponent, ok := n.registry.component(componentAttr.GetType())
		if !ok {
			return serviceerror.NewInternal(fmt.Sprintf("component type name not registered: %v", componentAttr.GetType()))
		}

		if err := n.deserialize(registrableComponent.goType); err != nil {
			return fmt.Errorf("failed to deserialize component: %w", err)
		}
	}

	// For now, we assume if a node is accessed with a MutableContext,
	// its value will be mutated and no longer in sync with the serializedNode.
	_, componentCanBeMutated := chasmContext.(MutableContext)
	if componentCanBeMutated {
		n.valueState = valueStateNeedSerialize
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
		return serviceerror.NewInternal(
			fmt.Sprintf("expect chasm node to have DataAttributes, actual attributes: %v", metadata.Attributes),
		)
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
		n.valueState = valueStateNeedSerialize
	}

	return nil
}

func (n *Node) fieldType() fieldType {
	if n.serializedNode.GetMetadata().GetComponentAttributes() != nil {
		return fieldTypeComponent
	}

	if n.serializedNode.GetMetadata().GetDataAttributes() != nil {
		return fieldTypeData
	}

	if n.serializedNode.GetMetadata().GetCollectionAttributes() != nil {
		// Collection is not a Field.
		return fieldTypeUnspecified
	}

	if n.serializedNode.GetMetadata().GetPointerAttributes() != nil {
		return fieldTypeComponentPointer
	}

	return fieldTypeUnspecified
}

func assertStructPointer(t reflect.Type) error {
	if t == nil {
		return nil
	}

	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return serviceerror.NewInternal("only pointer to struct is supported for tree node value")
	}
	return nil
}

func fieldName(f reflect.StructField) string {
	if tagName := f.Tag.Get(fieldNameTag); tagName != "" {
		return tagName
	}
	return f.Name
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
	case fieldTypeComponentPointer:
		panic("not implemented")
	case fieldTypeUnspecified:
		// Do nothing. Panic?
	}
}

func (n *Node) setSerializedNode(
	nodePath []string,
	serializedNode *persistencespb.ChasmNode,
) {
	if len(nodePath) == 0 {
		n.serializedNode = serializedNode
		n.valueState = valueStateNeedDeserialize
		return
	}

	childName := nodePath[0]
	childNode, ok := n.children[childName]
	if !ok {
		childNode = newNode(n.nodeBase, n, childName)
		n.children[childName] = childNode
	}
	childNode.setSerializedNode(nodePath[1:], serializedNode)
}

// serialize sets or updates serializedValue field of the node n with serialized value.
func (n *Node) serialize() error {
	if n.valueState != valueStateNeedSerialize {
		return nil
	}

	switch n.serializedNode.GetMetadata().GetAttributes().(type) {
	case *persistencespb.ChasmNodeMetadata_ComponentAttributes:
		return n.serializeComponentNode()
	case *persistencespb.ChasmNodeMetadata_DataAttributes:
		return n.serializeDataNode()
	case *persistencespb.ChasmNodeMetadata_CollectionAttributes:
		panic("not implemented")
	case *persistencespb.ChasmNodeMetadata_PointerAttributes:
		panic("not implemented")
	default:
		return serviceerror.NewInternal("unknown node type")
	}
}

func (n *Node) serializeComponentNode() error {
	nodeValueT := reflect.TypeOf(n.value)
	nodeValueV := reflect.ValueOf(n.value)

	protoMessageFound := false
	// TODO: consider using walker pattern to unify walking over reflected fields.
	for i := 0; i < nodeValueT.Elem().NumField(); i++ {
		fieldV := nodeValueV.Elem().Field(i)
		if !fieldV.Type().AssignableTo(protoMessageT) {
			continue
		}

		if protoMessageFound {
			return serviceerror.NewInternal("only one proto field allowed in component")
		}
		protoMessageFound = true

		var blob *commonpb.DataBlob
		if !fieldV.IsNil() {
			var err error
			if blob, err = serialization.ProtoEncodeBlob(fieldV.Interface().(proto.Message), enumspb.ENCODING_TYPE_PROTO3); err != nil {
				return err
			}
		}

		rc, ok := n.registry.componentFor(n.value)
		if !ok {
			return serviceerror.NewInternal(fmt.Sprintf("component type %s is not registered", nodeValueT.String()))
		}

		n.serializedNode.Data = blob
		n.serializedNode.GetMetadata().GetComponentAttributes().Type = rc.fqType()
		n.updateLastUpdateVersionedTransition()
		n.valueState = valueStateSynced

		// continue to iterate over fields to validate that there is only one proto field in the component.
	}
	return nil
}

// Sync the entire tree recursively starting from node n from the underlining component value:
//   - Create:
//     -- if child node is nil but subcomponent is not empty, a new node with subcomponent value is created.
//   - Delete:
//     -- if subcomponent is empty, the corresponding child is removed from the tree,
//     -- if subcomponent is no longer in a component, the corresponding child is removed from the tree,
//     -- when a child is removed, all its children are removed too.
//
// All removed paths are added to mutation.DeletedNodes (which is shared between all nodes in the tree).
func (n *Node) syncSubComponents() error {
	if n.parent != nil {
		return serviceerror.NewInternal("syncSubComponents must be called on root node")
	}
	n.mutation.DeletedNodes = make(map[string]struct{})
	return n.syncSubComponentsInternal(nil)
}

func (n *Node) syncSubComponentsInternal(
	nodePath []string,
) error {
	nodeValueT := reflect.TypeOf(n.value)
	nodeValueV := reflect.ValueOf(n.value)

	childrenToKeep := make(map[string]struct{})
	for i := 0; i < nodeValueT.Elem().NumField(); i++ {
		fieldV := nodeValueV.Elem().Field(i)
		fieldT := fieldV.Type()

		if fieldT == UnimplementedComponentT {
			continue
		}

		if fieldT.Kind() == reflect.Ptr {
			continue
		}

		fieldN := fieldName(nodeValueT.Elem().Field(i))

		switch genericTypePrefix(fieldT) {
		case chasmFieldTypePrefix:
			internalV := fieldV.FieldByName(internalFieldName)
			//nolint:revive // Internal field is guaranteed to be of type fieldInternal.
			internal := internalV.Interface().(fieldInternal)
			if internal.isEmpty() {
				continue
			}
			if internal.node == nil && internal.value() != nil {
				// Field is not empty but tree node is not set. It means this is a new field, and a node must be created.
				childNode := newNode(n.nodeBase, n, fieldN)

				if err := assertStructPointer(reflect.TypeOf(internal.value())); err != nil {
					return err
				}
				childNode.value = internal.value()
				childNode.initSerializedNode(internal.fieldType())
				childNode.valueState = valueStateNeedSerialize

				n.children[fieldN] = childNode
				internal.node = childNode
				// TODO: this line can be remove if Internal becomes a *fieldInternal.
				internalV.Set(reflect.ValueOf(internal))
			}
			if err := internal.node.syncSubComponentsInternal(append(nodePath, fieldN)); err != nil {
				return err
			}

			childrenToKeep[fieldN] = struct{}{}
		case chasmCollectionTypePrefix:
			childrenToKeep[fieldN] = struct{}{}
			// TODO: need to go over every item in collection and update children for it.
			panic("not implemented")
		}
	}

	err := n.deleteChildren(childrenToKeep, nodePath)
	return err
}

func (n *Node) deleteChildren(childrenToKeep map[string]struct{}, currentPath []string) error {
	for childName, childNode := range n.children {
		if _, childToKeep := childrenToKeep[childName]; !childToKeep {
			if err := childNode.deleteChildren(nil, append(currentPath, childName)); err != nil {
				return err
			}
			path, err := n.pathEncoder.Encode(childNode, append(currentPath, childName))
			if err != nil {
				return err
			}
			n.mutation.DeletedNodes[path] = struct{}{}
			// If a parent is about to be removed, it must not have any children.
			softassert.That(n.logger, len(childNode.children) == 0, "childNode.children must be empty when childNode is removed")
			delete(n.children, childName)
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
		if blob, err = serialization.ProtoEncodeBlob(protoValue, enumspb.ENCODING_TYPE_PROTO3); err != nil {
			return err
		}
	}
	n.serializedNode.Data = blob
	n.updateLastUpdateVersionedTransition()
	n.valueState = valueStateSynced

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

	if n.valueState != valueStateNeedDeserialize {
		return nil
	}

	switch n.serializedNode.GetMetadata().GetAttributes().(type) {
	case *persistencespb.ChasmNodeMetadata_ComponentAttributes:
		return n.deserializeComponentNode(valueT)
	case *persistencespb.ChasmNodeMetadata_DataAttributes:
		return n.deserializeDataNode(valueT)
	case *persistencespb.ChasmNodeMetadata_CollectionAttributes:
		panic("not implemented")
	case *persistencespb.ChasmNodeMetadata_PointerAttributes:
		// TODO: return serviceerror.NewInternal(...) instead.
	}
	return nil
}

func (n *Node) deserializeComponentNode(
	valueT reflect.Type,
) error {
	// valueT is guaranteed to be a pointer to the struct because it was already validated by the assertStructPointer method.
	valueV := reflect.New(valueT.Elem())
	if n.serializedNode.GetData() == nil {
		// serializedNode is empty (has only metadata) => use constructed value of valueT type as value and return.
		// deserialize method acts as a component constructor.
		n.value = valueV.Interface()
		n.valueState = valueStateSynced
		return nil
	}

	protoMessageFound := false
	for i := 0; i < valueT.Elem().NumField(); i++ {
		fieldV := valueV.Elem().Field(i)
		fieldT := fieldV.Type()

		if fieldT == UnimplementedComponentT {
			continue
		}

		if fieldT.AssignableTo(protoMessageT) {
			if protoMessageFound {
				return serviceerror.NewInternal("only one proto field allowed in component")
			}
			protoMessageFound = true

			value, err := unmarshalProto(n.serializedNode.GetData(), fieldT)
			if err != nil {
				return err
			}
			fieldV.Set(value)
			continue
		}

		// chasm.Field field must NOT be a pointer, i.e. chasm.Field[T] not *chasm.Field[T].
		if fieldT.Kind() == reflect.Ptr {
			continue
		}

		fieldN := fieldName(valueT.Elem().Field(i))

		switch genericTypePrefix(fieldT) {
		case chasmFieldTypePrefix:
			if childNode, found := n.children[fieldN]; found {
				chasmFieldV := reflect.New(fieldT).Elem()
				internalValue := reflect.ValueOf(newFieldInternalWithNode(childNode))
				chasmFieldV.FieldByName(internalFieldName).Set(internalValue)
				fieldV.Set(chasmFieldV)
			}
			continue
		case chasmCollectionTypePrefix:
			// TODO: support collection
			// init the map and populate
			panic("not implemented")
			// continue
		}

		return serviceerror.NewInternal(fmt.Sprintf("unsupported field type %s in component %s", fieldT.String(), valueT.String()))
	}

	if !protoMessageFound {
		return serviceerror.NewInternal("no proto field found in component")
	}

	n.value = valueV.Interface()
	n.valueState = valueStateSynced
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
	n.valueState = valueStateSynced
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

	if err := serialization.ProtoDecodeBlob(dataBlob, value.Interface().(proto.Message)); err != nil {
		return reflect.Value{}, err
	}

	return value, nil
}

// Ref implements the CHASM Context interface
func (n *Node) Ref(
	component Component,
) (ComponentRef, bool) {
	panic("not implemented")
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
) error {
	n.nodeBase.newTasks[component] = append(n.nodeBase.newTasks[component], taskWithAttributes{
		task:       task,
		attributes: taskAttributes,
	})
	return nil
}

// CloseTransaction is used by MutableState to close the transaction and
// track changes made in the current transaction.
func (n *Node) CloseTransaction() (NodesMutation, error) {
	defer n.cleanupTransaction()

	// TODO: serialize updated nodes and update tree structure here.

	if err := n.closeTransactionUpdateComponentTasks(); err != nil {
		return NodesMutation{}, err
	}

	if err := n.closeTransactionGeneratePhysicalSideEffectTasks(); err != nil {
		return NodesMutation{}, err
	}

	if err := n.closeTransactionGeneratePhysicalPureTask(); err != nil {
		return NodesMutation{}, err
	}

	panic("not implemented")
	// return n.mutation, nil
}

func (n *Node) closeTransactionUpdateComponentTasks() error {
	taskOffset := int64(1)
	nextVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
		TransitionCount:          n.backend.NextTransitionCount(),
	}

	for _, node := range n.andAllChildren() {
		// no-op if node is not a component
		componentAttr := node.serializedNode.Metadata.GetComponentAttributes()
		if componentAttr == nil {
			return nil
		}

		// no-op if node is not updated in this transition
		lastUpdateVT := node.serializedNode.GetMetadata().LastUpdateVersionedTransition
		if transitionhistory.Compare(lastUpdateVT, nextVersionedTransition) != 0 {
			return nil
		}

		// no-op if node is not even deserialized
		// NOTE: do not check if node.valueState == valueStateNeedSerialize here, because this method needs to be called
		// after the tree structure is updated and value is serialized, and that flag will
		// get set to valueStateSynced.
		if node.valueState == valueStateNeedDeserialize {
			return nil
		}

		// Validate existing tasks and remove invalid ones.
		validateContext := NewContext(context.Background(), n)
		var validationErr error
		deleteFunc := func(existingTask *persistencespb.ChasmComponentAttributes_Task) bool {
			valid, err := node.validateComponentTask(validateContext, existingTask)
			if err != nil {
				validationErr = err
				return false
			}
			return !valid
		}
		componentAttr.SideEffectTasks = slices.DeleteFunc(componentAttr.SideEffectTasks, deleteFunc)
		if validationErr != nil {
			return validationErr
		}
		componentAttr.PureTasks = slices.DeleteFunc(componentAttr.PureTasks, deleteFunc)
		if validationErr != nil {
			return validationErr
		}

		// no-op if no new tasks for this component
		newTasks, ok := node.nodeBase.newTasks[node.value]
		if !ok {
			return nil
		}

		for _, newTask := range newTasks {
			taskValue := newTask.task
			registrableTask, ok := n.registry.taskFor(taskValue)
			if !ok {
				return serviceerror.NewInternal(fmt.Sprintf("task type %s is not registered", reflect.TypeOf(taskValue).String()))
			}

			taskBlob, err := serializeTask(registrableTask, taskValue)
			if err != nil {
				return err
			}

			componentTask := &persistencespb.ChasmComponentAttributes_Task{
				Type:                      registrableTask.fqType(),
				Destination:               newTask.attributes.Destination,
				ScheduledTime:             timestamppb.New(newTask.attributes.ScheduledTime),
				Data:                      taskBlob,
				VersionedTransition:       nextVersionedTransition,
				VersionedTransitionOffset: taskOffset,
				PhysicalTaskStatus:        physicalTaskStatusNone,
			}

			if registrableTask.isPureTask {
				componentAttr.PureTasks = append(componentAttr.PureTasks, componentTask)
			} else {
				componentAttr.SideEffectTasks = append(componentAttr.SideEffectTasks, componentTask)
			}

			taskOffset++
		}

		// pure tasks are sorted by scheduled time.
		slices.SortFunc(componentAttr.PureTasks, comparePureTasks)
	}

	return nil
}

func (n *Node) validateComponentTask(
	validateContext Context,
	componentTask *persistencespb.ChasmComponentAttributes_Task,
) (bool, error) {
	registableTask, ok := n.registry.task(componentTask.Type)
	if !ok {
		return false, serviceerror.NewInternal(fmt.Sprintf("task type %s is not registered", componentTask.Type))
	}

	// TODO: cache validateMethod (reflect.Value) in the registry
	validator := registableTask.validator
	validateMethod := reflect.ValueOf(validator).MethodByName("Validate")

	// TODO: cache deserialized task value (reflect.Value) in the node,
	// use task VT and offset as the key
	deserizedTaskValue, err := deserializeTask(registableTask, componentTask.Data)
	if err != nil {
		return false, err
	}

	retValues := validateMethod.Call([]reflect.Value{
		reflect.ValueOf(validateContext),
		reflect.ValueOf(n.value),
		deserizedTaskValue,
	})
	if !retValues[1].IsNil() {
		//revive:disable-next-line:unchecked-type-assertion
		return false, retValues[1].Interface().(error)
	}
	//revive:disable-next-line:unchecked-type-assertion
	return retValues[0].Interface().(bool), nil
}

func (n *Node) closeTransactionGeneratePhysicalSideEffectTasks() error {
	entityKey := n.backend.GetWorkflowKey()

	for encodedPath, updatedNode := range n.mutation.UpdatedNodes {
		componentAttr := updatedNode.GetMetadata().GetComponentAttributes()
		if componentAttr == nil {
			continue
		}

		sideEffectTasks := componentAttr.GetSideEffectTasks()
		for idx := len(sideEffectTasks) - 1; idx >= 0; idx-- {
			sideEffectTask := sideEffectTasks[idx]
			if sideEffectTask.PhysicalTaskStatus == physicalTaskStatusCreated {
				break
			}

			category, err := taskCategory(sideEffectTask)
			if err != nil {
				return err
			}

			physicalTask := &tasks.ChasmTask{
				WorkflowKey:         entityKey,
				VisibilityTimestamp: sideEffectTask.ScheduledTime.AsTime(),
				Destination:         sideEffectTask.Destination,
				Category:            category,
				Info: &persistencespb.ChasmTaskInfo{
					Ref: &persistencespb.ChasmComponentRef{
						ComponentInitialVersionedTransition:    updatedNode.Metadata.InitialVersionedTransition,
						ComponentLastUpdateVersionedTransition: updatedNode.Metadata.LastUpdateVersionedTransition,
						Path:                                   encodedPath,
					},
					Type: sideEffectTask.Type,
					Data: sideEffectTask.Data,
				},
			}
			n.backend.AddTasks(physicalTask)
			sideEffectTask.PhysicalTaskStatus = physicalTaskStatusCreated
		}
	}

	return nil
}

func (n *Node) closeTransactionGeneratePhysicalPureTask() error {
	var firstPureTask *persistencespb.ChasmComponentAttributes_Task
	var firstTaskNode *Node
	for _, node := range n.andAllChildren() {
		componentAttr := node.serializedNode.GetMetadata().GetComponentAttributes()
		if componentAttr == nil {
			return nil
		}

		pureTasks := componentAttr.GetPureTasks()
		if len(pureTasks) == 0 {
			return nil
		}

		if firstPureTask == nil ||
			comparePureTasks(pureTasks[0], firstPureTask) < 0 {
			firstPureTask = pureTasks[0]
			firstTaskNode = node
		}
	}

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
	encodedPath, err := firstTaskNode.encodedPath()
	if err != nil {
		return err
	}
	n.mutation.UpdatedNodes[encodedPath] = firstTaskNode.serializedNode
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
	n.newTasks = make(map[any][]taskWithAttributes)
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
	n.snapshotInternal(exclusiveMinVT, []string{}, nodes)

	return NodesSnapshot{
		Nodes: nodes,
	}
}

func (n *Node) snapshotInternal(
	exclusiveMinVT *persistencespb.VersionedTransition,
	currentPath []string,
	nodes map[string]*persistencespb.ChasmNode,
) {
	if n == nil {
		return
	}

	if transitionhistory.Compare(n.serializedNode.Metadata.LastUpdateVersionedTransition, exclusiveMinVT) > 0 {
		encodedPath, err := n.pathEncoder.Encode(n, currentPath)
		if !softassert.That(n.logger, err == nil, "chasm path encoding should always succeed on clean tree") {
			panic(fmt.Sprintf("failed to encode chasm path on clean tree: %v", err))
		}
		nodes[encodedPath] = n.serializedNode
	}

	for childName, childNode := range n.children {
		childNode.snapshotInternal(
			exclusiveMinVT,
			append(currentPath, childName),
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

	return n.applyUpdates(mutation.UpdatedNodes)
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

		node, ok := n.getNodeByPath(path)
		if !ok {
			// Already deleted.
			// This could happen when:
			// - If the mutations passed in include changes
			// older than the current state of the tree.
			// - We are already applied the deletion on a parent node.
			continue
		}

		if err := node.delete(path, &encodedPath); err != nil {
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

		node, ok := n.getNodeByPath(path)
		if !ok {
			// Node doesn't exist, we need to create it.
			n.setSerializedNode(path, updatedNode)
			n.mutation.UpdatedNodes[encodedPath] = updatedNode
			continue
		}

		if transitionhistory.Compare(
			node.serializedNode.Metadata.LastUpdateVersionedTransition,
			updatedNode.Metadata.LastUpdateVersionedTransition,
		) != 0 {
			localComponentAttr := node.serializedNode.GetMetadata().GetComponentAttributes()
			updatedComponentAttr := updatedNode.GetMetadata().GetComponentAttributes()
			if localComponentAttr != nil && updatedComponentAttr != nil {
				carryOverTaskStatus(
					localComponentAttr.SideEffectTasks,
					updatedComponentAttr.SideEffectTasks,
					compareSideEffectTasks,
				)
				carryOverTaskStatus(
					localComponentAttr.PureTasks,
					updatedComponentAttr.PureTasks,
					comparePureTasks,
				)
			}

			n.mutation.UpdatedNodes[encodedPath] = updatedNode
			node.serializedNode = updatedNode
			node.value = nil
			n.valueState = valueStateNeedDeserialize

			// Clearing decoded value for ancestor nodes is not necessary because the value field is not referenced directly.
			// Parent node is pointing to the Node struct.
		}
	}

	return nil
}

func (n *Node) encodedPath() (string, error) {
	return n.pathEncoder.Encode(n, n.path())
}

func (n *Node) path() []string {
	if n.parent == nil {
		return []string{n.nodeName}
	}

	return append(n.parent.path(), n.nodeName)
}

func (n *Node) getNodeByPath(
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
	return childNode.getNodeByPath(path[1:])
}

func (n *Node) delete(
	currentNodePath []string,
	currentEncodedPathPtr *string,
) error {
	for childName, childNode := range n.children {
		if err := childNode.delete(append(currentNodePath, childName), nil); err != nil {
			return err
		}
	}

	if n.parent != nil {
		delete(n.parent.children, n.nodeName)
	}

	var encodedPath string
	if currentEncodedPathPtr == nil {
		var err error
		encodedPath, err = n.pathEncoder.Encode(n, currentNodePath)
		if err != nil {
			return err
		}
	} else {
		encodedPath = *currentEncodedPathPtr
	}

	n.mutation.DeletedNodes[encodedPath] = struct{}{}

	return nil
}

// IsDirty returns true if any node rooted at Node n has been modified,
// and different from the state persisted in DB.
// The result will be reset to false after a call to CloseTransaction().
func (n *Node) IsDirty() bool {
	if len(n.mutation.UpdatedNodes) > 0 || len(n.mutation.DeletedNodes) > 0 {
		return true
	}

	return n.isValueNeedSerialize()
}

func (n *Node) isValueNeedSerialize() bool {
	if n.valueState == valueStateNeedSerialize {
		return true
	}

	for _, childNode := range n.children {
		if childNode.isValueNeedSerialize() {
			return true
		}
	}

	return false
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

func carryOverTaskStatus(
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
			sourceIdx++
			targetIdx++
		case -1:
			// Source task has a smaller key, meaning the task has been deleted.
			// Move on to the next source task.
			sourceIdx++
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
}

func taskCategory(
	task *persistencespb.ChasmComponentAttributes_Task,
) (tasks.Category, error) {
	isImmediate := task.ScheduledTime == nil || task.ScheduledTime.AsTime().Equal(TaskScheduledTimeImmediate)

	if task.Destination != "" {
		if !isImmediate {
			return tasks.Category{}, serviceerror.NewInternal(
				fmt.Sprintf("Task cannot have both destination and scheduled time set, destination: %v, scheduled time: %v", task.Destination, task.ScheduledTime.AsTime()),
			)
		}
		return tasks.CategoryOutbound, nil
	}

	if isImmediate {
		return tasks.CategoryTransfer, nil
	}
	return tasks.CategoryTimer, nil
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
	task any,
) (*commonpb.DataBlob, error) {
	protoValue, ok := task.(proto.Message)
	if ok {
		return serialization.ProtoEncodeBlob(protoValue, enumspb.ENCODING_TYPE_PROTO3)
	}

	taskGoType := registrableTask.goType
	taskValue := reflect.ValueOf(task)

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
			return nil, serviceerror.NewInternal(fmt.Sprintf("only one proto field allowed in task struct of type: %v", taskGoType.String()))
		}
		protoMessageFound = true

		var err error
		blob, err = serialization.ProtoEncodeBlob(fieldV.Interface().(proto.Message), enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
	}

	if !protoMessageFound {
		return nil, serviceerror.NewInternal("no proto field found in task struct")
	}

	return blob, nil
}
