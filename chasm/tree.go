// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination tree_mock.go

package chasm

import (
	"reflect"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

var (
	protoMessageT = reflect.TypeFor[proto.Message]()
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

		// Type of attributes controls the type of the node.
		serializedValue *persistencespb.ChasmNode // serialized component | data | collection
		value           any                       // deserialized component | data | collection

		// TODO: this is not used yet.
		dirty bool

		// TODO: add other necessary fields here, e.g.
		//
		// key string   // key of this node in parent's children map.
	}

	// nodeBase is a set of dependencies and states shared by all nodes in a CHASM tree.
	nodeBase struct {
		registry    *Registry
		timeSource  clock.TimeSource
		backend     NodeBackend
		pathEncoder NodePathEncoder
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
	}

	// NodePathEncoder is an interface for encoding and decoding node paths.
	// Logic outside the chasm package should only work with encoded paths.
	NodePathEncoder interface {
		Encode(node *Node, path []string) (string, error)
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
) (*Node, error) {
	base := &nodeBase{
		registry:    registry,
		timeSource:  timeSource,
		backend:     backend,
		pathEncoder: pathEncoder,
	}

	root := newNode(base, nil)
	for encodedPath, serializedNode := range serializedNodes {
		nodePath, err := pathEncoder.Decode(encodedPath)
		if err != nil {
			return nil, err
		}
		root.setSerializedValue(nodePath, serializedNode)
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
	}
	root := newNode(base, nil)
	return root
}

func (n *Node) setValue(v any, ft fieldType) error {
	if err := validateValueType(v); err != nil {
		return err
	}

	n.value = v

	switch ft {
	case fieldTypeData:
		n.serializedValue = &persistencespb.ChasmNode{
			Attributes: &persistencespb.ChasmNode_DataAttributes{
				DataAttributes: &persistencespb.ChasmDataAttributes{},
			},
		}
	case fieldTypeComponent:
		n.serializedValue = &persistencespb.ChasmNode{
			Attributes: &persistencespb.ChasmNode_ComponentAttributes{
				ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
			},
		}
	case fieldTypeComponentPointer:
		panic("not implemented")
	}

	return n.reflectSubcomponents()
}

func validateValueType(v any) error {
	return validateType(reflect.TypeOf(v))
}

func validateType(t reflect.Type) error {
	// TODO: for component, interface should also be supported.
	if t.Kind() != reflect.Ptr && t.Elem().Kind() != reflect.Struct {
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

func (n *Node) reflectSubcomponents() error {
	nodeValueT := reflect.TypeOf(n.value)
	nodeValueV := reflect.ValueOf(n.value)

	for i := 0; i < nodeValueT.Elem().NumField(); i++ {
		fieldV := nodeValueV.Elem().Field(i)
		fieldT := fieldV.Type()

		// chasm.Field field must be a pointer, i.e. *chasm.Field[T].
		// TODO: for collection it is not true though.
		if fieldT.Kind() != reflect.Ptr {
			continue
		}

		if fieldV.IsNil() {
			continue
		}

		fieldN := fieldName(nodeValueT.Elem().Field(i))

		// TODO: cache Data field in n and set it later to save on reflection.

		switch genericTypePrefix(fieldT.Elem()) {
		case chasmFieldTypePrefix:
			internalV := fieldV.Elem().FieldByName(internalFieldName)
			internal := internalV.Interface().(fieldInternal)
			if internal.node != nil {
				// This subcomponent already has a tree node. No need to create a new one.
				continue
			}

			childN := newNode(n.nodeBase, n)
			// Recursively set all sub-subcomponents.
			if err := childN.setValue(internal.value, internal.fieldType); err != nil {
				return err
			}
			n.children[fieldN] = childN
			internal.node = childN
			// TODO: this line can be remove if Internal becomes a *fieldInternal.
			internalV.Set(reflect.ValueOf(internal))
		case chasmCollectionTypePrefix:
			panic("not implemented")
		}
	}
	return nil
}

func (n *Node) setSerializedValue(
	nodePath []string,
	serializedNode *persistencespb.ChasmNode,
) {
	if len(nodePath) == 0 {
		n.serializedValue = serializedNode
		return
	}

	childName := nodePath[0]
	childNode, ok := n.children[childName]
	if !ok {
		childNode = newNode(n.nodeBase, n)
		n.children[childName] = childNode
	}
	childNode.setSerializedValue(nodePath[1:], serializedNode)
}

// TODO: this is called from CloseTransaction for every node in the tree.
// serialize sets or updates serializedValue field of the node n with serialized value.
func (n *Node) serialize() error {
	switch n.serializedValue.Attributes.(type) {
	case *persistencespb.ChasmNode_ComponentAttributes:
		return n.serializeComponentNode()
	case *persistencespb.ChasmNode_DataAttributes:
		return n.serializeDataNode()
	case *persistencespb.ChasmNode_CollectionAttributes:
		panic("not implemented")
	case *persistencespb.ChasmNode_PointerAttributes:
		panic("not implemented")
	default:
		return serviceerror.NewInternal("unknown node type")
	}
}

func (n *Node) serializeComponentNode() error {
	nodeValueT := reflect.TypeOf(n.value)
	nodeValueV := reflect.ValueOf(n.value)

	protoMessageFound := false
	for i := 0; i < nodeValueT.Elem().NumField(); i++ {
		fieldV := nodeValueV.Elem().Field(i)
		fieldT := fieldV.Type()
		if !fieldT.AssignableTo(protoMessageT) {
			continue
		}

		if fieldV.IsNil() {
			continue
		}

		if protoMessageFound {
			return serviceerror.NewInternal("only one proto field allowed in component")
		}
		protoMessageFound = true

		blob, err := serialization.ProtoEncodeBlob(fieldV.Interface().(proto.Message), enums.ENCODING_TYPE_PROTO3)
		if err != nil {
			return err
		}

		n.serializedValue.GetComponentAttributes().Data = blob
		n.serializedValue.GetComponentAttributes().Type = nodeValueT.Elem().String() // type name w/o *
		if n.serializedValue.GetInitialVersionedTransition() == nil {
			n.serializedValue.InitialVersionedTransition = &persistencespb.VersionedTransition{
				TransitionCount:          n.backend.NextTransitionCount(),
				NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
			}
		}
	}
	return nil
}

// Sync tree starting from node n with underlining component value:
//   - if a child is nil in a corresponding component, it is removed from the tree,
//   - if a child is no longer in a corresponding component, it is removed from the tree,
//   - when a child is removed, all its children are removed too.
//
// Returns slice of paths to removed nodes.
func (n *Node) syncChildren() ([]string, error) {
	var removedPaths []string
	err := n.syncChildrenInternal(&removedPaths, nil)
	return removedPaths, err
}

func (n *Node) syncChildrenInternal(
	removedPaths *[]string,
	nodePath []string,
) error {
	nodeValueT := reflect.TypeOf(n.value)
	nodeValueV := reflect.ValueOf(n.value)

	childrenToKeep := make(map[string]struct{})
	for i := 0; i < nodeValueT.Elem().NumField(); i++ {
		fieldV := nodeValueV.Elem().Field(i)
		fieldT := fieldV.Type()

		if fieldT.Kind() != reflect.Ptr {
			continue
		}

		if fieldV.IsNil() {
			continue
		}

		fieldN := fieldName(nodeValueT.Elem().Field(i))

		switch genericTypePrefix(fieldT.Elem()) {
		case chasmFieldTypePrefix:
			internal := fieldV.Elem().FieldByName(internalFieldName).Interface().(fieldInternal)
			if internal.node == nil {
				// This can't happen!
				// If parent is in the tree all children should be in the tree too i.e. has node set.
				// TODO: use softassert
				panic("internal.node is nil")
			}
			if err := internal.node.syncChildrenInternal(removedPaths, append(nodePath, fieldN)); err != nil {
				return err
			}

			childrenToKeep[fieldN] = struct{}{}
		case chasmCollectionTypePrefix:
			childrenToKeep[fieldN] = struct{}{}
			// TODO: need to go over every item in collection and update children for it.
			panic("not implemented")
		}
	}

	if err := n.deleteChildren(removedPaths, childrenToKeep, nodePath); err != nil {
		return err
	}

	return nil
}

func (n *Node) deleteChildren(removedPaths *[]string, childrenToKeep map[string]struct{}, currentPath []string) error {
	for childName, childNode := range n.children {
		if _, childToKeep := childrenToKeep[childName]; !childToKeep {
			if err := childNode.deleteChildren(removedPaths, nil, append(currentPath, childName)); err != nil {
				return err
			}
			path, err := n.pathEncoder.Encode(n, append(currentPath, childName))
			if err != nil {
				return err
			}
			*removedPaths = append(*removedPaths, path)
			// If parent is about to be removed, it must not have any children.
			// TODO: softassert: len(childNode.children)==0
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

	blob, err := serialization.ProtoEncodeBlob(protoValue, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	n.serializedValue.GetDataAttributes().Data = blob
	return nil
}

// deserialize initializes the node's value from its serializedValue.
// If value is of component type, it initializes every chasm.Field of it and sets node field but not value field.
// deserialize is not recursive; i.e., must be called on every node separately.
func (n *Node) deserialize(
	valueT reflect.Type,
) error {
	if err := validateType(valueT); err != nil {
		return err
	}

	switch n.serializedValue.Attributes.(type) {
	case *persistencespb.ChasmNode_ComponentAttributes:
		return n.deserializeComponentNode(valueT)
	case *persistencespb.ChasmNode_DataAttributes:
		return n.deserializeDataNode(valueT)
	case *persistencespb.ChasmNode_CollectionAttributes:
		panic("not implemented")
	case *persistencespb.ChasmNode_PointerAttributes:
		// TODO: return serviceerror.NewInternal(...) instead.
	}
	return nil
}

func (n *Node) deserializeComponentNode(
	valueT reflect.Type,
) error {
	valueV := reflect.New(valueT.Elem())
	protoMessageFound := false
	for i := 0; i < valueT.Elem().NumField(); i++ {
		fieldV := valueV.Elem().Field(i)
		fieldT := fieldV.Type()
		if fieldT.AssignableTo(protoMessageT) {
			if protoMessageFound {
				return serviceerror.NewInternal("only one proto field allowed in component")
			}
			protoMessageFound = true

			value, err := n.unmarshalProto(n.serializedValue.GetComponentAttributes().GetData(), fieldT)
			if err != nil {
				return err
			}
			fieldV.Set(value)
			continue
		}

		// chasm.Field field must be a pointer, i.e. *chasm.Field[T].
		// TODO: for collection it is not true though.
		if fieldT.Kind() != reflect.Ptr {
			continue
		}

		fieldN := fieldName(valueT.Elem().Field(i))

		switch genericTypePrefix(fieldT.Elem()) {
		case chasmFieldTypePrefix:
			if childNode, found := n.children[fieldN]; found {
				chasmFieldV := reflect.New(fieldT.Elem()) // TODO: support chasm.Field[interface]
				internalValue := reflect.ValueOf(fieldInternal{
					node: childNode,
				})
				chasmFieldV.Elem().FieldByName(internalFieldName).Set(internalValue)
				fieldV.Set(chasmFieldV)
			}

		case chasmCollectionTypePrefix:
			// TODO: support collection
			// init the map and populate
			panic("not implemented")
		}

		// TODO: error or just go around?
		// return serviceerror.NewInternal("unsupported field type in component " + fieldT.String())
	}

	if !protoMessageFound {
		return serviceerror.NewInternal("no proto field found in component")
	}

	n.value = valueV.Interface()
	return nil
}

func (n *Node) deserializeDataNode(
	valueT reflect.Type,
) error {
	value, err := n.unmarshalProto(n.serializedValue.GetDataAttributes().GetData(), valueT)
	if err != nil {
		return err
	}

	n.value = value.Interface()
	return nil
}

func (n *Node) unmarshalProto(
	dataBlob *common.DataBlob,
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

// Component retrieves a component from the tree rooted at node n
// using the provided component reference
// It also performs consistency, access rule, and task validation checks
// (for task processing requests) before returning the component.
func (n *Node) Component(
	chasmContext Context,
	ref ComponentRef,
) (Component, error) {
	panic("not implemented")
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
	task interface{},
) error {
	panic("not implemented")
}

// CloseTransaction is used by MutableState to close the transaction and
// track changes made in the current transaction.
func (n *Node) CloseTransaction() (NodesMutation, error) {
	panic("not implemented")
}

// Snapshot returns all nodes in the tree that have been modified after the given min versioned transition.
// A nil minVT will be treated as the same as the zero versioned transition and returns all nodes in the tree.
// This method should only be invoked when IsDirty() is false.
func (n *Node) Snapshot(
	minVT *persistencespb.VersionedTransition,
) NodesSnapshot {
	panic("not implemented")
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
	panic("not implemented")
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
	snapshot NodesSnapshot,
) error {
	panic("not implemented")
}

// IsDirty returns true if any node rooted at Node n has been modified.
// The result will be reset to false after a call to CloseTransaction().
func (n *Node) IsDirty() bool {
	return n.dirty
}

func newNode(
	base *nodeBase,
	parent *Node,
) *Node {
	return &Node{
		dirty:    false,
		nodeBase: base,
		parent:   parent,
		children: make(map[string]*Node),
	}
}
