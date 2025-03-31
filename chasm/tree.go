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
	"fmt"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/softassert"
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
		nodeName string           // key of this node in parent's children map.

		// Type of attributes controls the type of the node.
		serializedNode *persistencespb.ChasmNode // serialized component | data | collection with metadata
		value          any                       // deserialized component | data | collection

		// TODO: synced flag need to be added here and it should be cleared
		//   when values serializedNode anc value got in-sync.
		//   And deserialization/serialization can be skipped if synced flag is true.

		// TODO: add other necessary fields here, e.g.
		//
		// dirty    bool
	}

	// nodeBase is a set of dependencies and states shared by all nodes in a CHASM tree.
	nodeBase struct {
		registry    *Registry
		timeSource  clock.TimeSource
		backend     NodeBackend
		pathEncoder NodePathEncoder
		logger      log.Logger

		// Mutations accumulated so far in this transaction.
		mutation NodesMutation
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
	}

	root := newNode(base, nil, "")
	if len(serializedNodes) == 0 {
		// If serializedNodes is empty, it means that this new tree.
		// Create empty node with nil value and empty serializedNode.
		err := root.setValue(nil, fieldTypeComponent)
		if err != nil {
			return nil, err
		}
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
	}
	root := newNode(base, nil, "")
	return root
}

// setValue sets node value field and initialize serializedNode field with empty attributes based on fieldType.
func (n *Node) setValue(v any, ft fieldType) error {
	if err := validateValueType(v); err != nil {
		return err
	}

	n.value = v

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
	}
	return nil
}

func validateValueType(v any) error {
	return validateType(reflect.TypeOf(v))
}

func validateType(t reflect.Type) error {
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

func (n *Node) setSerializedNode(
	nodePath []string,
	serializedNode *persistencespb.ChasmNode,
) {
	if len(nodePath) == 0 {
		n.serializedNode = serializedNode
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

// deserialize initializes the node's value from its serializedNode.
// If value is of component type, it initializes every chasm.Field of it and sets node field but not value field
// i.e. it doesn't deserialize recursively and must be called on every node separately.
func (n *Node) deserialize(
	valueT reflect.Type,
) error {
	if err := validateType(valueT); err != nil {
		return err
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
	// TODO: use n.serializedNode.GetComponentAttributes().GetType() instead to support deserialization to interface.
	valueV := reflect.New(valueT.Elem())
	if n.serializedNode.GetData() == nil {
		// serializedNode is empty (has only metadata) => clear value and return.
		return n.setValue(valueV.Interface(), fieldTypeComponent)
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

			value, err := n.unmarshalProto(n.serializedNode.GetData(), fieldT)
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
				// TODO: support chasm.Field[interface], type should go from registry
				//  using childNode.serializedNode.GetComponentAttributes().GetType()
				chasmFieldV := reflect.New(fieldT).Elem()
				internalValue := reflect.ValueOf(fieldInternal{
					node: childNode,
				})
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
	return nil
}

func (n *Node) deserializeDataNode(
	valueT reflect.Type,
) error {
	value, err := n.unmarshalProto(n.serializedNode.GetData(), valueT)
	if err != nil {
		return err
	}

	n.value = value.Interface()
	return nil
}

func (n *Node) unmarshalProto(
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
	defer n.cleanupTransaction()

	panic("not implemented")
	// return n.mutation, nil
}

func (n *Node) cleanupTransaction() {
	n.mutation = NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}
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
			n.mutation.UpdatedNodes[encodedPath] = updatedNode
			node.serializedNode = updatedNode
			node.value = nil

			// Clearing decoded value for ancestor nodes is not necessary because the value field is not referenced directly.
			// Parent node is pointing to the Node struct.
		}
	}

	return nil
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

// IsDirty returns true if any node rooted at Node n has been modified.
// The result will be reset to false after a call to CloseTransaction().
func (n *Node) IsDirty() bool {
	panic("not implemented")
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
