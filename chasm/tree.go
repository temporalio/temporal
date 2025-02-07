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
	"strings"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

type (
	// Node is the in-memory representation of a persisted CHASM node.
	//
	// Node and all its methods are NOT meant to be used by CHASM component authors.
	// They are exported for use by the CHASM engine and underlying MutableState implementation only.
	Node struct {
		*nodeBase

		parent   *Node
		children map[string]*Node
		pNode    *persistencespb.ChasmNode // serialized component/data/collection
		// TODO: what is the better name?
		component any // deserialized component/data/collection
		dirty     bool

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
	pNodes map[string]*persistencespb.ChasmNode, // This is coming from MS map[nodePath]ChasmNode.

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
	for encodedPath, pNode := range pNodes {
		nodePath, err := pathEncoder.Decode(encodedPath)
		if err != nil {
			return nil, err
		}
		root.addChild(nodePath, pNode)
	}

	return root, nil
}

func (n *Node) addChild(
	nodePath []string,
	pNode *persistencespb.ChasmNode,
) {
	if len(nodePath) == 0 {
		n.pNode = pNode
		return
	}

	childName := nodePath[0]
	childNode, ok := n.children[childName]
	if !ok {
		childNode = newNode(n.nodeBase, n)
		n.children[childName] = childNode
	}
	childNode.addChild(nodePath[1:], pNode)
}

// TODO: this is called from CloseTransaction.
func (n *Node) serializeNode(
	mutation *NodesMutation,
	currentPath []string,
) error {
	switch n.pNode.Attributes.(type) {
	case *persistencespb.ChasmNode_ComponentAttributes:
		return n.serializeComponentNode(mutation, currentPath)
	case *persistencespb.ChasmNode_DataAttributes:
		return n.serializeDataNode()
	case *persistencespb.ChasmNode_CollectionAttributes:
		panic("not implemented")
	case *persistencespb.ChasmNode_PointerAttributes:
		panic("not implemented")
	default:
		return serviceerror.NewInternal("unknown pNode type")
	}
}

func (n *Node) serializeComponentNode(
	mutation *NodesMutation,
	currentPath []string,
) error {
	componentT := reflect.TypeOf(n.component)
	componentV := reflect.ValueOf(n.component)
	for componentT.Kind() == reflect.Ptr {
		componentT = componentT.Elem()
		componentV = componentV.Elem()
	}
	if componentT.Kind() != reflect.Struct {
		return serviceerror.NewInternal("only struct (or pointer to struct) is supported for component")
	}

	protoMessageFound := false
	processedChildren := make(map[string]struct{})
	for i := 0; i < componentT.NumField(); i++ {
		fieldV := componentV.Field(i)
		fieldT := fieldV.Type()
		if fieldT.AssignableTo(reflect.TypeFor[proto.Message]()) {
			if protoMessageFound {
				return serviceerror.NewInternal("only one proto field allowed in component")
			}
			protoMessageFound = true

			blob, err := serialization.ProtoEncodeBlob(fieldV.Interface().(proto.Message), enums.ENCODING_TYPE_PROTO3)
			if err != nil {
				return err
			}

			n.pNode.GetComponentAttributes().Data = blob
			n.pNode.GetComponentAttributes().Type = componentT.String()
			if n.pNode.GetInitialVersionedTransition() == nil {
				n.pNode.InitialVersionedTransition = &persistencespb.VersionedTransition{
					TransitionCount:          n.backend.NextTransitionCount(),
					NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
				}
			}
			continue
		}

		// TODO: support chasm name tag on the field
		fieldName := componentT.Field(i).Name
		processedChildren[fieldName] = struct{}{}

		for fieldT.Kind() == reflect.Ptr {
			fieldT = fieldT.Elem()
			fieldV = fieldV.Elem()
		}

		if strings.HasPrefix(fieldT.String(), chasmFieldTypeName) { // not string comparison because chasm.Field is generic
			internal := fieldV.FieldByName(internalFieldName).Interface().(fieldInternal)
			if internal.treeNode != nil {
				// this is not a new tree node, don't need to do anything
				continue
			}

			// new node or replacing an existing node
			switch internal.fieldType {
			case fieldTypeData:
				childN := newNode(n.nodeBase, n)
				childN.pNode = &persistencespb.ChasmNode{
					Attributes: &persistencespb.ChasmNode_DataAttributes{},
				}
				childN.component = internal.component
				n.children[fieldName] = childN
			case fieldTypeComponent:
				childN := newNode(n.nodeBase, n)
				childN.pNode = &persistencespb.ChasmNode{
					Attributes: &persistencespb.ChasmNode_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
					},
				}
				childN.component = internal.component
				n.children[fieldName] = childN
			case fieldTypeComponentPointer:
			case fieldTypeCollection:
			}

			// TODO: set the field with non-nil backing node
		}

		if strings.HasPrefix(fieldT.String(), chasmCollectionTypeName) {
			continue
		}
	}

	// remove deleted children
	// TODO: this needs to be done recursively
	// i.e remove the entire subtree
	for childName := range n.children {
		if _, processedChild := processedChildren[childName]; !processedChild {
			mutation.DeletedNodes[strings.Join(append(currentPath, childName), "/")] = struct{}{}
			delete(n.children, childName)
		}
	}

	// validate existing tasks

	// check dangling tasks

	return nil
}

func (n *Node) serializeDataNode() error {
	componentProto, ok := n.component.(proto.Message)
	if !ok {
		return serviceerror.NewInternal("only support proto.Message as chasm data")
	}

	blob, err := serialization.ProtoEncodeBlob(componentProto, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	n.pNode.GetDataAttributes().Data = blob
	return nil
}

func (n *Node) deserializeNode(
	instanceT reflect.Type,
) error {
	switch n.pNode.Attributes.(type) {
	case *persistencespb.ChasmNode_ComponentAttributes:
		return n.deserializeComponentNode(instanceT)
	case *persistencespb.ChasmNode_DataAttributes:
		return n.deserializeDataNode(instanceT)
	case *persistencespb.ChasmNode_CollectionAttributes:
		panic("not implemented")
	case *persistencespb.ChasmNode_PointerAttributes:
	}
	return nil
}

func (n *Node) deserializeComponentNode(
	instanceT reflect.Type,
) error {
	if instanceT.Kind() != reflect.Struct {
		return serviceerror.NewInternal("only struct is supported for component")
	}

	attr := n.pNode.GetComponentAttributes()

	instanceV := reflect.New(instanceT).Elem()
	protoMessageFound := false
	for i := 0; i < instanceT.NumField(); i++ {
		fieldV := instanceV.Field(i)
		fieldT := fieldV.Type()
		if fieldT.AssignableTo(reflect.TypeFor[proto.Message]()) {
			if protoMessageFound {
				return serviceerror.NewInternal("only one proto field allowed in component")
			}
			protoMessageFound = true

			value, err := n.unmarshalProto(attr.GetData(), fieldT)
			if err != nil {
				return err
			}
			fieldV.Set(value)
			continue
		}

		// TODO: support chasm name tag on the field
		fieldName := instanceT.Field(i).Name

		for fieldT.Kind() == reflect.Ptr {
			fieldT = fieldT.Elem()
			fieldV = fieldV.Elem()
		}

		if strings.HasPrefix(fieldT.String(), chasmFieldTypeName) { // not string comparison because chasm.Field is generic
			chasmFieldV := reflect.New(fieldT).Elem()

			if childNode, found := n.children[fieldName]; found {
				internalValue := reflect.ValueOf(fieldInternal{
					treeNode: childNode,
				})
				chasmFieldV.FieldByName(internalFieldName).Set(internalValue)
			}

			fieldV.Set(chasmFieldV)

			continue
		}

		if strings.HasPrefix(fieldT.String(), chasmCollectionTypeName) {
			// TODO: support collection
			// init the map and populate
			continue
		}

		return serviceerror.NewInternal("unsupported field type in component " + fieldT.String())
	}

	if !protoMessageFound {
		return serviceerror.NewInternal("no proto field found in component")
	}

	n.component = instanceV.Interface()

	return nil
}

func (n *Node) deserializeDataNode(
	instanceType reflect.Type,
) error {
	value, err := n.unmarshalProto(n.pNode.GetDataAttributes().GetData(), instanceType)
	if err != nil {
		return err
	}

	n.component = value.Interface()
	return nil
}

func (n *Node) unmarshalProto(
	dataBlob *common.DataBlob,
	instanceT reflect.Type,
) (reflect.Value, error) {
	if !instanceT.AssignableTo(reflect.TypeFor[proto.Message]()) {
		return reflect.Value{}, serviceerror.NewInternal("only support proto.Message as chasm data")
	}

	var value reflect.Value
	if instanceT.Kind() == reflect.Ptr {
		value = reflect.New(instanceT.Elem())
	} else {
		value = reflect.New(instanceT).Elem()
	}

	if err := serialization.ProtoDecodeBlob(
		dataBlob,
		value.Interface().(proto.Message),
	); err != nil {
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

// CloseTransactionAsMutation is used by MutableState
// to close the transaction and persist mutations into DB.
func (n *Node) CloseTransactionAsMutation() (*NodesMutation, error) {
	panic("not implemented")
}

// CloseTransactionAsSnapshot is used by MutableState
// to close the transaction and persist entire CHASM tree into DB.
func (n *Node) CloseTransactionAsSnapshot() (*NodesSnapshot, error) {
	panic("not implemented")
}

// ApplyMutation is used by replication stack to apply node
// mutations from the source cluster.
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

func newNode(
	base *nodeBase,
	parent *Node,
) *Node {
	return &Node{
		dirty:    true,
		nodeBase: base,
		parent:   parent,
		children: make(map[string]*Node),
	}
}
