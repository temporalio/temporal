// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package hsm

import (
	"errors"
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// ErrStateMachineNotFound is returned when looking up a non-existing state machine in a [Node] or a [Collection].
var ErrStateMachineNotFound = errors.New("state machine not found")

// ErrStateMachineAlreadyExists is returned when trying to add a state machine with an ID that already exists in a [Collection].
var ErrStateMachineAlreadyExists = errors.New("state machine already exists")

// ErrIncompatibleType is returned when trying to cast a state machine's data to a type that it is incompatible with.
var ErrIncompatibleType = errors.New("state machine data was cast into an incompatible type")

// Key is used for looking up a state machine in a [Node].
type Key struct {
	// Type ID of the state machine.
	Type int32
	// ID of the state machine.
	ID string
}

// State machine type.
type MachineType struct {
	// Type ID that is used to minimize the persistence storage space and address a machine (see also [Key]).
	// Type IDs are expected to be immutable as they are used for looking up state machine definitions when loading data
	// from persistence.
	ID int32
	// Human readable name for this type.
	Name string
}

// StateMachineDefinition provides type information and a serializer for a state machine.
type StateMachineDefinition interface {
	Type() MachineType
	// Serialize a state machine into bytes.
	Serialize(any) ([]byte, error)
	// Deserialize a state machine from bytes.
	Deserialize([]byte) (any, error)
}

// cachedMachine contains deserialized data and state for a state machine in a [Node].
type cachedMachine struct {
	// An indicator that the data has not yet been loaded or has been marked stale and should be deserialized again.
	dataLoaded bool
	// Deserialized data.
	data any
	// Cached children.
	children map[Key]*Node
	// Outputs of all transitions in the current transaction.
	outputs []TransitionOutput
}

// Node is a node in a hierarchical state machine tree.
//
// It holds a persistent representation of itself and maintains an in-memory cache of deserialized data and child nodes.
// Node data should not be manipulated directly and should only be done using [MachineTransition] or
// [Collection.Transtion] to ensure the tree tracks dirty states and update transition counts.
type Node struct {
	// Key of this node in parent's map. Empty if node is the root.
	Key Key
	// Parent node. Nil if current node is the root.
	Parent      *Node
	registry    *Registry
	cache       *cachedMachine
	persistence *persistencespb.StateMachineNode
	definition  StateMachineDefinition
}

// NewRoot creates a new root [Node].
// Children may be provided from persistence to rehydrate the tree.
// Returns [ErrNotRegistered] if the key's type is not registered in the given registry or serialization errors.
func NewRoot(registry *Registry, t int32, data any, children map[int32]*persistencespb.StateMachineMap) (*Node, error) {
	def, ok := registry.Machine(t)
	if !ok {
		return nil, fmt.Errorf("%w: state machine for type: %v", ErrNotRegistered, t)
	}
	serialized, err := def.Serialize(data)
	if err != nil {
		return nil, err
	}
	return &Node{
		definition: def,
		registry:   registry,
		persistence: &persistencespb.StateMachineNode{
			Children: children,
			Data:     serialized,
		},
		cache: &cachedMachine{
			dataLoaded: true,
			data:       data,
			children:   make(map[Key]*Node),
		},
	}, nil
}

// Dirty returns true if any of the tree's state machines have transitioned.
func (n *Node) Dirty() bool {
	if len(n.cache.outputs) > 0 {
		return true
	}
	for _, child := range n.cache.children {
		if child.Dirty() {
			return true
		}
	}
	return false
}

type PathAndOutputs struct {
	Path    []Key
	Outputs []TransitionOutput
}

func (n *Node) Path() []Key {
	if n.Parent == nil {
		return []Key{}
	}
	return append(n.Parent.Path(), n.Key)
}

// Outputs returns all outputs produced by transitions on this tree.
func (n *Node) Outputs() []PathAndOutputs {
	var paos []PathAndOutputs
	if len(n.cache.outputs) > 0 {
		paos = append(paos, PathAndOutputs{Path: n.Path(), Outputs: n.cache.outputs})
	}
	for _, child := range n.cache.children {
		paos = append(paos, child.Outputs()...)
	}
	return paos
}

// ClearTransactionState resets all transition outputs in the tree.
// This should be called at the end of every transaction where the transitions are performed to avoid emitting duplicate
// transition outputs.
func (n *Node) ClearTransactionState() {
	n.cache.outputs = nil
	for _, child := range n.cache.children {
		child.ClearTransactionState()
	}
}

// Child recursively gets a child for the given path.
func (n *Node) Child(path []Key) (*Node, error) {
	if len(path) == 0 {
		return n, nil
	}
	key, rest := path[0], path[1:]
	if child, ok := n.cache.children[key]; ok {
		return child.Child(rest)
	}
	def, ok := n.registry.Machine(key.Type)
	if !ok {
		return nil, fmt.Errorf("%w: state machine for type: %v", ErrNotRegistered, key.Type)
	}
	machines, ok := n.persistence.Children[key.Type]
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrStateMachineNotFound, key)
	}
	machine, ok := machines.MachinesById[key.ID]
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrStateMachineNotFound, key)
	}
	child := &Node{
		Key:        key,
		Parent:     n,
		registry:   n.registry,
		definition: def,
		cache: &cachedMachine{
			children: make(map[Key]*Node),
		},
		persistence: machine,
	}
	n.cache.children[key] = child
	return child.Child(rest)
}

// AddChild adds an immediate child to a node, serializing the given data.
// Returns [ErrStateMachineAlreadyExists] if a child with the given key already exists, [ErrNotRegistered] if the key's
// type is not found in the node's state machine registry and serialization errors.
func (n *Node) AddChild(key Key, data any) (*Node, error) {
	machines, ok := n.persistence.Children[key.Type]
	if ok {
		if _, ok = machines.MachinesById[key.ID]; ok {
			if ok {
				return nil, fmt.Errorf("%w: %v", ErrStateMachineAlreadyExists, key)
			}
		}
	}
	def, ok := n.registry.Machine(key.Type)
	if !ok {
		return nil, fmt.Errorf("%w: state machine for type: %v", ErrNotRegistered, key.Type)
	}
	serialized, err := def.Serialize(data)
	if err != nil {
		return nil, err
	}
	node := &Node{
		Key:        key,
		Parent:     n,
		definition: def,
		registry:   n.registry,
		persistence: &persistencespb.StateMachineNode{
			Children: make(map[int32]*persistencespb.StateMachineMap),
			Data:     serialized,
		},
		cache: &cachedMachine{
			dataLoaded: true,
			data:       data,
			children:   make(map[Key]*Node),
		},
	}
	n.cache.children[key] = node
	children, ok := n.persistence.Children[key.Type]
	if !ok {
		children = &persistencespb.StateMachineMap{MachinesById: make(map[string]*persistencespb.StateMachineNode)}
		n.persistence.Children[key.Type] = children
	}
	children.MachinesById[key.ID] = node.persistence
	return node, nil
}

// MachineData deserializes the persistent state machine's data, casts it to type T, and returns it.
// Returns an error when deserialization or casting fails.
func MachineData[T any](n *Node) (T, error) {
	var t T
	if n.cache.dataLoaded {
		if t, ok := n.cache.data.(T); ok {
			return t, nil
		}
		return t, ErrIncompatibleType
	}
	a, err := n.definition.Deserialize(n.persistence.Data)
	if err != nil {
		return t, err
	}
	n.cache.data = a
	n.cache.dataLoaded = true

	if t, ok := a.(T); ok {
		return t, nil
	}
	return t, ErrIncompatibleType
}

// TransitionCount returns the transition count for the state machine contained in this node.
func (n *Node) TransitionCount() int64 {
	return n.persistence.TransitionCount
}

// MachineTransition runs the given transitionFn on a machine's data for the given key.
// It updates the state machine's metadata and marks the entry as dirty in the node's cache.
// If the transition fails, the changes are rolled back and no state is mutated.
func MachineTransition[T any](n *Node, transitionFn func(T) (TransitionOutput, error)) (retErr error) {
	data, err := MachineData[T](n)
	if err != nil {
		return err
	}
	n.persistence.TransitionCount++
	// Rollback on error
	defer func() {
		if retErr != nil {
			n.persistence.TransitionCount--
			// Force reloading data.
			n.cache.dataLoaded = false
		}
	}()
	output, err := transitionFn(data)
	if err != nil {
		return err
	}
	serialized, err := n.definition.Serialize(data)
	if err != nil {
		return err
	}
	n.persistence.Data = serialized
	n.cache.outputs = append(n.cache.outputs, output)
	return nil
}

// A Collection of similarly typed sibling state machines.
type Collection[T any] struct {
	// The type of machines stored in this collection.
	Type int32
	node *Node
}

// NewCollection creates a new [Collection].
func NewCollection[T any](node *Node, stateMachineType int32) Collection[T] {
	return Collection[T]{
		Type: stateMachineType,
		node: node,
	}
}

// Node gets an [Node] for a given state machine ID.
func (c Collection[T]) Node(stateMachineID string) (*Node, error) {
	return c.node.Child([]Key{{Type: c.Type, ID: stateMachineID}})
}

// List returns all nodes in this collection.
func (c Collection[T]) List() []*Node {
	machines, ok := c.node.persistence.Children[c.Type]
	if !ok {
		return nil
	}
	nodes := make([]*Node, 0, len(machines.MachinesById))
	for id := range machines.MachinesById {
		node, err := c.node.Child([]Key{{Type: c.Type, ID: id}})
		if err != nil {
			panic("expected child to be present")
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// Add adds a node to the collection as a child of the collection's underlying [Node].
func (c Collection[T]) Add(stateMachineID string, data T) (*Node, error) {
	return c.node.AddChild(Key{Type: c.Type, ID: stateMachineID}, data)
}

// Data gets the data for a given state machine ID.
func (c Collection[T]) Data(stateMachineID string) (T, error) {
	node, err := c.Node(stateMachineID)
	if err != nil {
		var zero T
		return zero, err
	}
	return MachineData[T](node)
}

// Transition transitions a machine by ID.
func (c Collection[T]) Transition(stateMachineID string, transitionFn func(T) (TransitionOutput, error)) error {
	node, err := c.Node(stateMachineID)
	if err != nil {
		return err
	}
	return MachineTransition(node, transitionFn)
}
