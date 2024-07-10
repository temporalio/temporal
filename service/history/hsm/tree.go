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
	"context"
	"errors"
	"fmt"
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/service/history/consts"
)

// ErrStateMachineNotFound is returned when looking up a non-existing state machine in a [Node] or a [Collection].
var ErrStateMachineNotFound = errors.New("state machine not found")

// ErrStateMachineAlreadyExists is returned when trying to add a state machine with an ID that already exists in a [Collection].
var ErrStateMachineAlreadyExists = errors.New("state machine already exists")

// ErrIncompatibleType is returned when trying to cast a state machine's data to a type that it is incompatible with.
var ErrIncompatibleType = errors.New("state machine data was cast into an incompatible type")

// ErrInitialTransitionMismatch is returned when the initial failover version or transition count of a node does not match the incoming node upon sync.
var ErrInitialTransitionMismatch = errors.New("node initial failover version or transition count mismatch")

// Key is used for looking up a state machine in a [Node].
type Key struct {
	// Type of the state machine.
	Type string
	// ID of the state machine.
	ID string
}

// StateMachineDefinition provides type information and a serializer for a state machine.
type StateMachineDefinition interface {
	Type() string
	// Serialize a state machine into bytes.
	Serialize(any) ([]byte, error)
	// Deserialize a state machine from bytes.
	Deserialize([]byte) (any, error)
	// CompareState compares two state objects. It should return 0 if the states are equal, a positive number if the
	// first state is considered newer, a negative number if the second state is considered newer.
	// TODO: Remove this method and implementations once transition history is fully implemented. For now, we have to
	// rely on each component to tell the framework which state is newer and if sync state can overwrite the states in
	// the standby cluster.
	CompareState(any, any) (int, error)
}

// cachedMachine contains deserialized data and state for a state machine in a [Node].
type cachedMachine struct {
	// An indicator that the data has not yet been loaded or has been marked stale and should be deserialized again.
	dataLoaded bool
	// Deserialized data.
	data any
	// Cached children.
	children map[Key]*Node
	// A flag that indicates the cached machine is dirty.
	dirty bool
	// Outputs of all transitions in the current transaction.
	outputs []TransitionOutputWithCount
}

// NodeBackend is a concrete implementation to support interacting with the underlying platform.
// It currently has only a single implementation - workflow mutable state.
type NodeBackend interface {
	// AddHistoryEvent adds a history event to be committed at the end of the current transaction.
	AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent
	// LoadHistoryEvent loads a history event by token generated via [GenerateEventLoadToken].
	LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error)
	// GetCurrentVersion returns the current namespace failover version.
	GetCurrentVersion() int64
	// NextTransitionCount returns the current state transition count from the state transition history.
	NextTransitionCount() int64
}

// EventIDFromToken gets the event ID associated with an event load token.
func EventIDFromToken(token []byte) (int64, error) {
	ref := &tokenspb.HistoryEventRef{}
	err := proto.Unmarshal(token, ref)
	return ref.EventId, err
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
	backend     NodeBackend
}

// NewRoot creates a new root [Node].
// Children may be provided from persistence to rehydrate the tree.
// Returns [ErrNotRegistered] if the key's type is not registered in the given registry or serialization errors.
func NewRoot(registry *Registry, t string, data any, children map[string]*persistencespb.StateMachineMap, backend NodeBackend) (*Node, error) {
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
			Children:                      children,
			Data:                          serialized,
			InitialVersionedTransition:    &persistencespb.VersionedTransition{},
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{},
			TransitionCount:               0,
		},
		cache: &cachedMachine{
			dataLoaded: true,
			data:       data,
			children:   make(map[Key]*Node),
		},
		backend: backend,
	}, nil
}

// Dirty returns true if any of the tree's state machines have transitioned.
func (n *Node) Dirty() bool {
	if n.cache.dirty {
		return true
	}
	for _, child := range n.cache.children {
		if child.Dirty() {
			return true
		}
	}
	return false
}

type TransitionOutputWithCount struct {
	TransitionOutput
	TransitionCount int64
}
type PathAndOutputs struct {
	Path    []Key
	Outputs []TransitionOutputWithCount
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
	n.cache.dirty = false
	n.cache.outputs = nil
	for _, child := range n.cache.children {
		child.ClearTransactionState()
	}
}

// Walk applies the given function to all nodes rooted at the current node.
// Returns after successfully applying the function to all nodes or first error.
func (n *Node) Walk(fn func(*Node) error) error {
	if n == nil {
		return nil
	}

	if err := fn(n); err != nil {
		return err
	}

	for childType := range n.persistence.Children {
		childNodes := NewCollection[any](n, childType).List()
		for _, child := range childNodes {
			if err := child.Walk(fn); err != nil {
				return err
			}
		}
	}

	return nil
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
		backend:     n.backend,
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

	nextVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
		// The transition count for the backend is only incremented when closing the current transaction,
		// but any change to state machine node is a state transtion,
		// so we can safely using next transition count here is safe.
		TransitionCount: n.backend.NextTransitionCount(),
	}
	node := &Node{
		Key:        key,
		Parent:     n,
		definition: def,
		registry:   n.registry,
		persistence: &persistencespb.StateMachineNode{
			Children:                      make(map[string]*persistencespb.StateMachineMap),
			Data:                          serialized,
			InitialVersionedTransition:    nextVersionedTransition,
			LastUpdateVersionedTransition: nextVersionedTransition,
			TransitionCount:               0,
		},
		cache: &cachedMachine{
			dataLoaded: true,
			data:       data,
			dirty:      true,
			children:   make(map[Key]*Node),
		},
		backend: n.backend,
	}
	n.cache.children[key] = node
	children, ok := n.persistence.Children[key.Type]
	if !ok {
		children = &persistencespb.StateMachineMap{MachinesById: make(map[string]*persistencespb.StateMachineNode)}
		// Children may be nil if the map was empty and the proto message we serialized and deserialized.
		if n.persistence.Children == nil {
			n.persistence.Children = make(map[string]*persistencespb.StateMachineMap, 1)
		}
		n.persistence.Children[key.Type] = children
	}
	children.MachinesById[key.ID] = node.persistence
	return node, nil
}

// AddHistoryEvent adds a history event to be committed at the end of the current transaction.
// Must be called within an [Environment.Access] function block with write access.
func (n *Node) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	return n.backend.AddHistoryEvent(t, setAttributes)
}

// Load a history event by token generated via [GenerateEventLoadToken].
// Must be called within an [Environment.Access] function block with either read or write access.
func (n *Node) LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error) {
	return n.backend.LoadHistoryEvent(ctx, token)
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

// CheckRunning has two modes of operation:
// 1. If the node is **not** attached to a workflow (not yet supported), it returns nil.
// 2. If the node is attached to a workflow, it verifies that the workflow execution is running, and returns
// ErrWorkflowCompleted or nil.
//
// May return other errors returned from [MachineData].
func (n *Node) CheckRunning() error {
	root := n
	for root.Parent != nil {
		root = root.Parent
	}

	execution, err := MachineData[interface{ IsWorkflowExecutionRunning() bool }](root)
	if err != nil {
		if errors.Is(err, ErrIncompatibleType) {
			// The machine is not attached to a workflow. It is currently assumed to be running.
			return nil
		}
		return err
	}
	if !execution.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowCompleted
	}
	return nil
}

// InternalRepr returns the internal persistence representation of this node.
// Meant to be used by the framework, **not** by components.
func (n *Node) InternalRepr() *persistencespb.StateMachineNode {
	return n.persistence
}

// CompareState compare current node state with the incoming node state.
// Returns 0 if the states are equal,
// a positive number if the current state is considered newer,
// a negative number if the incoming state is considered newer.
// Meant to be used by the framework, **not** by components.
// TODO: remove once transition history is enabled.
func (n *Node) CompareState(incomingNode *Node) (int, error) {
	currentState, err := MachineData[any](n)
	if err != nil {
		return 0, err
	}
	incomingState, err := MachineData[any](incomingNode)
	if err != nil {
		return 0, err
	}
	return n.definition.CompareState(currentState, incomingState)
}

// Sync updates the state of the current node to that of the incoming node.
// Meant to be used by the framework, **not** by components.
func (n *Node) Sync(incomingNode *Node) error {
	incomingInternalRepr := incomingNode.InternalRepr()

	currentInitialVersionedTransition := n.InternalRepr().InitialVersionedTransition
	incomingInitialVersionedTransition := incomingNode.InternalRepr().InitialVersionedTransition
	if currentInitialVersionedTransition.NamespaceFailoverVersion !=
		incomingInitialVersionedTransition.NamespaceFailoverVersion {
		return ErrInitialTransitionMismatch
	}
	if currentInitialVersionedTransition.TransitionCount != 0 &&
		incomingInitialVersionedTransition.TransitionCount != 0 &&
		currentInitialVersionedTransition.TransitionCount !=
			incomingInitialVersionedTransition.TransitionCount {
		return ErrInitialTransitionMismatch
	}

	n.persistence.Data = incomingInternalRepr.Data
	// do not sync children, we are just syncing the current node
	// do not sync transitionCount, that is cluster local information

	// force reload data
	n.cache.dataLoaded = false

	// reuse MachineTransition for
	// - marking the node as dirty
	// - generate transition outputs (tasks)
	// - update transition count
	if err := MachineTransition(n, func(taskRegenerator TaskRegenerator) (TransitionOutput, error) {
		tasks, err := taskRegenerator.RegenerateTasks(n)
		return TransitionOutput{
			Tasks: tasks,
		}, err
	}); err != nil {
		return err
	}

	// sync LastUpdateVersionedTransition last as MachineTransition can't correctly handle it.
	n.persistence.LastUpdateVersionedTransition = incomingInternalRepr.LastUpdateVersionedTransition
	return nil
}

// MachineTransition runs the given transitionFn on a machine's data for the given key.
// It updates the state machine's metadata and marks the entry as dirty in the node's cache.
// If the transition fails, the changes are rolled back and no state is mutated.
func MachineTransition[T any](n *Node, transitionFn func(T) (TransitionOutput, error)) (retErr error) {
	data, err := MachineData[T](n)
	if err != nil {
		return err
	}
	// Update the transition counts before applying the transition function in case the transition function needs to
	// generate references to this node.
	n.persistence.TransitionCount++
	prevLastUpdatedVersionedTransition := n.persistence.LastUpdateVersionedTransition
	n.persistence.LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: n.backend.GetCurrentVersion(),
		// The transition count for the backend is only incremented when closing the current transaction,
		// but any change to state machine node is a state transtion,
		// so we can safely using next transition count here is safe.
		TransitionCount: n.backend.NextTransitionCount(),
	}
	// Rollback on error
	defer func() {
		if retErr != nil {
			n.persistence.TransitionCount--
			n.persistence.LastUpdateVersionedTransition = prevLastUpdatedVersionedTransition
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
	n.cache.dirty = true
	outputWithCount := TransitionOutputWithCount{
		TransitionOutput: output,
		TransitionCount:  n.persistence.TransitionCount,
	}
	n.cache.outputs = append(n.cache.outputs, outputWithCount)
	return nil
}

// A Collection of similarly typed sibling state machines.
type Collection[T any] struct {
	// The type of machines stored in this collection.
	Type string
	node *Node
}

// NewCollection creates a new [Collection].
func NewCollection[T any](node *Node, stateMachineType string) Collection[T] {
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

// Size returns the number of machines in this collection.
func (c Collection[T]) Size() int {
	machines, ok := c.node.persistence.Children[c.Type]
	if !ok {
		return 0
	}
	return len(machines.MachinesById)
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

// GenerateEventLoadToken generates a token for loading a history event from an [Environment].
// Events should typically be immutable making this function safe to call outside of an [Environment.Access] call.
func GenerateEventLoadToken(event *historypb.HistoryEvent) ([]byte, error) {
	attrs := reflect.ValueOf(event.Attributes).Elem()

	// Attributes is always a struct with a single field (e.g: HistoryEvent_NexusOperationScheduledEventAttributes)
	if attrs.Kind() != reflect.Struct || attrs.NumField() != 1 {
		return nil, serviceerror.NewInternal(fmt.Sprintf("got an invalid event structure: %v", event.EventType))
	}

	f := attrs.Field(0).Interface()

	var eventBatchID int64
	if getter, ok := f.(interface{ GetWorkflowTaskCompletedEventId() int64 }); ok {
		// Command-Events always have a WorkflowTaskCompletedEventId field that is equal to the batch ID.
		eventBatchID = getter.GetWorkflowTaskCompletedEventId()
	} else if attrs := event.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
		// WFEStarted is always stored in the first batch of events.
		eventBatchID = 1
	} else {
		// By default events aren't referenceable as they may end up buffered.
		// This limitation may be relaxed later and the platform would need a way to fix references to buffered events.
		return nil, serviceerror.NewInternal(fmt.Sprintf("cannot reference event: %v", event.EventType))
	}
	ref := &tokenspb.HistoryEventRef{
		EventId:      event.EventId,
		EventBatchId: eventBatchID,
	}
	return proto.Marshal(ref)
}
