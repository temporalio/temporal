package chasm

import (
	"fmt"
	"reflect"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/softassert"
)

const (
	parentPtrInternalFieldName = "Internal"
)

// ParentPtr is a in-memory pointer to the parent component of a CHASM component.
//
// CHASM map is not a component, so if a component is inside a map, its ParentPtr
// will point to the nearest ancestor component that is not a map.
//
// ParentPtr is only initialized and available for use **after** the transition that
// creates the component using ParentPtr is completed.
type ParentPtr[T any] struct {
	// Exporting this field as this generic struct needs to be created via reflection,
	// and reflection can't set private fields.
	Internal parentPtrInternal
}

type parentPtrInternal struct {
	// Storing currentNode instead of parent component Node here so that
	// we can differentiate between root node and non-initialized ParentPtr.
	currentNode *Node
}

// Get returns the parent component, deserializing it if necessary.
// Panics rather than returning an error, as errors are supposed to be handled by the framework as opposed to the
// application.
func (p ParentPtr[T]) Get(chasmContext Context) T {
	vT, ok := p.TryGet(chasmContext)
	if !ok {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(serviceerror.NewInternal("expect parent component value but got nil"))
	}
	return vT
}

// TryGet returns the parent component and a boolean indicating if the value was found,
// deserializing if necessary.
// Panics rather than returning an error, as errors are supposed to be handled by the framework as opposed to the
// application.
func (p ParentPtr[T]) TryGet(chasmContext Context) (T, bool) {
	var nilT T
	parent, ok := p.parentNode()
	if !ok {
		return nilT, false
	}

	if err := parent.prepareComponentValue(chasmContext); err != nil {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(err)
	}

	if parent.value == nil {
		return nilT, false
	}

	vT, isT := parent.value.(T)
	if !isT {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(serviceerror.NewInternalf("parent component value doesn't implement %s", reflect.TypeFor[T]().Name()))
	}
	return vT, true
}

// parentNode returns the nearest ancestor node that is a component, skipping CHASM maps, and a
// boolean indicating whether one was found.
func (p ParentPtr[T]) parentNode() (*Node, bool) {
	if p.Internal.currentNode == nil {
		// ParentPtr not initialized
		return nil, false
	}

	parent := p.Internal.currentNode.parent
	if parent == nil {
		return nil, false
	}

	for parent.isMap() {
		parent = parent.parent
		if parent == nil {
			encodedPath, _ := p.Internal.currentNode.getEncodedPath()
			// nolint:forbidigo // Panic is intended here for framework error handling.
			panic(softassert.UnexpectedInternalErr(
				p.Internal.currentNode.logger,
				"unable to find parent component for CHASM component inside a map",
				fmt.Errorf("child node name: %s", encodedPath),
			))
		}
	}

	if !parent.isComponent() {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(softassert.UnexpectedInternalErr(
			parent.logger,
			"unexpected CHASM node that has a child component",
			fmt.Errorf("node %s, node metadata: %s",
				parent.nodeName,
				parent.serializedNode.GetMetadata().String(),
			),
		))
	}

	return parent, true
}

// Fqn returns the fully qualified name that the parent component is registered under, e.g.
// "workflow.workflow". Returns "" if the ParentPtr is not initialized or the parent's type is not registered.
func (p ParentPtr[T]) Fqn() string {
	parent, ok := p.parentNode()
	if !ok {
		return ""
	}

	if typeID := parent.serializedNode.GetMetadata().GetComponentAttributes().GetTypeId(); typeID != 0 {
		if fqn, ok := parent.registry.ComponentFqnByID(typeID); ok {
			return fqn
		}
	}
	if rc, ok := parent.registry.componentFor(parent.value); ok {
		return rc.fqType()
	}

	softassert.Fail(parent.logger, "parent component type is not registered with CHASM")
	return ""
}
