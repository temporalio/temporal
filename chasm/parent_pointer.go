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
type ParentPtr[T Component] struct {
	// Exporting this field as this generic struct needs to be created via reflection,
	// and reflection can't set private fields.
	Internal parentPtrInternal
}

type parentPtrInternal struct {
	// Storing currentNode instead of parent component Node here so that
	// we can differentiate between root node and non-initialized ParentPtr.
	currentNode *Node
}

// Get returns the parent component this ParentPtr points to, deserializing it if necessary.
// Panics rather than returning an error, as errors are supposed to be handled by the framework as opposed to the
// application.
func (p ParentPtr[T]) Get(chasmContext Context) T {
	vT, _ := p.TryGet(chasmContext)
	return vT
}

// Get returns the parent component this ParentPtr points to and a boolean indicating if the value was found,
// deserializing if necessary.
// Panics rather than returning an error, as errors are supposed to be handled by the framework as opposed to the
// application.
func (p ParentPtr[T]) TryGet(chasmContext Context) (T, bool) {
	var nilT T
	if p.Internal.currentNode == nil {
		panic(serviceerror.NewInternal("parent pointer not initialized yet"))
	}

	parent := p.Internal.currentNode.parent
	if parent == nil {
		return nilT, false
	}

	for parent.isMap() {
		parent = parent.parent
		if parent == nil {
			encodedPath, _ := p.Internal.currentNode.getEncodedPath()
			panic(softassert.UnexpectedInternalErr(
				p.Internal.currentNode.logger,
				"unable to find parent component for CHASM component inside a map",
				fmt.Errorf("child node name: %s", encodedPath),
			))
		}
	}

	if !parent.isComponent() {
		panic(softassert.UnexpectedInternalErr(
			parent.logger,
			"unexpected CHASM node that has a child component",
			fmt.Errorf("node %s, node metadata: %s",
				parent.nodeName,
				parent.serializedNode.GetMetadata().String(),
			),
		))
	}

	if err := parent.prepareComponentValue(chasmContext); err != nil {
		panic(err)
	}

	if parent.value == nil {
		return nilT, false
	}

	vT, isT := parent.value.(T)
	if !isT {
		panic(serviceerror.NewInternalf("node value doesn't implement %s", reflect.TypeFor[T]().Name()))
	}
	return vT, true
}
