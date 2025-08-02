package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
)

const (
	// Used by reflection.
	internalFieldName = "Internal"
)

type Field[T any] struct {
	// This struct needs to be created via reflection, but reflection can't set private fields.
	Internal fieldInternal
}

// re. Data v.s. Component.
// Components have behavior and has a lifecycle.
// while Data doesn't and must be attached to a component.
//
// You can define a component just for storing the data,
// that may contain other information like ref count etc.
// most importantly, the framework needs to know when it's safe to delete the data.
// i.e. the lifecycle of that data component reaches completed.
func NewDataField[D proto.Message](
	ctx MutableContext,
	d D,
) Field[D] {
	return Field[D]{
		Internal: newFieldInternalWithValue(fieldTypeData, d),
	}
}

func NewComponentField[C Component](
	ctx MutableContext,
	c C,
	options ...ComponentFieldOption,
) Field[C] {
	return Field[C]{
		Internal: newFieldInternalWithValue(fieldTypeComponent, c),
	}
}

// TODO: The Component|DataPointerTo() implementation below can't handle the case
// where Pointer is created in the NewEntity transition, as the tree structure is
// unknown to the framework yet.
//
// To handle that case, we have to store the Component value in the field when
// the Pointer field is created and resolve the pointer at the end of the transition
// i.e. when closing the transaction.
func ComponentPointerTo[C Component](
	ctx MutableContext,
	c C,
) (Field[C], error) {
	path, err := ctx.componentNodePath(c)
	if err != nil {
		// If we can't resolve the path (e.g., during NewEntity transition),
		// store the component directly for deferred resolution
		return Field[C]{
			Internal: newFieldInternalWithValue(fieldTypeDeferredPointer, c),
		}, nil
	}
	return Field[C]{
		Internal: newFieldInternalWithValue(fieldTypePointer, path),
	}, nil
}

func DataPointerTo[D proto.Message](
	ctx MutableContext,
	d D,
) (Field[D], error) {
	path, err := ctx.dataNodePath(d)
	if err != nil {
		return NewEmptyField[D](), err
	}
	return Field[D]{
		Internal: newFieldInternalWithValue(fieldTypePointer, path),
	}, nil
}

func (f Field[T]) Get(chasmContext Context) (T, error) {
	var nilT T

	// If node is nil, then there is nothing to deserialize from, return value (even if it is also nil).
	if f.Internal.node == nil {
		if f.Internal.v == nil {
			return nilT, nil
		}
		vT, isT := f.Internal.v.(T)
		if !isT {
			return nilT, serviceerror.NewInternalf("internal value doesn't implement %s", reflect.TypeFor[T]().Name())
		}
		return vT, nil
	}

	var nodeValue any
	switch f.Internal.fieldType() {
	case fieldTypeComponent:
		if err := f.Internal.node.prepareComponentValue(chasmContext); err != nil {
			return nilT, err
		}
		nodeValue = f.Internal.node.value
	case fieldTypeData:
		// For data fields, T is always a concrete type.
		if err := f.Internal.node.prepareDataValue(chasmContext, reflect.TypeFor[T]()); err != nil {
			return nilT, err
		}
		nodeValue = f.Internal.node.value
	case fieldTypePointer:
		if err := f.Internal.node.preparePointerValue(chasmContext); err != nil {
			return nilT, err
		}
		//nolint:revive // value is guaranteed to be of type []string.
		path := f.Internal.value().([]string)
		if referencedNode, found := f.Internal.node.root().findNode(path); found {
			fieldT := reflect.TypeFor[T]()
			if fieldT.AssignableTo(protoMessageT) {
				if err := f.Internal.node.prepareDataValue(chasmContext, fieldT); err != nil {
					return nilT, err
				}
			} else {
				if err := referencedNode.prepareComponentValue(chasmContext); err != nil {
					return nilT, err
				}
			}
			nodeValue = referencedNode.value
		}
	case fieldTypeDeferredPointer:
		// For deferred pointers, return the component directly stored in v
		nodeValue = f.Internal.v
	default:
		return nilT, serviceerror.NewInternalf("unsupported field type: %v", f.Internal.fieldType())
	}

	if nodeValue == nil {
		return nilT, nil
	}
	vT, isT := nodeValue.(T)
	if !isT {
		return nilT, serviceerror.NewInternalf("node value doesn't implement %s", reflect.TypeFor[T]().Name())
	}
	return vT, nil
}

func NewEmptyField[T any]() Field[T] {
	return Field[T]{}
}
