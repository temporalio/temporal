package chasm

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

// This struct needs to be create via reflection
// but reflection can't set prviate fields...
type Field[T any] struct {
	Internal fieldInternal
}

type fieldInternal struct {
	fieldType int // data, component, componentPointer
	component reflect.Value

	// backingNode *nodeInfo
}

func (d *Field[T]) Get(Context) (T, error) {
	// remember to handle d == nil case

	panic("not implemented")
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
) *Field[D] {
	return &Field[D]{}
}

type componentFieldOptions struct {
	detached bool
}

type ComponentFieldOption func(*componentFieldOptions)

func ComponentFieldDetached() ComponentFieldOption {
	return func(o *componentFieldOptions) {
		o.detached = true
	}
}

func NewComponentField[C Component](
	ctx MutableContext,
	c C,
	options ...ComponentFieldOption,
) *Field[C] {
	return &Field[C]{
		Internal: fieldInternal{
			component: reflect.ValueOf(c),
		},
	}
}

func NewComponentPointerField[C Component](
	ctx MutableContext,
	c C,
) *Field[C] {
	panic("not implemented")
}

func NewDataPointerField[D proto.Message](
	ctx MutableContext,
	d D,
) *Field[D] {
	panic("not implemented")
}

type Collection[T any] map[string]*Field[T]
