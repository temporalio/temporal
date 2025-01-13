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

	dirty bool
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
func NewData[D proto.Message](
	ctx MutableContext,
	d D,
) *Field[D] {
	return &Field[D]{}
}

type componentOptions struct {
	detached bool
}

type ComponentOption func(*componentOptions)

func ComponentOptionDetached() ComponentOption {
	return func(o *componentOptions) {
		o.detached = true
	}
}

func NewComponent[C Component](
	ctx MutableContext,
	c C,
	options ...ComponentOption,
) *Field[C] {
	return &Field[C]{
		Internal: fieldInternal{
			dirty:     true,
			component: reflect.ValueOf(c),
		},
	}
}

func NewComponentPointer[C Component](
	ctx MutableContext,
	c C,
) *Field[C] {
	panic("not implemented")
}

func NewDataPointer[D proto.Message](
	ctx MutableContext,
	d D,
) *Field[D] {
	panic("not implemented")
}

type Collection[T any] map[string]*Field[T]
