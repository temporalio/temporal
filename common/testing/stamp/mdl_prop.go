package stamp

import (
	"reflect"
	"sync"
)

var (
	propClonerType            = reflect.TypeFor[propCloner]()
	_              propCloner = &Prop[any]{}
)

type (
	Prop[T any] struct {
		propMetadata
		lock sync.RWMutex
		val  T
	}
	Marker       = Prop[bool]
	propMetadata struct {
		owner modelWrapper
		name  string
		t     reflect.Type
		// TODO: description
	}
	propCloner interface {
		createFromMetadata(propMetadata) any // returns *Prop[T]
	}
	propAccessor interface {
		getVal() any
		getName() string
		getOwner() modelWrapper
	}
)

func (p propMetadata) getName() string {
	return p.name
}

func (p propMetadata) getOwner() modelWrapper {
	return p.owner
}

func (p Prop[T]) createFromMetadata(metadata propMetadata) any {
	return Prop[T]{
		propMetadata: metadata,
	}
}

func (p Prop[T]) String() string {
	return p.name
}

func (p *Prop[T]) getVal() any {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.val
}

func (p *Prop[T]) Set(val T) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.val = val
}
