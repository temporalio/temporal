package chasm

import (
	"context"
	"reflect"
)

func (r *Registry) Component(fqn string) (*RegistrableComponent, bool) {
	return r.component(fqn)
}

func (r *Registry) Task(fqn string) (*RegistrableTask, bool) {
	return r.task(fqn)
}

func (r *Registry) ComponentFor(componentInstance any) (*RegistrableComponent, bool) {
	return r.componentFor(componentInstance)
}

func (r *Registry) ComponentOf(componentGoType reflect.Type) (*RegistrableComponent, bool) {
	return r.componentOf(componentGoType)
}

func (r *Registry) TaskFor(taskInstance any) (*RegistrableTask, bool) {
	return r.taskFor(taskInstance)
}

func (r *Registry) TaskOf(taskGoType reflect.Type) (*RegistrableTask, bool) {
	return r.taskOf(taskGoType)
}

func (rc RegistrableComponent) FqType() string {
	return rc.fqType()
}

func (rt RegistrableTask) FqType() string {
	return rt.fqType()
}

func EngineFromContext(ctx context.Context) Engine {
	return engineFromContext(ctx)
}
