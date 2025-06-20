package chasm

import (
	"reflect"
)

type (
	RegistrableTask struct {
		taskType        string
		library         namer
		goType          reflect.Type
		componentGoType reflect.Type // It is not clear how this one is used.
		validator       any
		handler         any
		isPureTask      bool
	}

	RegistrableTaskOption func(*RegistrableTask)
)

// NOTE: C is not Component but any.
func NewRegistrableSideEffectTask[C any, T any](
	taskType string,
	validator TaskValidator[C, T],
	handler SideEffectTaskExecutor[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	return newRegistrableTask(
		taskType,
		reflect.TypeFor[T](),
		reflect.TypeFor[C](),
		validator,
		handler,
		false,
		opts...,
	)
}

func NewRegistrablePureTask[C any, T any](
	taskType string,
	validator TaskValidator[C, T],
	handler PureTaskExecutor[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	return newRegistrableTask(
		taskType,
		reflect.TypeFor[T](),
		reflect.TypeFor[C](),
		validator,
		handler,
		true,
		opts...,
	)
}

func newRegistrableTask(
	taskType string,
	goType, componentGoType reflect.Type,
	validator, handler any,
	isPureTask bool,
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	rt := &RegistrableTask{
		taskType:        taskType,
		goType:          goType,
		componentGoType: componentGoType,
		validator:       validator,
		handler:         handler,
		isPureTask:      isPureTask,
	}

	for _, opt := range opts {
		opt(rt)
	}

	return rt
}

// fqType returns the fully qualified name of the task, which is a combination of
// the library name and the task type. This is used to uniquely identify
// the task in the registry.
func (rt RegistrableTask) fqType() string {
	if rt.library == nil {
		// this should never happen because the task is only accessible from the library.
		panic("task is not registered to a library")
	}
	return fullyQualifiedName(rt.library.Name(), rt.taskType)
}
