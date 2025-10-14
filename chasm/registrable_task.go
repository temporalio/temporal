package chasm

import (
	"reflect"
)

type (
	RegistrableTask struct {
		taskType        string
		library         namer
		goType          reflect.Type
		componentGoType reflect.Type  // It is not clear how this one is used.
		validateFn      reflect.Value // The Validate() method of the TaskValidator interface.
		executeFn       reflect.Value // The Execute() method of the TaskExecutor interface.
		isPureTask      bool
	}

	RegistrableTaskOption func(*RegistrableTask)
)

// NOTE: C is not Component but any.
func NewRegistrableSideEffectTask[C any, T any](
	taskType string,
	validator TaskValidator[C, T],
	executor SideEffectTaskExecutor[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	return newRegistrableTask(
		taskType,
		reflect.TypeFor[T](),
		reflect.TypeFor[C](),
		reflect.ValueOf(validator).MethodByName("Validate"),
		reflect.ValueOf(executor).MethodByName("Execute"),
		false,
		opts...,
	)
}

func NewRegistrablePureTask[C any, T any](
	taskType string,
	validator TaskValidator[C, T],
	executor PureTaskExecutor[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	return newRegistrableTask(
		taskType,
		reflect.TypeFor[T](),
		reflect.TypeFor[C](),
		reflect.ValueOf(validator).MethodByName("Validate"),
		reflect.ValueOf(executor).MethodByName("Execute"),
		true,
		opts...,
	)
}

func newRegistrableTask(
	taskType string,
	goType, componentGoType reflect.Type,
	validateFn, executeFn reflect.Value,
	isPureTask bool,
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	rt := &RegistrableTask{
		taskType:        taskType,
		goType:          goType,
		componentGoType: componentGoType,
		validateFn:      validateFn,
		executeFn:       executeFn,
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
