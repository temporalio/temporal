package chasm

import (
	"fmt"
	"reflect"
)

type (
	RegistrableTask struct {
		taskType        string
		goType          reflect.Type
		componentGoType reflect.Type  // It is not clear how this one is used.
		validateFn      reflect.Value // The Validate() method of the TaskValidator interface.
		executeFn       reflect.Value // The Execute() method of the TaskExecutor interface.
		isPureTask      bool

		// Those two fields are initialized when the component is registered to a library.
		library    namer
		taskTypeID uint32
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

func (rt *RegistrableTask) registerToLibrary(
	library namer,
) (string, uint32, error) {
	if rt.library != nil {
		return "", 0, fmt.Errorf("task %s is already registered in library %s", rt.taskType, rt.library.Name())
	}

	rt.library = library

	fqn := rt.fqType()
	rt.taskTypeID = generateTypeID(fqn)
	return fqn, rt.taskTypeID, nil
}

// GoType returns the reflect.Type of the task's Go struct.
func (rt *RegistrableTask) GoType() reflect.Type {
	return rt.goType
}

// fqType returns the fully qualified name of the task, which is a combination of
// the library name and the task type. This is used to uniquely identify
// the task in the registry.
func (rt *RegistrableTask) fqType() string {
	if rt.library == nil {
		// this should never happen because the task is only accessible from the library.
		panic("task is not registered to a library")
	}
	return fullyQualifiedName(rt.library.Name(), rt.taskType)
}
