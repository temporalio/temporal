package chasm

import (
	"context"
	"fmt"
	"reflect"
)

type (
	RegistrableTask struct {
		taskType                string
		goType                  reflect.Type
		componentGoType         reflect.Type // It is not clear how this one is used.
		validateFn              validateFn
		pureTaskExecuteFn       pureTaskExecuteFn
		sideEffectTaskExecuteFn sideEffectTaskExecuteFn
		sideEffectTaskDiscardFn sideEffectTaskDiscardFn
		isPureTask              bool
		sideEffectTaskGroup     string

		// Those two fields are initialized when the component is registered to a library.
		library    namer
		taskTypeID uint32
	}

	RegistrableTaskOption func(*RegistrableTask)

	validateFn              func(Context, any, TaskAttributes, any, *Registry) (bool, error)
	pureTaskExecuteFn       func(MutableContext, any, TaskAttributes, any, *Registry) error
	sideEffectTaskExecuteFn func(context.Context, ComponentRef, TaskAttributes, any) error
	sideEffectTaskDiscardFn func(context.Context, ComponentRef, TaskAttributes, any) error
)

// NewRegistrableSideEffectTask creates a new registrable side-effect task. NOTE: C is not Component but any.
// The handler's Discard method is called on standby clusters when a task has been pending past the discard delay.
func NewRegistrableSideEffectTask[C any, T any](
	taskType string,
	handler SideEffectTaskHandler[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	return newRegistrableTask(
		taskType,
		reflect.TypeFor[T](),
		reflect.TypeFor[C](),
		func(
			ctx Context,
			component any,
			taskAttrs TaskAttributes,
			taskData any,
			registry *Registry,
		) (bool, error) {
			return handler.Validate(
				ctx,
				component.(C),
				taskAttrs,
				taskData.(T),
			)
		},
		nil, // pureTaskExecuteFn is not used for side effect tasks
		func(
			ctx context.Context,
			componentRef ComponentRef,
			taskAttrs TaskAttributes,
			taskData any,
		) error {
			return handler.Execute(ctx, componentRef, taskAttrs, taskData.(T))
		},
		false,
		func(ctx context.Context, ref ComponentRef, attrs TaskAttributes, task any) error {
			return handler.Discard(ctx, ref, attrs, task.(T))
		},
		handler.TaskGroup(),
		opts...,
	)
}

func NewRegistrablePureTask[C any, T any](
	taskType string,
	handler PureTaskHandler[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	return newRegistrableTask(
		taskType,
		reflect.TypeFor[T](),
		reflect.TypeFor[C](),
		func(
			ctx Context,
			component any,
			taskAttrs TaskAttributes,
			taskData any,
			registry *Registry,
		) (bool, error) {
			return handler.Validate(
				ctx,
				component.(C),
				taskAttrs,
				taskData.(T),
			)
		},
		func(
			ctx MutableContext,
			component any,
			taskAttrs TaskAttributes,
			taskData any,
			registry *Registry,
		) error {
			return handler.Execute(
				ctx,
				component.(C),
				taskAttrs,
				taskData.(T),
			)
		},
		nil, // sideEffectTaskExecuteFn is not used for pure tasks
		true,
		nil, // sideEffectTaskDiscardFn is not used for pure tasks
		"",  // sideEffectTaskGroup is not used for pure tasks
		opts...,
	)
}

func newRegistrableTask(
	taskType string,
	goType, componentGoType reflect.Type,
	validateFn validateFn,
	pureTaskExecuteFn pureTaskExecuteFn,
	sideEffectTaskExecuteFn sideEffectTaskExecuteFn,
	isPureTask bool,
	sideEffectTaskDiscardFn sideEffectTaskDiscardFn,
	sideEffectTaskGroup string,
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	rt := &RegistrableTask{
		taskType:                taskType,
		goType:                  goType,
		componentGoType:         componentGoType,
		validateFn:              validateFn,
		pureTaskExecuteFn:       pureTaskExecuteFn,
		sideEffectTaskExecuteFn: sideEffectTaskExecuteFn,
		sideEffectTaskDiscardFn: sideEffectTaskDiscardFn,
		isPureTask:              isPureTask,
		sideEffectTaskGroup:     sideEffectTaskGroup,
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
	rt.taskTypeID = GenerateTypeID(fqn)
	return fqn, rt.taskTypeID, nil
}

// TaskGroup returns the task group string for this registrable task.
func (rt *RegistrableTask) TaskGroup() string {
	return rt.sideEffectTaskGroup
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
	return FullyQualifiedName(rt.library.Name(), rt.taskType)
}
