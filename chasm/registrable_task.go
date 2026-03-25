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
// If the executor also implements SideEffectTaskDiscarder, the framework will call Discard instead of silently
// discarding the task on standby clusters after the discard delay.
func NewRegistrableSideEffectTask[C any, T any](
	taskType string,
	validator TaskValidator[C, T],
	executor SideEffectTaskExecutor[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	var discardFn sideEffectTaskDiscardFn
	if discarder, ok := any(executor).(SideEffectTaskDiscarder[T]); ok {
		discardFn = func(ctx context.Context, ref ComponentRef, attrs TaskAttributes, task any) error {
			return discarder.Discard(ctx, ref, attrs, task.(T))
		}
	}

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
			return validator.Validate(
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
			return executor.Execute(ctx, componentRef, taskAttrs, taskData.(T))
		},
		false,
		discardFn,
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
		func(
			ctx Context,
			component any,
			taskAttrs TaskAttributes,
			taskData any,
			registry *Registry,
		) (bool, error) {
			return validator.Validate(
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
			return executor.Execute(
				ctx,
				component.(C),
				taskAttrs,
				taskData.(T),
			)
		},
		nil, // sideEffectTaskExecuteFn is not used for pure tasks
		true,
		nil, // sideEffectTaskDiscardFn is not used for pure tasks
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

// GoType returns the reflect.Type of the task's Go struct.
func (rt *RegistrableTask) GoType() reflect.Type {
	return rt.goType
}

// HasDiscardHandler returns true if the task's executor implements SideEffectTaskDiscarder, meaning it has custom
// discard behavior for standby clusters.
func (rt *RegistrableTask) HasDiscardHandler() bool {
	return rt.sideEffectTaskDiscardFn != nil
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
