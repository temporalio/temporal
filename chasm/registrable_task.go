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
		outboundTaskGroup       string // For grouping on the outbound queue. See [WithTaskGroup] for details.

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
	// If outboundTaskGroup wasn't set on creation default it here,
	// since this is the first place we will have the fqn.
	if rt.outboundTaskGroup == "" {
		rt.outboundTaskGroup = fqn
	}
	return fqn, rt.taskTypeID, nil
}

// TaskGroup returns the side-effect task group for the task.
func (rt *RegistrableTask) TaskGroup() string {
	return rt.outboundTaskGroup
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

// WithTaskGroup sets the task group for the task. The task group is used when
// the side effect's destination is specified for grouping semantics on the outbound queue,
// affects multi-cursor and the circuit breaker.
// If task group isn't provided, the task group will default to the fully qualified name at library registration.
func WithTaskGroup(taskgroup string) RegistrableTaskOption {
	return func(rt *RegistrableTask) {
		rt.outboundTaskGroup = taskgroup
	}
}
