package chasmtest

import (
	"context"
	"fmt"

	"go.temporal.io/server/chasm"
)

// ExecutePureTask validates and executes a pure task atomically via [Engine.UpdateComponent].
// It returns taskDropped set to true if [chasm.PureTaskHandler.Validate] returns (false, nil),
// indicating the task is no longer relevant and was not executed.
//
// The component ref is resolved automatically — no separate [Engine.ReadComponent] call to
// obtain a ref is needed. Pass the component pointer directly.
//
// This helper ensures that Validate is always exercised alongside Execute, matching the real
// engine's behavior. Use [chasm.MockMutableContext] directly when you need to inspect the
// typed task payloads added to the context during execution.
func ExecutePureTask[C chasm.Component, T any](
	ctx context.Context,
	e *Engine,
	component C,
	handler chasm.PureTaskHandler[C, T],
	attrs chasm.TaskAttributes,
	task T,
) (taskDropped bool, err error) {
	ref, err := e.refForComponent(component)
	if err != nil {
		return false, err
	}

	engineCtx := chasm.NewEngineContext(ctx, e)
	_, err = e.UpdateComponent(
		engineCtx,
		ref,
		func(mutableCtx chasm.MutableContext, c chasm.Component) error {
			typedC, ok := c.(C)
			if !ok {
				return fmt.Errorf("component type mismatch: got %T", c)
			}
			var valid bool
			valid, err = handler.Validate(mutableCtx, typedC, attrs, task)
			if err != nil {
				return err
			}
			if !valid {
				taskDropped = true
				return nil
			}
			return handler.Execute(mutableCtx, typedC, attrs, task)
		},
	)
	return taskDropped, err
}

// ExecuteSideEffectTask validates and executes a side effect task.
// Validation runs via [Engine.ReadComponent] in read only mode, and if valid,
// [chasm.SideEffectTaskHandler.Execute] is called with an engine context so that
// [chasm.UpdateComponent] and [chasm.ReadComponent] inside the handler route through
// the test engine.
//
// It returns taskDropped set to true if [chasm.SideEffectTaskHandler.Validate] returns (false, nil),
// indicating the task is no longer relevant and was not executed.
//
// The component ref is resolved automatically — no separate [Engine.ReadComponent] call to
// obtain a ref is needed. Pass the component pointer directly.
//
// Use [chasm.MockMutableContext] directly when you need to inspect typed task payloads added
// during execution, since the real engine serializes them into history layer tasks.
func ExecuteSideEffectTask[C chasm.Component, T any](
	ctx context.Context,
	e *Engine,
	component C,
	handler chasm.SideEffectTaskHandler[C, T],
	attrs chasm.TaskAttributes,
	task T,
) (taskDropped bool, err error) {
	ref, err := e.refForComponent(component)
	if err != nil {
		return false, err
	}

	engineCtx := chasm.NewEngineContext(ctx, e)

	var valid bool
	if err = e.ReadComponent(
		engineCtx,
		ref,
		func(chasmCtx chasm.Context, c chasm.Component) error {
			typedC, ok := c.(C)
			if !ok {
				return fmt.Errorf("component type mismatch: got %T", c)
			}
			valid, err = handler.Validate(chasmCtx, typedC, attrs, task)
			return err
		},
	); err != nil {
		return false, err
	}
	if !valid {
		return true, nil
	}

	return false, handler.Execute(engineCtx, ref, attrs, task)
}
