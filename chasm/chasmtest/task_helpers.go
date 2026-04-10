package chasmtest

import (
	"context"
	"fmt"

	"go.temporal.io/server/chasm"
)

// ExecutePureTask validates and executes a pure task atomically via [Engine.UpdateComponent].
// It returns taskDropped=true if [chasm.PureTaskHandler.Validate] returns (false, nil),
// indicating the task is no longer relevant and was not executed.
//
// For root components, construct ref with [chasm.NewComponentRef]. For subcomponents, use
// [Engine.Ref] to look up the ref from the component instance.
//
// This helper ensures that Validate is always exercised alongside Execute, matching the real
// engine's behavior. Use [chasm.MockMutableContext] directly when you need to inspect the
// typed task payloads added to the context during execution.
func ExecutePureTask[C chasm.Component, T any](
	ctx context.Context,
	e *Engine,
	ref chasm.ComponentRef,
	handler chasm.PureTaskHandler[C, T],
	attrs chasm.TaskAttributes,
	task T,
) (taskDropped bool, err error) {
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

// ExecuteSideEffectTask validates and executes a side-effect task.
// Validation runs via [Engine.ReadComponent] (read-only), and if valid,
// [chasm.SideEffectTaskHandler.Execute] is called with an engine context so that
// [chasm.UpdateComponent] and [chasm.ReadComponent] inside the handler route through
// the test engine.
//
// It returns taskDropped=true if [chasm.SideEffectTaskHandler.Validate] returns (false, nil),
// indicating the task is no longer relevant and was not executed.
//
// For root components, construct ref with [chasm.NewComponentRef]. For subcomponents, use
// [Engine.Ref] to look up the ref from the component instance.
//
// Use [chasm.MockMutableContext] directly when you need to inspect typed task payloads added
// during execution, since the real engine serializes them into history-layer tasks.
func ExecuteSideEffectTask[C chasm.Component, T any](
	ctx context.Context,
	e *Engine,
	ref chasm.ComponentRef,
	handler chasm.SideEffectTaskHandler[C, T],
	attrs chasm.TaskAttributes,
	task T,
) (taskDropped bool, err error) {
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
