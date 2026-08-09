package chasmtest

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/tasks"
)

// NextTaskTime returns the earliest non-visibility CHASM physical task after
// the supplied time. Physical tasks can be stale; callers should execute task
// validation when the returned time is reached.
func (e *Engine) NextTaskTime(ref chasm.ComponentRef, after time.Time) (time.Time, bool, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return time.Time{}, false, err
	}

	var next time.Time
	for category, categoryTasks := range exec.backend.TasksByCategory {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			visibilityTime := task.GetVisibilityTime()
			if !visibilityTime.After(after) || (!next.IsZero() && !visibilityTime.Before(next)) {
				continue
			}
			next = visibilityTime
		}
	}
	return next, !next.IsZero(), nil
}

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
			valid, err = handler.Validate(mutableCtx, typedC, chasm.TaskInvocation{TaskAttributes: attrs}, task)
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

// FirePureTasks executes persisted pure tasks due by referenceTime and commits.
func (e *Engine) FirePureTasks(ref chasm.ComponentRef, referenceTime time.Time) (executed int, err error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return 0, err
	}

	engineCtx := chasm.NewEngineContext(context.Background(), e)
	if err := exec.node.EachPureTask(
		referenceTime,
		func(handler chasm.NodePureTask, taskAttributes chasm.TaskAttributes, taskInstance any) (bool, error) {
			ran, err := handler.ExecutePureTask(engineCtx, taskAttributes, taskInstance)
			if err == nil && ran {
				executed++
			}
			return ran, err
		},
	); err != nil {
		return executed, err
	}

	if err := exec.closeTransaction(); err != nil {
		return executed, err
	}
	return executed, nil
}

// FireSideEffectTasks executes side-effect tasks due by referenceTime. Stale physical
// tasks are ignored after their logical task has been removed from the tree.
func (e *Engine) FireSideEffectTasks(ref chasm.ComponentRef, referenceTime time.Time) (executed int, err error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return 0, err
	}
	engineCtx := chasm.NewEngineContext(context.Background(), e)
	for category, categoryTasks := range exec.backend.TasksByCategory {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			chasmTask, ok := task.(*tasks.ChasmTask)
			if !ok || chasmTask.GetVisibilityTime().After(referenceTime) {
				continue
			}
			inTree, valid, err := exec.node.ValidateSideEffectTask(engineCtx, chasmTask)
			if err != nil {
				return executed, err
			}
			if !inTree || !valid {
				continue
			}
			if err := exec.node.ExecuteSideEffectTask(
				engineCtx,
				exec.key,
				chasmTask,
				func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil },
			); err != nil {
				return executed, err
			}
			executed++
		}
	}
	return executed, nil
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
			valid, err = handler.Validate(chasmCtx, typedC, chasm.TaskInvocation{TaskAttributes: attrs}, task)
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
