package history

import (
	"context"

	"go.temporal.io/server/chasm"
)

// discardableTaskTestLibrary is a minimal CHASM library that registers a side-effect task whose executor implements
// SideEffectDiscardHandler, used for testing discard paths in standby task executors.
type discardableTaskTestLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *discardableTaskTestLibrary) Name() string { return "DiscardableTestLib" }

func (l *discardableTaskTestLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"discard_task",
			&discardableTestTaskValidator{},
			&discardableTestTaskExecutor{},
		),
	}
}

type discardableTestTask struct{}

type discardableTestTaskValidator struct{}

func (v *discardableTestTaskValidator) Validate(_ chasm.Context, _ any, _ chasm.TaskAttributes, _ *discardableTestTask) (bool, error) {
	return true, nil
}

type discardableTestTaskExecutor struct{}

func (e *discardableTestTaskExecutor) Execute(_ context.Context, _ chasm.ComponentRef, _ chasm.TaskAttributes, _ *discardableTestTask) error {
	return nil
}

func (e *discardableTestTaskExecutor) HandleDiscard(_ context.Context, _ chasm.ComponentRef, _ chasm.TaskAttributes, _ *discardableTestTask) error {
	return nil
}

// nonDiscardableTaskTestLibrary is a minimal CHASM library that registers a side-effect task whose executor does NOT
// implement SideEffectDiscardHandler.
type nonDiscardableTaskTestLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *nonDiscardableTaskTestLibrary) Name() string { return "NonDiscardableTestLib" }

func (l *nonDiscardableTaskTestLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"non_discard_task",
			&nonDiscardableTestTaskValidator{},
			&nonDiscardableTestTaskExecutor{},
		),
	}
}

type nonDiscardableTestTask struct{}

type nonDiscardableTestTaskValidator struct{}

func (v *nonDiscardableTestTaskValidator) Validate(_ chasm.Context, _ any, _ chasm.TaskAttributes, _ *nonDiscardableTestTask) (bool, error) {
	return true, nil
}

type nonDiscardableTestTaskExecutor struct{}

func (e *nonDiscardableTestTaskExecutor) Execute(_ context.Context, _ chasm.ComponentRef, _ chasm.TaskAttributes, _ *nonDiscardableTestTask) error {
	return nil
}
