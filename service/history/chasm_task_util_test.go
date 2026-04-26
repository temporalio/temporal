package history

import (
	"context"

	"go.temporal.io/server/chasm"
)

// discardableTaskTestLibrary is a minimal CHASM library that registers a side-effect task whose handler has a custom
// Discard implementation, used for testing discard paths in standby task executors.
type discardableTaskTestLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *discardableTaskTestLibrary) Name() string { return "DiscardableTestLib" }

func (l *discardableTaskTestLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"discard_task",
			&discardableTestTaskHandler{},
		),
	}
}

type discardableTestTask struct{}

type discardableTestTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*discardableTestTask]
}

func (e *discardableTestTaskHandler) Validate(_ chasm.Context, _ any, _ chasm.TaskAttributes, _ *discardableTestTask) (bool, error) {
	return true, nil
}

func (e *discardableTestTaskHandler) Execute(_ context.Context, _ chasm.ComponentRef, _ chasm.TaskAttributes, _ *discardableTestTask) error {
	return nil
}

func (e *discardableTestTaskHandler) Discard(_ context.Context, _ chasm.ComponentRef, _ chasm.TaskAttributes, _ *discardableTestTask) error {
	return nil
}

// nonDiscardableTaskTestLibrary is a minimal CHASM library that registers a side-effect task whose handler uses the
// default Discard from SideEffectTaskHandlerBase (returns ErrTaskDiscarded).
type nonDiscardableTaskTestLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *nonDiscardableTaskTestLibrary) Name() string { return "NonDiscardableTestLib" }

func (l *nonDiscardableTaskTestLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"non_discard_task",
			&nonDiscardableTestTaskHandler{},
		),
	}
}

type nonDiscardableTestTask struct{}

type nonDiscardableTestTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nonDiscardableTestTask]
}

func (e *nonDiscardableTestTaskHandler) Validate(_ chasm.Context, _ any, _ chasm.TaskAttributes, _ *nonDiscardableTestTask) (bool, error) {
	return true, nil
}

func (e *nonDiscardableTestTaskHandler) Execute(_ context.Context, _ chasm.ComponentRef, _ chasm.TaskAttributes, _ *nonDiscardableTestTask) error {
	return nil
}
