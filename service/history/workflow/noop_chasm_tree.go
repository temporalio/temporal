package workflow

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	historyi "go.temporal.io/server/service/history/interfaces"
)

var _ historyi.ChasmTree = (*noopChasmTree)(nil)

type noopChasmTree struct{}

func (*noopChasmTree) CloseTransaction() (chasm.NodesMutation, error) {
	return chasm.NodesMutation{}, nil
}

func (*noopChasmTree) Snapshot(*persistencespb.VersionedTransition) chasm.NodesSnapshot {
	return chasm.NodesSnapshot{}
}

func (*noopChasmTree) ApplyMutation(chasm.NodesMutation) error {
	return nil
}

func (*noopChasmTree) ApplySnapshot(chasm.NodesSnapshot) error {
	return nil
}

func (*noopChasmTree) IsDirty() bool {
	return false
}

func (*noopChasmTree) Terminate(chasm.TerminateComponentRequest) error {
	return nil
}

func (*noopChasmTree) Archetype() string {
	return ""
}

func (*noopChasmTree) EachPureTask(
	deadline time.Time,
	callback func(executor chasm.NodeExecutePureTask, task any) error,
) error {
	return nil
}

func (*noopChasmTree) IsStale(chasm.ComponentRef) error {
	return nil
}

func (*noopChasmTree) Component(chasm.Context, chasm.ComponentRef) (chasm.Component, error) {
	return nil, serviceerror.NewInternal("Component() method invoked on noop CHASM tree")
}

func (*noopChasmTree) ExecuteSideEffectTask(
	ctx context.Context,
	registry *chasm.Registry,
	entityKey chasm.EntityKey,
	taskInfo *persistencespb.ChasmTaskInfo,
	validate func(chasm.NodeBackend, chasm.Context, chasm.Component) error,
) error {
	return nil
}

func (*noopChasmTree) ValidateSideEffectTask(
	ctx context.Context,
	registry *chasm.Registry,
	taskInfo *persistencespb.ChasmTaskInfo,
) (any, error) {
	return nil, nil
}
