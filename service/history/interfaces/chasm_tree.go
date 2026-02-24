//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination chasm_tree_mock.go

package interfaces

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/tasks"
)

var _ ChasmTree = (*chasm.Node)(nil)

// TODO: Remove this interface and use *chasm.Node directly
// when chasm/tree.go implementation completes.
type ChasmTree interface {
	CloseTransaction() (chasm.NodesMutation, error)
	Snapshot(*persistencespb.VersionedTransition) chasm.NodesSnapshot
	ApplyMutation(chasm.NodesMutation) error
	ApplySnapshot(chasm.NodesSnapshot) error
	RefreshTasks() error
	IsStateDirty() bool
	IsDirty() bool
	Terminate(chasm.TerminateComponentRequest) error
	Archetype() (chasm.Archetype, error)
	ArchetypeID() chasm.ArchetypeID
	EachPureTask(
		deadline time.Time,
		callback func(executor chasm.NodePureTask, taskAttributes chasm.TaskAttributes, task any) (bool, error),
	) error
	ExecuteSideEffectTask(
		ctx context.Context,
		registry *chasm.Registry,
		executionKey chasm.ExecutionKey,
		task *tasks.ChasmTask,
		validate func(chasm.NodeBackend, chasm.Context, chasm.Component) error,
	) error
	ValidateSideEffectTask(
		ctx context.Context,
		task *tasks.ChasmTask,
	) (bool, error)
	IsStale(chasm.ComponentRef) error
	Component(chasm.Context, chasm.ComponentRef) (chasm.Component, error)
	ComponentByPath(chasm.Context, []string) (chasm.Component, error)
}
