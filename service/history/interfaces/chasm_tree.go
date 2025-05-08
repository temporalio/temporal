//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination chasm_tree_mock.go

package interfaces

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

var _ ChasmTree = (*chasm.Node)(nil)

// TODO: Remove this interface and use *chasm.Node directly
// when chasm/tree.go implementation completes.
type ChasmTree interface {
	CloseTransaction() (chasm.NodesMutation, error)
	Snapshot(*persistencespb.VersionedTransition) chasm.NodesSnapshot
	ApplyMutation(chasm.NodesMutation) error
	ApplySnapshot(chasm.NodesSnapshot) error
	IsDirty() bool
	Terminate(chasm.TerminateComponentRequest) error
	Archetype() string

	EachPureTask(
		deadline time.Time,
		callback func(executor chasm.NodeExecutePureTask, task any) error,
	) error
}
