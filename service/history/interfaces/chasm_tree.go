//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination chasm_tree_mock.go

package interfaces

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

var _ ChasmTree = (*chasm.Node)(nil)

type ChasmTree interface {
	CloseTransaction() (chasm.NodesMutation, error)
	Snapshot(*persistencespb.VersionedTransition) chasm.NodesSnapshot
	ApplyMutation(chasm.NodesMutation) error
	ApplySnapshot(chasm.NodesSnapshot) error
	IsDirty() bool
}
