package temporalfs

import (
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
)

var _ chasm.RootComponent = (*Filesystem)(nil)

// Filesystem is the root CHASM component for the TemporalFS archetype.
// FS layer data (inodes, chunks, directory entries) is stored in a dedicated
// store managed by FSStoreProvider, not as CHASM Fields. Only FS metadata
// (config, stats, lifecycle) lives in CHASM state.
type Filesystem struct {
	chasm.UnimplementedComponent

	*temporalfspb.FilesystemState

	Visibility chasm.Field[*chasm.Visibility]
}

// LifecycleState implements chasm.Component.
func (f *Filesystem) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch f.Status {
	case temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
		temporalfspb.FILESYSTEM_STATUS_DELETED:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

// Terminate implements chasm.RootComponent.
func (f *Filesystem) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	f.Status = temporalfspb.FILESYSTEM_STATUS_DELETED
	return chasm.TerminateComponentResponse{}, nil
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider.
func (f *Filesystem) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		statusSearchAttribute.Value(f.GetStatus().String()),
	}
}
