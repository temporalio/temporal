package namespacereplication

import (
	"go.temporal.io/server/chasm"
)

type Library struct {
	chasm.UnimplementedLibrary

	ApplyLocalTaskHandler *applyLocalTaskHandler
	ApplyPeerTaskHandler  *applyPeerTaskHandler
}

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

func newLibrary(
	applyLocal *applyLocalTaskHandler,
	applyPeer *applyPeerTaskHandler,
) *Library {
	return &Library{
		ApplyLocalTaskHandler: applyLocal,
		ApplyPeerTaskHandler:  applyPeer,
	}
}

func (l *Library) Name() string {
	return chasm.NamespaceReplicationLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*NamespaceMutationComponent](
			chasm.NamespaceReplicationComponentName,
			// Ephemeral: component is per-mutation, completes in seconds-to-minutes,
			// then retention deletes it. Not a long-lived state machine.
			chasm.WithEphemeral(),
			// Surface the BusinessID (= target namespace_id) as a visibility
			// search-attribute alias so components show up in
			// `temporal workflow list` and can be queried by namespace UUID.
			chasm.WithBusinessIDAlias("NamespaceId"),
		),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"apply_local",
			l.ApplyLocalTaskHandler,
		),
		chasm.NewRegistrableSideEffectTask(
			"apply_peer",
			l.ApplyPeerTaskHandler,
		),
	}
}

// NOTE: the history-side NamespaceReplicationService gRPC handler and the
// RegisterServices override that binds it land in a later PR. Until then the
// Library inherits the no-op RegisterServices from chasm.UnimplementedLibrary,
// so this foundation registers its component and tasks without exposing any RPC.
