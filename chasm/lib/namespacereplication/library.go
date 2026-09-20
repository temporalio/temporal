package namespacereplication

import (
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	ApplyLocalTaskHandler  *applyLocalTaskHandler
	ApplyPeerTaskHandler   *applyPeerTaskHandler
	PeerBackoffTaskHandler *applyPeerBackoffTaskHandler
	handler                *handler
}

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

func newLibrary(
	applyLocal *applyLocalTaskHandler,
	applyPeer *applyPeerTaskHandler,
	peerBackoff *applyPeerBackoffTaskHandler,
	handler *handler,
) *Library {
	return &Library{
		ApplyLocalTaskHandler:  applyLocal,
		ApplyPeerTaskHandler:   applyPeer,
		PeerBackoffTaskHandler: peerBackoff,
		handler:                handler,
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	if l.handler != nil {
		server.RegisterService(&namespacereplicationpb.NamespaceReplicationService_ServiceDesc, l.handler)
	}
}

func (l *Library) Name() string {
	return chasm.NamespaceReplicationLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*NamespaceMutationComponent](
			chasm.NamespaceReplicationComponentName,
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
		chasm.NewRegistrablePureTask(
			"apply_peer_backoff",
			l.PeerBackoffTaskHandler,
		),
	}
}
