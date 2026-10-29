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
	serviceHandler         *serviceHandler
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
	serviceHandler *serviceHandler,
) *Library {
	return &Library{
		ApplyLocalTaskHandler:  applyLocal,
		ApplyPeerTaskHandler:   applyPeer,
		PeerBackoffTaskHandler: peerBackoff,
		serviceHandler:         serviceHandler,
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	if l.serviceHandler != nil {
		server.RegisterService(&namespacereplicationpb.NamespaceReplicationService_ServiceDesc, l.serviceHandler)
	}
}

func (l *Library) Name() string {
	return chasm.NamespaceReplicationLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*NamespaceMutationComponent](
			chasm.NamespaceReplicationComponentName,
			// This alias is visibility schema, not Workflow wiring. The eventual
			// operator repair CLI uses it to resolve the exact CHASM business/run
			// IDs returned by namespace-indexed queries.
			chasm.WithBusinessIDAlias("NamespaceMutationBusinessId"),
			chasm.WithSearchAttributes(namespaceIDSearchAttribute, namespaceNameSearchAttribute),
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
