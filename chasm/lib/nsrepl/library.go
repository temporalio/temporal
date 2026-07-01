package nsrepl

import (
	"go.temporal.io/server/chasm"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	ApplyLocalTaskHandler *applyLocalTaskHandler
	ApplyPeerTaskHandler  *applyPeerTaskHandler

	// handler implements the history-side NamespaceReplicationService gRPC.
	// Registered with the history gRPC server via RegisterServices below.
	handler *handler
}

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

func newLibrary(
	applyLocal *applyLocalTaskHandler,
	applyPeer *applyPeerTaskHandler,
	h *handler,
) *Library {
	return &Library{
		ApplyLocalTaskHandler: applyLocal,
		ApplyPeerTaskHandler:  applyPeer,
		handler:               h,
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

// RegisterServices registers the NamespaceReplicationService gRPC with the
// history server (which is where the CHASM engine is in context via the
// ChasmEngineInterceptor). The receiver-side ApplyNamespaceMutation admin RPC
// is registered separately with the admin service in the frontend.
func (l *Library) RegisterServices(server *grpc.Server) {
	if l.handler != nil {
		server.RegisterService(&nsreplpb.NamespaceReplicationService_ServiceDesc, l.handler)
	}
}
