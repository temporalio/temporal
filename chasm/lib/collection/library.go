package collection

import (
	"go.temporal.io/server/chasm"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"google.golang.org/grpc"
)

// componentOnlyLibrary registers the Collection component without task handlers or gRPC services.
// The frontend uses it so it can serialize ComponentRefs; tdbg uses it for registration only.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

// NewComponentOnlyLibrary returns a registration-only library (no task or gRPC handlers).
func NewComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Collection](componentName),
	}
}

// library is the full History-side library: the component plus the CollectionService gRPC handler.
type library struct {
	componentOnlyLibrary

	handler *handler
}

func newLibrary(handler *handler) *library {
	return &library{handler: handler}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&collectionpb.CollectionService_ServiceDesc, l.handler)
}
