package tquserdata

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "taskQueueUserData"
	componentName = "taskQueueUserData"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary
	handler *handler
}

// NewNilLibrary returns a library with nil handlers for offline state decoding.
func NewNilLibrary() chasm.Library {
	return newLibrary(nil)
}

func newLibrary(handler *handler) *library {
	return &library{handler: handler}
}

func (*library) Name() string {
	return libraryName
}

func (*library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*TaskQueueUserData](componentName),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	tquserdatapb.RegisterTaskQueueUserDataServiceServer(server, l.handler)
}
