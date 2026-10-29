package taskqueue

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/taskqueue/gen/taskqueuepb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName           = "taskQueue"
	componentName         = "taskQueue"
	userDataComponentName = "userData"
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
		chasm.NewRegistrableComponent[*TaskQueue](componentName),
		chasm.NewRegistrableComponent[*UserData](userDataComponentName),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	taskqueuepb.RegisterTaskQueueUserDataServiceServer(server, l.handler)
}
