package tquserdata

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"google.golang.org/grpc"
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
	return "taskQueueUserData"
}

func (*library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*TaskQueueUserData]("taskQueueUserData"),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	tquserdatapb.RegisterTaskQueueUserDataServiceServer(server, l.handler)
}
