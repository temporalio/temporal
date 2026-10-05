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

func newLibrary(handler *handler) *library {
	return &library{handler: handler}
}

func (*library) Name() string {
	return "tquserdata"
}

func (*library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*TaskQueueUserData]("userData"),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	tquserdatapb.RegisterTaskQueueUserDataServiceServer(server, l.handler)
}
