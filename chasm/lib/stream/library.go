package stream

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc"
)

type library struct {
	chasm.UnimplementedLibrary
	handler *handler
}

func newLibrary(handler *handler) *library {
	return &library{
		handler: handler,
	}
}

func (l *library) Name() string {
	return "stream"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Stream]("stream"),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&streampb.StreamService_ServiceDesc, l.handler)
}
