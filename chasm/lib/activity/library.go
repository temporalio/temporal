package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/grpc"
)

type library struct {
	chasm.UnimplementedLibrary

	handler *handler
}

func newLibrary(
	handler *handler,
) *library {
	return &library{
		handler: handler,
	}
}

func (l *library) Name() string {
	return "activity"
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&activitypb.ActivityService_ServiceDesc, l.handler)
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity]("activity"),
	}
}
