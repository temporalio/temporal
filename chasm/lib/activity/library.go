package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

type Library struct {
	fx.In

	chasm.UnimplementedLibrary
}

func NewLibrary() *Library {
	return &Library{}
}

func (l *Library) RegisterServices(server *grpc.Server) error {
	activitypb.RegisterActivityServiceServer(server, Handler{})

	return nil
}
