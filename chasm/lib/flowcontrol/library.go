package flowcontrol

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/flowcontrol/concurrency"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"google.golang.org/grpc"
)

type library struct {
	chasm.UnimplementedLibrary

	concurrencyHandler *concurrency.Handler
}

func newLibrary(
	concurrencyHandler *concurrency.Handler,
) *library {
	return &library{
		concurrencyHandler: concurrencyHandler,
	}
}

func NewNilLibrary() *library { return &library{} }

func (l *library) Name() string {
	return "flowcontrol"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*concurrency.Component](
			"concurrency_limiter",
			// chasm.WithBusinessIDAlias("ConcurrencyLimiterId"), // TODO(fc): enable visibility?
		),
	}
}

func (l *library) RegisterServices(s *grpc.Server) {
	s.RegisterService(&fcpb.ConcurrencyService_ServiceDesc, l.concurrencyHandler)
}
