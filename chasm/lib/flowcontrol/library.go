package flowcontrol

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/flowcontrol/concurrency"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"google.golang.org/grpc"
)

type library struct {
	chasm.UnimplementedLibrary

	concurrencyHandler           *concurrency.Handler
	concurrencyStagedWakeHandler *concurrency.StagedWakeHandler
}

func newLibrary(
	concurrencyHandler *concurrency.Handler,
	concurrencyStagedWakeHandler *concurrency.StagedWakeHandler,
) *library {
	return &library{
		concurrencyHandler:           concurrencyHandler,
		concurrencyStagedWakeHandler: concurrencyStagedWakeHandler,
	}
}

func (l *library) Name() string {
	return "flowcontrol"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*concurrency.Component](
			"concurrency_limiter",
		),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"concurrency_staged_wake",
			l.concurrencyStagedWakeHandler,
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace),
		),
	}
}

func (l *library) RegisterServices(s *grpc.Server) {
	s.RegisterService(&fcpb.ConcurrencyService_ServiceDesc, l.concurrencyHandler)
}
