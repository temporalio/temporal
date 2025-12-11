package tests

import "go.temporal.io/server/chasm"

type (
	library struct {
		chasm.UnimplementedLibrary
	}
)

var Library = &library{}

func (l *library) Name() string {
	return "tests"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*PayloadStore]("payloadStore",
			chasm.WithBusinessIDAlias("PayloadStoreId"),
			chasm.WithSearchAttributes(
				PayloadTotalCountSearchAttribute,
				PayloadTotalSizeSearchAttribute,
				PayloadExecutionStatusSearchAttribute,
			),
		),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"payloadTTLPureTask",
			&PayloadTTLPureTaskValidator{},
			&PayloadTTLPureTaskExecutor{},
		),
		chasm.NewRegistrableSideEffectTask(
			"payloadTTLSideEffectTask",
			&PayloadTTLSideEffectTaskValidator{},
			&PayloadTTLSideEffectTaskExecutor{},
		),
	}
}
