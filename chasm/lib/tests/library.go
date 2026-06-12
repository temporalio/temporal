package tests

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
)

type (
	library struct {
		chasm.UnimplementedLibrary
	}
)

const (
	libraryName   = "tests"
	componentName = "payloadStore"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

var Library = &library{}

func (l *library) Name() string {
	return libraryName
}

func (l *library) NexusServices() []*nexus.Service {
	return []*nexus.Service{NewTestServiceNexusService()}
}

func (l *library) NexusServiceProcessors() []*chasm.NexusServiceProcessor {
	return []*chasm.NexusServiceProcessor{NewTestServiceNexusServiceProcessor()}
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*PayloadStore](
			componentName,
			chasm.WithBusinessIDAlias("PayloadStoreId"),
			chasm.WithSearchAttributes(
				PayloadTotalCountSearchAttribute,
				PayloadTotalSizeSearchAttribute,
				ExecutionStatusSearchAttribute,
				chasm.SearchAttributeTaskQueue,
			),
			chasm.WithContextValues(map[any]any{
				componentCtxKey: componentCtxVal,
			}),
		),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"payloadTTLPureTask",
			&PayloadTTLPureTaskHandler{},
		),
		chasm.NewRegistrableSideEffectTask(
			"payloadTTLSideEffectTask",
			&PayloadTTLSideEffectTaskHandler{},
		),
	}
}
