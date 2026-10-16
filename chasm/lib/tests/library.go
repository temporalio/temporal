package tests

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	library struct {
		chasm.UnimplementedLibrary

		// retention is nil on the shared Library, so only test clusters, which register their own
		// library instance (see Module), set a PayloadStore retention.
		retention dynamicconfig.DurationPropertyFnWithNamespaceFilter
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
	opts := []chasm.RegistrableComponentOption{
		chasm.WithBusinessIDAlias("PayloadStoreId"),
		chasm.WithRecordPrincipals(),
		chasm.WithSearchAttributes(
			PayloadTotalCountSearchAttribute,
			PayloadTotalSizeSearchAttribute,
			ExecutionStatusSearchAttribute,
			chasm.SearchAttributeTaskQueue,
		),
		chasm.WithContextValues(map[any]any{
			componentCtxKey: componentCtxVal,
		}),
	}
	if l.retention != nil {
		opts = append(opts, chasm.WithRetention(l.retention))
	}
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*PayloadStore](componentName, opts...),
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
