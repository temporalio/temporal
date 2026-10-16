package tests

import (
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
)

type (
	library struct {
		chasm.UnimplementedLibrary
	}
)

const (
	libraryName   = "tests"
	componentName = "payloadStore"

	// RetentionNamespaceDataKey is the namespace data key that sets the PayloadStore archetype
	// retention, so functional tests can exercise chasm.WithRetention per namespace.
	RetentionNamespaceDataKey = "chasm-tests-payload-store-retention"
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
			chasm.WithRetention(retentionFromNamespaceData),
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

// retentionFromNamespaceData returns 0 (no override) when the namespace data key is absent or invalid.
func retentionFromNamespaceData(ns *namespace.Namespace) time.Duration {
	retention, err := time.ParseDuration(ns.GetCustomData(RetentionNamespaceDataKey))
	if err != nil {
		return 0
	}
	return retention
}
