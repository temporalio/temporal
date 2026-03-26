package temporalzfs

import (
	"go.temporal.io/server/chasm"
	temporalzfspb "go.temporal.io/server/chasm/lib/temporalzfs/gen/temporalzfspb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "temporalzfs"
	componentName = "filesystem"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary

	handler                     *handler
	chunkGCTaskExecutor         *chunkGCTaskExecutor
	manifestCompactTaskExecutor *manifestCompactTaskExecutor
	quotaCheckTaskExecutor      *quotaCheckTaskExecutor
	ownerCheckTaskExecutor      *ownerCheckTaskExecutor
	dataCleanupTaskExecutor     *dataCleanupTaskExecutor
}

func newLibrary(
	handler *handler,
	chunkGCTaskExecutor *chunkGCTaskExecutor,
	manifestCompactTaskExecutor *manifestCompactTaskExecutor,
	quotaCheckTaskExecutor *quotaCheckTaskExecutor,
	ownerCheckTaskExecutor *ownerCheckTaskExecutor,
	dataCleanupTaskExecutor *dataCleanupTaskExecutor,
) *library {
	return &library{
		handler:                     handler,
		chunkGCTaskExecutor:         chunkGCTaskExecutor,
		manifestCompactTaskExecutor: manifestCompactTaskExecutor,
		quotaCheckTaskExecutor:      quotaCheckTaskExecutor,
		ownerCheckTaskExecutor:      ownerCheckTaskExecutor,
		dataCleanupTaskExecutor:     dataCleanupTaskExecutor,
	}
}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Filesystem](
			componentName,
			chasm.WithSearchAttributes(
				statusSearchAttribute,
			),
			chasm.WithBusinessIDAlias("FilesystemId"),
		),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"chunkGC",
			l.chunkGCTaskExecutor,
			l.chunkGCTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"manifestCompact",
			l.manifestCompactTaskExecutor,
			l.manifestCompactTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"quotaCheck",
			l.quotaCheckTaskExecutor,
			l.quotaCheckTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"ownerCheck",
			l.ownerCheckTaskExecutor,
			l.ownerCheckTaskExecutor,
		),
		chasm.NewRegistrableSideEffectTask(
			"dataCleanup",
			l.dataCleanupTaskExecutor,
			l.dataCleanupTaskExecutor,
		),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&temporalzfspb.TemporalFSService_ServiceDesc, l.handler)
}
