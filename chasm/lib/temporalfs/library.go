package temporalfs

import (
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "temporalfs"
	componentName = "filesystem"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary

	handler                      *handler
	chunkGCTaskExecutor          *chunkGCTaskExecutor
	manifestCompactTaskExecutor  *manifestCompactTaskExecutor
	quotaCheckTaskExecutor       *quotaCheckTaskExecutor
}

func newLibrary(
	handler *handler,
	chunkGCTaskExecutor *chunkGCTaskExecutor,
	manifestCompactTaskExecutor *manifestCompactTaskExecutor,
	quotaCheckTaskExecutor *quotaCheckTaskExecutor,
) *library {
	return &library{
		handler:                      handler,
		chunkGCTaskExecutor:          chunkGCTaskExecutor,
		manifestCompactTaskExecutor:  manifestCompactTaskExecutor,
		quotaCheckTaskExecutor:       quotaCheckTaskExecutor,
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
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&temporalfspb.TemporalFSService_ServiceDesc, l.handler)
}
