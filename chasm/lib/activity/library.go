package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type ctxKeyActivityContextType struct{}

var ctxKeyActivityContext = ctxKeyActivityContextType{}

// activityContext holds dependencies injected into the chasm.Context for use by Activity methods.
type activityContext struct {
	config            *Config
	namespaceRegistry namespace.Registry
}

// activityContextFromChasm extracts the activityContext from a chasm.Context.
// Panics if the context value is missing, which indicates a library registration bug.
func activityContextFromChasm(ctx chasm.Context) *activityContext {
	return ctx.Value(ctxKeyActivityContext).(*activityContext)
}

const (
	libraryName   = "activity"
	componentName = "activity"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
	config            *Config
	namespaceRegistry namespace.Registry
}

func newComponentOnlyLibrary(
	config *Config,
	namespaceRegistry namespace.Registry,
) *componentOnlyLibrary {
	return &componentOnlyLibrary{
		config:            config,
		namespaceRegistry: namespaceRegistry,
	}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity](
			componentName,
			chasm.WithSearchAttributes(
				TypeSearchAttribute,
				StatusSearchAttribute,
				chasm.SearchAttributeTaskQueue,
			),
			chasm.WithBusinessIDAlias("ActivityId"),
			chasm.WithContextValues(map[any]any{
				ctxKeyActivityContext: &activityContext{
					config:            l.config,
					namespaceRegistry: l.namespaceRegistry,
				},
			}),
		),
	}
}

type library struct {
	componentOnlyLibrary

	handler                            *handler
	activityDispatchTaskExecutor       *activityDispatchTaskExecutor
	scheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor
	scheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor
	startToCloseTimeoutTaskExecutor    *startToCloseTimeoutTaskExecutor
	heartbeatTimeoutTaskExecutor       *heartbeatTimeoutTaskExecutor
}

func newLibrary(
	handler *handler,
	activityDispatchTaskExecutor *activityDispatchTaskExecutor,
	scheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor,
	scheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor,
	startToCloseTimeoutTaskExecutor *startToCloseTimeoutTaskExecutor,
	heartbeatTimeoutTaskExecutor *heartbeatTimeoutTaskExecutor,
	config *Config,
	namespaceRegistry namespace.Registry,
) *library {
	return &library{
		componentOnlyLibrary:               *newComponentOnlyLibrary(config, namespaceRegistry),
		handler:                            handler,
		activityDispatchTaskExecutor:       activityDispatchTaskExecutor,
		scheduleToStartTimeoutTaskExecutor: scheduleToStartTimeoutTaskExecutor,
		scheduleToCloseTimeoutTaskExecutor: scheduleToCloseTimeoutTaskExecutor,
		startToCloseTimeoutTaskExecutor:    startToCloseTimeoutTaskExecutor,
		heartbeatTimeoutTaskExecutor:       heartbeatTimeoutTaskExecutor,
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&activitypb.ActivityService_ServiceDesc, l.handler)
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"dispatch",
			l.activityDispatchTaskExecutor,
			l.activityDispatchTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToStartTimer",
			l.scheduleToStartTimeoutTaskExecutor,
			l.scheduleToStartTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToCloseTimer",
			l.scheduleToCloseTimeoutTaskExecutor,
			l.scheduleToCloseTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"startToCloseTimer",
			l.startToCloseTimeoutTaskExecutor,
			l.startToCloseTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"heartbeatTimer",
			l.heartbeatTimeoutTaskExecutor,
			l.heartbeatTimeoutTaskExecutor,
		),
	}
}
