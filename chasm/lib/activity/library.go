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
	//nolint:revive // unchecked-type-assertion: intentional panic on missing context value
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
	activityDispatchTaskHandler       *activityDispatchTaskHandler
	scheduleToStartTimeoutTaskHandler *scheduleToStartTimeoutTaskHandler
	scheduleToCloseTimeoutTaskHandler *scheduleToCloseTimeoutTaskHandler
	startToCloseTimeoutTaskHandler    *startToCloseTimeoutTaskHandler
	heartbeatTimeoutTaskHandler       *heartbeatTimeoutTaskHandler
}

func newLibrary(
	handler *handler,
	activityDispatchTaskHandler *activityDispatchTaskHandler,
	scheduleToStartTimeoutTaskHandler *scheduleToStartTimeoutTaskHandler,
	scheduleToCloseTimeoutTaskHandler *scheduleToCloseTimeoutTaskHandler,
	startToCloseTimeoutTaskHandler *startToCloseTimeoutTaskHandler,
	heartbeatTimeoutTaskHandler *heartbeatTimeoutTaskHandler,
	config *Config,
	namespaceRegistry namespace.Registry,
) *library {
	return &library{
		componentOnlyLibrary:               *newComponentOnlyLibrary(config, namespaceRegistry),
		handler:                            handler,
		activityDispatchTaskHandler:       activityDispatchTaskHandler,
		scheduleToStartTimeoutTaskHandler: scheduleToStartTimeoutTaskHandler,
		scheduleToCloseTimeoutTaskHandler: scheduleToCloseTimeoutTaskHandler,
		startToCloseTimeoutTaskHandler:    startToCloseTimeoutTaskHandler,
		heartbeatTimeoutTaskHandler:       heartbeatTimeoutTaskHandler,
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&activitypb.ActivityService_ServiceDesc, l.handler)
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"dispatch",
			l.activityDispatchTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToStartTimer",
			l.scheduleToStartTimeoutTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToCloseTimer",
			l.scheduleToCloseTimeoutTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"startToCloseTimer",
			l.startToCloseTimeoutTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"heartbeatTimer",
			l.heartbeatTimeoutTaskHandler,
		),
	}
}
