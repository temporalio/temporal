package activity

import (
	"context"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/workercommands"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

// cancelCommandDispatchTaskHandler dispatches a cancel command to the worker via the Nexus
// worker commands control queue. This is a best-effort mechanism — the activity will eventually
// time out if the worker doesn't respond.
type cancelCommandDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.CancelCommandDispatchTask]
	matchingClient    resource.MatchingClient
	namespaceRegistry namespace.Registry
	config            *configs.Config
	metricsHandler    metrics.Handler
	logger            log.Logger
}

type cancelCommandDispatchTaskHandlerOptions struct {
	fx.In

	MatchingClient    resource.MatchingClient
	NamespaceRegistry namespace.Registry
	Config            *configs.Config
	MetricsHandler    metrics.Handler
	Logger            log.Logger
}

func newCancelCommandDispatchTaskHandler(opts cancelCommandDispatchTaskHandlerOptions) *cancelCommandDispatchTaskHandler {
	return &cancelCommandDispatchTaskHandler{
		matchingClient:    opts.MatchingClient,
		namespaceRegistry: opts.NamespaceRegistry,
		config:            opts.Config,
		metricsHandler:    opts.MetricsHandler,
		logger:            opts.Logger,
	}
}

func (h *cancelCommandDispatchTaskHandler) Validate(
	_ chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.CancelCommandDispatchTask,
) (bool, error) {
	// Valid if the activity is in a state where it has been requested to cancel or terminated
	// (meaning it was running on a worker when the cancel/terminate was issued).
	return activity.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED ||
		activity.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, nil
}

func (h *cancelCommandDispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	taskAttrs chasm.TaskAttributes,
	_ *activitypb.CancelCommandDispatchTask,
) error {
	// Read the activity to build the task token for the cancel command.
	taskToken, err := chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).buildCancelCommandTaskToken,
		activityRef,
	)
	if err != nil {
		return err
	}

	nsEntry, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(activityRef.NamespaceID))
	if err != nil {
		return err
	}

	command := &workerpb.WorkerCommand{
		Type: &workerpb.WorkerCommand_CancelActivity{
			CancelActivity: &workerpb.CancelActivityCommand{
				TaskToken: taskToken,
			},
		},
	}

	task := &tasks.WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey(activityRef.NamespaceID, "", ""),
		Commands:    []*workerpb.WorkerCommand{command},
		Destination: taskAttrs.Destination,
	}

	dispatcher := workercommands.NewDispatcher(
		h.matchingClient,
		h.config,
		h.metricsHandler,
		h.logger,
	)

	// TODO: CHASM's SideEffectTaskHandler interface doesn't expose an attempt count. The
	// dispatcher's max attempts check is effectively bypassed here. We need to either expose
	// attempt count in the CHASM task interface or handle retry limiting differently.
	return dispatcher.Execute(ctx, task, 1, nsEntry.Name().String())
}
