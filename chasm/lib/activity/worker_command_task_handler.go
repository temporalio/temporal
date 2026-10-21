package activity

import (
	"context"
	"fmt"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/workercommands"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

// workerCommandDispatchTaskHandler dispatches worker commands (cancel, pause, resume) via the
// worker control queue.
type workerCommandDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.WorkerCommandDispatchTask]
	matchingClient    resource.MatchingClient
	namespaceRegistry namespace.Registry
	config            *configs.Config
	metricsHandler    metrics.Handler
	logger            log.Logger
}

type workerCommandDispatchTaskHandlerOptions struct {
	fx.In

	MatchingClient    resource.MatchingClient
	NamespaceRegistry namespace.Registry
	Config            *configs.Config
	MetricsHandler    metrics.Handler
	Logger            log.Logger
}

func newWorkerCommandDispatchTaskHandler(opts workerCommandDispatchTaskHandlerOptions) *workerCommandDispatchTaskHandler {
	return &workerCommandDispatchTaskHandler{
		matchingClient:    opts.MatchingClient,
		namespaceRegistry: opts.NamespaceRegistry,
		config:            opts.Config,
		metricsHandler:    opts.MetricsHandler,
		logger:            opts.Logger,
	}
}

func (h *workerCommandDispatchTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	invocation chasm.TaskInvocation,
	task *activitypb.WorkerCommandDispatchTask,
) (bool, error) {
	if invocation.Attempt > h.config.WorkerCommandsMaxAttempts() {
		key := ctx.ExecutionKey()
		h.logger.Info("Worker command dispatch task exceeded max attempts, dropping",
			tag.WorkflowNamespaceID(key.NamespaceID),
			tag.ActivityID(key.BusinessID),
			tag.Attempt(int32(invocation.Attempt)),
		)
		namespaceName := h.resolveNamespaceName(key.NamespaceID)
		metrics.WorkerCommandsSent.With(h.metricsHandler).Record(
			1,
			metrics.NamespaceTag(namespaceName),
			metrics.OutcomeTag("max_attempts_exceeded"),
			metrics.StringTag("command_type", commandTypeName(task)),
		)
		return false, nil
	}

	switch task.GetCommand().(type) {
	case *activitypb.WorkerCommandDispatchTask_Cancel:
		return validateCancelCommand(activity), nil
	default:
		h.logger.Warn("Unknown worker command type in dispatch task, dropping",
			tag.NewStringTag("command_type", fmt.Sprintf("%T", task.GetCommand())),
		)
		return false, nil
	}
}

// Execute reads the activity to build a task token, constructs the worker command, and
// dispatches it to the worker's control queue.
func (h *workerCommandDispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	taskAttrs chasm.TaskAttributes,
	task *activitypb.WorkerCommandDispatchTask,
) error {
	taskToken, err := chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).buildWorkerCommandTaskToken,
		activityRef,
	)
	if err != nil {
		return err
	}

	nsEntry, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(activityRef.NamespaceID))
	if err != nil {
		return err
	}

	command, err := buildWorkerCommand(task, taskToken)
	if err != nil {
		return err
	}

	dispatchTask := &tasks.WorkerCommandsTask{
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

	return dispatcher.Execute(ctx, dispatchTask, nsEntry.Name().String())
}

// validateCancelCommand returns true if the activity is in a state where a cancel command
// should be dispatched to the worker.
func validateCancelCommand(activity *Activity) bool {
	return activity.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED ||
		activity.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED
}

// buildWorkerCommand constructs the WorkerCommand proto for the given task.
func buildWorkerCommand(task *activitypb.WorkerCommandDispatchTask, taskToken []byte) (*workerpb.WorkerCommand, error) {
	switch task.GetCommand().(type) {
	case *activitypb.WorkerCommandDispatchTask_Cancel:
		return &workerpb.WorkerCommand{
			Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{
					TaskToken: taskToken,
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown worker command type: %T", task.GetCommand())
	}
}

// resolveNamespaceName best-effort resolves a namespace ID to its name for metrics.
// Falls back to "unknown" if the lookup fails.
func (h *workerCommandDispatchTaskHandler) resolveNamespaceName(namespaceID string) string {
	if h.namespaceRegistry == nil {
		return "unknown"
	}
	nsEntry, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return "unknown"
	}
	return nsEntry.Name().String()
}

// commandTypeName returns a metric-friendly name for the task's command type.
func commandTypeName(task *activitypb.WorkerCommandDispatchTask) string {
	switch task.GetCommand().(type) {
	case *activitypb.WorkerCommandDispatchTask_Cancel:
		return "cancel_activity"
	default:
		return "unknown"
	}
}
