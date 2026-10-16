package wci

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/chasm/lib/wci/migration"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/fx"
)

type MigrateToWorkflowTaskHandlerOptions struct {
	fx.In

	HistoryClient  resource.HistoryClient
	DynamicConfig  *dynamicconfig.Collection
	MetricsHandler metrics.Handler
}

// MigrateToWorkflowTaskHandler starts the workflow from converted state, then marks the root MIGRATED.
type MigrateToWorkflowTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.MigrateToWorkflowTask]
	historyClient  resource.HistoryClient
	taskQueue      dynamicconfig.StringPropertyFn
	metricsHandler metrics.Handler
}

func NewMigrateToWorkflowTaskHandler(opts MigrateToWorkflowTaskHandlerOptions) *MigrateToWorkflowTaskHandler {
	return &MigrateToWorkflowTaskHandler{historyClient: opts.HistoryClient, taskQueue: WorkflowTaskQueue.Get(opts.DynamicConfig), metricsHandler: opts.MetricsHandler}
}

func (h *MigrateToWorkflowTaskHandler) Validate(_ chasm.Context, w *WorkerControllerInstance, _ chasm.TaskInvocation, _ *wcipb.MigrateToWorkflowTask) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, nil
}

func (h *MigrateToWorkflowTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.MigrateToWorkflowTask) error {
	req, err := chasm.ReadComponent(ctx, ref, func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (*historyservice.StartWorkflowExecutionRequest, error) {
		groups := make(map[string]*wcipb.ScalingGroupState, len(w.ScalingGroups))
		for id, field := range w.ScalingGroups {
			if g, ok := field.TryGet(cctx); ok {
				groups[id] = g.ScalingGroupState
			}
		}
		ns := cctx.NamespaceEntry()
		args, err := migration.CHASMStateToWorkflow(ns.Name().String(), ns.ID().String(), w.WorkerControllerInstanceState, groups)
		if err != nil {
			return nil, err
		}
		input, err := sdk.PreferProtoDataConverter.ToPayloads(args)
		if err != nil {
			return nil, err
		}
		workflowID := cctx.ExecutionKey().BusinessID
		return &historyservice.StartWorkflowExecutionRequest{
			NamespaceId: ns.ID().String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				// Stable across task retries.
				RequestId:    workflowID + ":migrate-to-workflow",
				Namespace:    ns.Name().String(),
				WorkflowId:   workflowID,
				WorkflowType: &commonpb.WorkflowType{Name: iface.WorkerControllerInstanceWorkflowType},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: h.taskQueue()},
				Input:        input,
				// An earlier run (before migrating to CHASM) may exist and be closed.
				WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
					sadefs.TemporalNamespaceDivision: payload.EncodeString(iface.WorkerControllerInstanceNamespaceDivision),
				}},
				Identity: "history-service",
			},
		}, nil
	}, nil)
	if err != nil {
		return err
	}

	// Already started (a retry, or a crash after the start) counts as success.
	_, err = h.historyClient.StartWorkflowExecution(ctx, req)
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if err != nil && !errors.As(err, &started) {
		MigrateToWorkflowCount.With(h.metricsHandler.WithTags(metrics.NamespaceTag(req.GetStartRequest().GetNamespace()))).
			Record(1, metrics.StringTag(wcimetrics.MigrationOutcomeTagName, migrationOutcomeError), metrics.StringTag(wcimetrics.BackendTagName, backendCHASM))
		return err
	}

	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		MigrateToWorkflowCount.With(mctx.MetricsHandler()).
			Record(1, metrics.StringTag(wcimetrics.MigrationOutcomeTagName, migrationOutcomeSuccess), metrics.StringTag(wcimetrics.BackendTagName, backendCHASM))
		return struct{}{}, TransitionWorkflowStarted.Apply(w, mctx, EventWorkflowStarted{})
	}, nil)
	return err
}
