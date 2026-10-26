package wci

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	wciconfig "go.temporal.io/auto-scaled-workers/wci/client"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MigrateToWorkflowTaskHandlerOptions struct {
	fx.In

	HistoryClient  resource.HistoryClient
	DynamicConfig  *dynamicconfig.Collection
	MetricsHandler metrics.Handler
}

// MigrateToWorkflowTaskHandler coordinates the CHASM → workflow handover: it starts the workflow
// pending, activates it, then marks the root MIGRATED; a definitive failure resumes the root.
type MigrateToWorkflowTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.MigrateToWorkflowTask]
	historyClient  resource.HistoryClient
	taskQueue      dynamicconfig.StringPropertyFn
	pendingTTL     dynamicconfig.DurationPropertyFn
	metricsHandler metrics.Handler
}

func NewMigrateToWorkflowTaskHandler(opts MigrateToWorkflowTaskHandlerOptions) *MigrateToWorkflowTaskHandler {
	return &MigrateToWorkflowTaskHandler{
		historyClient:  opts.HistoryClient,
		taskQueue:      WorkflowTaskQueue.Get(opts.DynamicConfig),
		pendingTTL:     wciconfig.WorkerControllerCHASMMigrationPendingTTL.Get(opts.DynamicConfig),
		metricsHandler: opts.MetricsHandler,
	}
}

func (h *MigrateToWorkflowTaskHandler) Validate(_ chasm.Context, w *WorkerControllerInstance, _ chasm.TaskInvocation, _ *wcipb.MigrateToWorkflowTask) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, nil
}

type migrationTarget struct {
	start       *historyservice.StartWorkflowExecutionRequest
	namespaceID string
	namespace   string
	workflowID  string
	token       string
}

func (h *MigrateToWorkflowTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.MigrateToWorkflowTask) error {
	target, err := chasm.ReadComponent(ctx, ref, func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (migrationTarget, error) {
		groups := make(map[string]*wcipb.ScalingGroupState, len(w.ScalingGroups))
		for id, field := range w.ScalingGroups {
			if g, ok := field.TryGet(cctx); ok {
				groups[id] = g.ScalingGroupState
			}
		}
		ns := cctx.NamespaceEntry()
		args, err := migration.CHASMStateToWorkflow(ns.Name().String(), ns.ID().String(), w.WorkerControllerInstanceState, groups)
		if err != nil {
			return migrationTarget{}, err
		}
		args.PendingMigration = &iface.PendingMigration{
			Token:      w.MigrationToken,
			ExpireTime: timestamppb.New(cctx.Now(w).Add(h.pendingTTL())),
		}
		input, err := sdk.PreferProtoDataConverter.ToPayloads(args)
		if err != nil {
			return migrationTarget{}, err
		}
		workflowID := cctx.ExecutionKey().BusinessID
		return migrationTarget{
			namespaceID: ns.ID().String(),
			namespace:   ns.Name().String(),
			workflowID:  workflowID,
			token:       w.MigrationToken,
			start: &historyservice.StartWorkflowExecutionRequest{
				NamespaceId: ns.ID().String(),
				StartRequest: &workflowservice.StartWorkflowExecutionRequest{
					// The token keeps a retried start a duplicate of the first.
					RequestId:    w.MigrationToken,
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
			},
		}, nil
	}, nil)
	if err != nil {
		return err
	}

	activated, err := h.handOver(ctx, target)
	if err != nil {
		// Ambiguous: the task retries; the root stays frozen and the workflow stays pending.
		h.recordOutcome(target.namespace, migrationOutcomeError)
		return err
	}
	if !activated {
		h.abortWorkflow(ctx, target)
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		if !activated {
			MigrateToWorkflowCount.With(mctx.MetricsHandler()).
				Record(1, metrics.StringTag(wcimetrics.MigrationOutcomeTagName, migrationOutcomeAborted), metrics.StringTag(wcimetrics.BackendTagName, backendCHASM))
			return struct{}{}, TransitionWorkflowMigrationAborted.Apply(w, mctx, EventWorkflowMigrationAborted{})
		}
		MigrateToWorkflowCount.With(mctx.MetricsHandler()).
			Record(1, metrics.StringTag(wcimetrics.MigrationOutcomeTagName, migrationOutcomeSuccess), metrics.StringTag(wcimetrics.BackendTagName, backendCHASM))
		return struct{}{}, TransitionWorkflowActivated.Apply(w, mctx, EventWorkflowActivated{})
	}, nil)
	return err
}

// handOver starts the workflow pending and activates it. false is a definitive abort; an error is ambiguous.
func (h *MigrateToWorkflowTaskHandler) handOver(ctx context.Context, target migrationTarget) (bool, error) {
	_, err := h.historyClient.StartWorkflowExecution(ctx, target.start)
	// Another run owns the ID. It may be our activated run after a continue-as-new, which carries the
	// token and accepts the activation; any other run rejects it.
	if _, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); !ok && err != nil {
		return false, err
	}

	outcome, err := h.updateWorkflow(ctx, target, iface.UpdateActivateMigration)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		// Expired and completed before activation.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return outcome.GetFailure() == nil, nil
}

func (h *MigrateToWorkflowTaskHandler) abortWorkflow(ctx context.Context, target migrationTarget) {
	// Best effort: a pending workflow also expires on its own.
	_, _ = h.updateWorkflow(ctx, target, iface.UpdateAbortMigration)
}

func (h *MigrateToWorkflowTaskHandler) updateWorkflow(ctx context.Context, target migrationTarget, name string) (*updatepb.Outcome, error) {
	args, err := sdk.PreferProtoDataConverter.ToPayloads(&iface.MigrationTokenRequest{Token: target.token})
	if err != nil {
		return nil, err
	}
	resp, err := h.historyClient.UpdateWorkflowExecution(ctx, &historyservice.UpdateWorkflowExecutionRequest{
		NamespaceId: target.namespaceID,
		Request: &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         target.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: target.workflowID},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: name + ":" + target.token, Identity: "history-service"},
				Input: &updatepb.Input{Name: name, Args: args},
			},
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
		},
	})
	if err != nil {
		return nil, err
	}
	outcome := resp.GetResponse().GetOutcome()
	if outcome == nil {
		return nil, serviceerror.NewUnavailablef("%s update not completed yet", name)
	}
	return outcome, nil
}

func (h *MigrateToWorkflowTaskHandler) recordOutcome(namespaceName, outcome string) {
	MigrateToWorkflowCount.With(h.metricsHandler.WithTags(metrics.NamespaceTag(namespaceName))).
		Record(1, metrics.StringTag(wcimetrics.MigrationOutcomeTagName, outcome), metrics.StringTag(wcimetrics.BackendTagName, backendCHASM))
}
