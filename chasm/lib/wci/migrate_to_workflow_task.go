package wci

import (
	"context"
	"errors"
	"fmt"

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
	StaticConfig   *wciconfig.StaticConfig `optional:"true"`
}

// MigrateToWorkflowTaskHandler coordinates the CHASM → workflow handover: it starts the workflow
// pending, activates it, then marks the root MIGRATED; a definitive failure resumes the root.
type MigrateToWorkflowTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.MigrateToWorkflowTask]
	historyClient  resource.HistoryClient
	taskQueue      dynamicconfig.StringPropertyFn
	pendingTTL     dynamicconfig.DurationPropertyFn
	metricsHandler metrics.Handler
	hostRegion     string
}

func NewMigrateToWorkflowTaskHandler(opts MigrateToWorkflowTaskHandlerOptions) *MigrateToWorkflowTaskHandler {
	return &MigrateToWorkflowTaskHandler{
		historyClient:  opts.HistoryClient,
		taskQueue:      WorkflowTaskQueue.Get(opts.DynamicConfig),
		pendingTTL:     wciconfig.WorkerControllerCHASMMigrationPendingTTL.Get(opts.DynamicConfig),
		metricsHandler: opts.MetricsHandler,
		hostRegion:     hostRegionOf(opts.StaticConfig),
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
	// Task-adds queued while migrating, as of the read.
	taskAdds []*iface.SignalTaskAddRequest
}

func queuedTaskAdds(w *WorkerControllerInstance) []*iface.SignalTaskAddRequest {
	out := make([]*iface.SignalTaskAddRequest, len(w.MigrationTaskAdds))
	for i, event := range w.MigrationTaskAdds {
		out[i] = migration.TaskAddToWorkflow(event)
	}
	return out
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
		// Past due by activation, so the workflow polls at once and catches task-adds rejected while migrating.
		args.State.NextPollTime = timestamppb.New(cctx.Now(w))
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
			taskAdds:    queuedTaskAdds(w),
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
			if err := TransitionWorkflowMigrationAborted.Apply(w, mctx, EventWorkflowMigrationAborted{}); err != nil {
				return struct{}{}, err
			}
			w.recordHostRegion(h.hostRegion)
			// The workflow never activated, so it applied none of them.
			for _, event := range w.MigrationTaskAdds {
				if err := w.ProcessTaskAdd(mctx, event); err != nil {
					return struct{}{}, err
				}
			}
			w.MigrationTaskAdds = nil
			return struct{}{}, nil
		}
		MigrateToWorkflowCount.With(mctx.MetricsHandler()).
			Record(1, metrics.StringTag(wcimetrics.MigrationOutcomeTagName, migrationOutcomeSuccess), metrics.StringTag(wcimetrics.BackendTagName, backendCHASM))
		if err := TransitionWorkflowActivated.Apply(w, mctx, EventWorkflowActivated{}); err != nil {
			return struct{}{}, err
		}
		// Closed now, so later task-adds fall through to the workflow; forward those queued after the read.
		if len(w.MigrationTaskAdds) > len(target.taskAdds) {
			mctx.AddTask(w, chasm.TaskAttributes{}, &wcipb.ForwardTaskAddsTask{})
		}
		return struct{}{}, nil
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

	req, updateID := activationRequest(target)
	outcome, err := updateMigrationWorkflow(ctx, h.historyClient, target, iface.UpdateActivateMigration, updateID, req)
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
	_, _ = updateMigrationWorkflow(ctx, h.historyClient, target, iface.UpdateAbortMigration,
		iface.UpdateAbortMigration+":"+target.token, &iface.MigrationTokenRequest{Token: target.token})
}

// activationRequest carries the whole queue, numbered from 1; the update ID changes with its length so a
// longer queue isn't de-duplicated against a shorter one, and the workflow skips entries it has.
func activationRequest(target migrationTarget) (*iface.MigrationTokenRequest, string) {
	return &iface.MigrationTokenRequest{Token: target.token, TaskAdds: target.taskAdds, FirstTaskAddSeq: 1},
		fmt.Sprintf("%s:%s:%d", iface.UpdateActivateMigration, target.token, len(target.taskAdds))
}

func updateMigrationWorkflow(ctx context.Context, historyClient resource.HistoryClient, target migrationTarget, name, updateID string, req *iface.MigrationTokenRequest) (*updatepb.Outcome, error) {
	args, err := sdk.PreferProtoDataConverter.ToPayloads(req)
	if err != nil {
		return nil, err
	}
	resp, err := historyClient.UpdateWorkflowExecution(ctx, &historyservice.UpdateWorkflowExecutionRequest{
		NamespaceId: target.namespaceID,
		Request: &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         target.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: target.workflowID},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: updateID, Identity: "history-service"},
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

// ForwardTaskAddsTaskHandler sends the activated workflow the task-adds queued after the activation's read.
type ForwardTaskAddsTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.ForwardTaskAddsTask]
	historyClient resource.HistoryClient
}

func NewForwardTaskAddsTaskHandler(opts MigrateToWorkflowTaskHandlerOptions) *ForwardTaskAddsTaskHandler {
	return &ForwardTaskAddsTaskHandler{historyClient: opts.HistoryClient}
}

func (h *ForwardTaskAddsTaskHandler) Validate(_ chasm.Context, w *WorkerControllerInstance, _ chasm.TaskInvocation, _ *wcipb.ForwardTaskAddsTask) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_MIGRATED && len(w.MigrationTaskAdds) > 0, nil
}

func (h *ForwardTaskAddsTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.ForwardTaskAddsTask) error {
	target, err := chasm.ReadComponent(ctx, ref, func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (migrationTarget, error) {
		ns := cctx.NamespaceEntry()
		return migrationTarget{
			namespaceID: ns.ID().String(),
			namespace:   ns.Name().String(),
			workflowID:  cctx.ExecutionKey().BusinessID,
			token:       w.MigrationToken,
			taskAdds:    queuedTaskAdds(w),
		}, nil
	}, nil)
	if err != nil {
		return err
	}
	req, updateID := activationRequest(target)
	_, err = updateMigrationWorkflow(ctx, h.historyClient, target, iface.UpdateActivateMigration, updateID, req)
	// Gone (deleted, or moved on), or rejected by a run without the handler: nothing to deliver to.
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}
