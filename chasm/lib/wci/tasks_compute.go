package wci

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/chasm/lib/wci/migration"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

var errSpecMissing = errors.New("scaling group has no spec")

type ComputeTaskHandlerOptions struct {
	fx.In

	ComputeClient ComputeClient
	HistoryClient resource.HistoryClient
	Logger        log.Logger
}

// groupCall is what a group task reads under the lock before calling the provider without it.
type groupCall struct {
	rc           compute.RequestContext
	cfg          *iface.ComputeProviderSpec
	providerType string
	size         int32
}

func readGroupCall(ctx context.Context, ref chasm.ComponentRef) (groupCall, error) {
	return chasm.ReadComponent(ctx, ref, func(g *ScalingGroup, cctx chasm.Context, _ any) (groupCall, error) {
		root := g.Root.Get(cctx)
		spec, ok := root.ScalingGroupSpecs[g.Id]
		if !ok {
			return groupCall{}, errSpecMissing
		}
		cfg := migration.SpecToWorkflow(spec).Compute
		return groupCall{rc: requestContext(cctx, root), cfg: &cfg, providerType: string(cfg.ProviderType), size: g.DesiredSize}, nil
	}, nil)
}

func providerTypeOf(ctx chasm.Context, g *ScalingGroup) string {
	root, ok := g.Root.TryGet(ctx)
	if !ok {
		return ""
	}
	return root.ScalingGroupSpecs[g.Id].GetCompute().GetProviderType()
}

func failureOf(err error) *failurepb.Failure {
	return &failurepb.Failure{Message: err.Error()}
}

// ResizeTaskHandler drives UpdateWorkerSetSize; retries go through BACKING_OFF, not task retry.
type ResizeTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.ResizeTask]
	client ComputeClient
}

func NewResizeTaskHandler(opts ComputeTaskHandlerOptions) *ResizeTaskHandler {
	return &ResizeTaskHandler{client: opts.ComputeClient}
}

func (h *ResizeTaskHandler) Validate(ctx chasm.Context, g *ScalingGroup, _ chasm.TaskInvocation, task *wcipb.ResizeTask) (bool, error) {
	return g.Status == wcipb.SCALING_GROUP_STATUS_SCALING && g.Attempt == task.GetAttempt() && g.rootRunning(ctx), nil
}

func (h *ResizeTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.ResizeTask) error {
	call, err := readGroupCall(ctx, ref)
	if errors.Is(err, errSpecMissing) {
		return nil
	}
	if err != nil {
		return err
	}
	callErr := h.client.UpdateWorkerSetSize(ctx, call.rc, call.cfg, call.size)
	_, _, err = chasm.UpdateComponent(ctx, ref, func(g *ScalingGroup, mctx chasm.MutableContext, _ any) (struct{}, error) {
		switch {
		case callErr == nil:
			return struct{}{}, g.CompleteResize(mctx, call.size, call.providerType)
		case retryable(callErr):
			next := mctx.Now(g).Add(resizeBackoff.ComputeNextDelay(0, int(g.Attempt), callErr))
			return struct{}{}, TransitionResizeRetryable.Apply(g, mctx, EventResizeRetryable{Failure: failureOf(callErr), NextAttemptTime: next})
		default:
			if err := TransitionResizeFailed.Apply(g, mctx, EventResizeFailed{Failure: failureOf(callErr)}); err != nil {
				return struct{}{}, err
			}
			g.Root.Get(mctx).recordValidation(mctx, callErr)
			return struct{}{}, nil
		}
	}, nil)
	return err
}

// ScalingGroupBackoffTaskHandler ends a resize backoff.
type ScalingGroupBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *ScalingGroupBackoffTaskHandler) Validate(ctx chasm.Context, g *ScalingGroup, _ chasm.TaskInvocation, task *wcipb.ScalingGroupBackoffTask) (bool, error) {
	return g.Status == wcipb.SCALING_GROUP_STATUS_BACKING_OFF && g.Attempt == task.GetAttempt() && g.rootRunning(ctx), nil
}

func (h *ScalingGroupBackoffTaskHandler) Execute(ctx chasm.MutableContext, g *ScalingGroup, _ chasm.TaskAttributes, _ *wcipb.ScalingGroupBackoffTask) error {
	return TransitionBackoffElapsed.Apply(g, ctx, EventBackoffElapsed{ProviderType: providerTypeOf(ctx, g)})
}

// InvokeTaskHandler drives InvokeWorker; see finishInvoke for the hand-off.
type InvokeTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.InvokeTask]
	client ComputeClient
}

func NewInvokeTaskHandler(opts ComputeTaskHandlerOptions) *InvokeTaskHandler {
	return &InvokeTaskHandler{client: opts.ComputeClient}
}

func (h *InvokeTaskHandler) Validate(ctx chasm.Context, g *ScalingGroup, _ chasm.TaskInvocation, task *wcipb.InvokeTask) (bool, error) {
	return g.InflightInvokeId == task.GetRequestId() && g.InflightInvokeAttempt == task.GetAttempt() && g.rootRunning(ctx), nil
}

func (h *InvokeTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.InvokeTask) error {
	call, err := readGroupCall(ctx, ref)
	if errors.Is(err, errSpecMissing) {
		return nil
	}
	if err != nil {
		return err
	}
	callErr := h.client.InvokeWorker(ctx, call.rc, call.cfg)
	_, _, err = chasm.UpdateComponent(ctx, ref, func(g *ScalingGroup, mctx chasm.MutableContext, _ any) (struct{}, error) {
		g.Root.Get(mctx).finishInvoke(mctx, g, call.providerType, callErr)
		return struct{}{}, nil
	}, nil)
	return err
}

// ValidateSpecTaskHandler validates algorithm configs in history and provider configs via ComputeClient.
type ValidateSpecTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.ValidateSpecTask]
	client ComputeClient
	logger log.Logger
}

func NewValidateSpecTaskHandler(opts ComputeTaskHandlerOptions) *ValidateSpecTaskHandler {
	return &ValidateSpecTaskHandler{client: opts.ComputeClient, logger: opts.Logger}
}

func (h *ValidateSpecTaskHandler) Validate(_ chasm.Context, w *WorkerControllerInstance, _ chasm.TaskInvocation, task *wcipb.ValidateSpecTask) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_RUNNING && proto.Equal(task.GetValidationTime(), w.NextValidationTime), nil
}

func (h *ValidateSpecTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.ValidateSpecTask) error {
	type input struct {
		rc      compute.RequestContext
		spec    *iface.WorkerControllerInstanceSpec
		algoErr error
	}
	in, err := chasm.ReadComponent(ctx, ref, func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (input, error) {
		spec := w.spec()
		return input{rc: requestContext(cctx, w), spec: spec, algoErr: validateAlgorithms(w.algorithmContext(cctx), spec)}, nil
	}, nil)
	if err != nil {
		return err
	}
	validationErr := in.algoErr
	if validationErr == nil {
		validationErr = h.client.ValidateSpec(ctx, in.rc, in.spec)
	}
	// Transient failures are not spec failures; keep the last status, as the workflow does.
	if errors.Is(validationErr, context.DeadlineExceeded) || errors.Is(validationErr, context.Canceled) {
		h.logger.Warn("Transient failure validating WCI spec", tag.Error(validationErr))
		return nil
	}
	_, _, err = chasm.UpdateComponent(ctx, ref, func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		w.recordValidation(mctx, validationErr)
		return struct{}{}, nil
	}, nil)
	return err
}

// validateAlgorithms checks each group's algorithm config; CPU-only.
func validateAlgorithms(algoCtx context.Context, spec *iface.WorkerControllerInstanceSpec) error {
	for id, group := range spec.ScalingGroupSpecs {
		if group.Scaling == nil {
			continue
		}
		algo, config, err := algorithmFor(algoCtx, group)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		if err := algo.ValidateConfig(algoCtx, config); err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
	}
	return nil
}

// SignalVersionWorkflowTaskHandler sends the newest validation status to the version workflow.
type SignalVersionWorkflowTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*wcipb.SignalVersionWorkflowTask]
	historyClient resource.HistoryClient
}

func NewSignalVersionWorkflowTaskHandler(opts ComputeTaskHandlerOptions) *SignalVersionWorkflowTaskHandler {
	return &SignalVersionWorkflowTaskHandler{historyClient: opts.HistoryClient}
}

func (h *SignalVersionWorkflowTaskHandler) Validate(_ chasm.Context, w *WorkerControllerInstance, _ chasm.TaskInvocation, task *wcipb.SignalVersionWorkflowTask) (bool, error) {
	return w.Status == wcipb.WCI_STATUS_RUNNING && proto.Equal(task.GetLastValidationTime(), w.ValidationStatus.GetLastValidationTime()), nil
}

func (h *SignalVersionWorkflowTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, _ *wcipb.SignalVersionWorkflowTask) error {
	req, err := chasm.ReadComponent(ctx, ref, func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (*historyservice.SignalWorkflowExecutionRequest, error) {
		status, err := migration.ValidationToWorkflow(w.ValidationStatus)
		if err != nil {
			return nil, err
		}
		input, err := sdk.PreferProtoDataConverter.ToPayloads(status)
		if err != nil {
			return nil, err
		}
		ns := cctx.NamespaceEntry()
		version := worker_versioning.ExternalWorkerDeploymentVersionToString(&deploymentpb.WorkerDeploymentVersion{DeploymentName: w.DeploymentName, BuildId: w.BuildId})
		return &historyservice.SignalWorkflowExecutionRequest{
			NamespaceId: ns.ID().String(),
			SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: ns.Name().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: worker_versioning.WorkerDeploymentVersionWorkflowIDPrefix + worker_versioning.WorkerDeploymentVersionDelimiter + version,
				},
				SignalName: worker_versioning.SignalSyncValidationStatus,
				Input:      input,
				Identity:   "history-service",
			},
		}, nil
	}, nil)
	if err != nil {
		return err
	}
	_, err = h.historyClient.SignalWorkflowExecution(ctx, req)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil
	}
	return err
}
