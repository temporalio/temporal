package wci

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/chasm/lib/wci/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

type HandlerOptions struct {
	fx.In

	ComputeClient     ComputeClient
	FrontendClient    workflowservice.WorkflowServiceClient
	NamespaceRegistry namespace.Registry
	Logger            log.Logger
}

type handler struct {
	wcipb.UnimplementedWorkerControllerInstanceServiceServer

	compute    ComputeClient
	frontend   workflowservice.WorkflowServiceClient
	namespaces namespace.Registry
	logger     log.Logger
}

func newHandler(opts HandlerOptions) *handler {
	return &handler{
		compute:    opts.ComputeClient,
		frontend:   opts.FrontendClient,
		namespaces: opts.NamespaceRegistry,
		logger:     opts.Logger,
	}
}

func refFor(namespaceID, instanceID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*WorkerControllerInstance](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: instanceID})
}

// snapshotOf is what a request reads under the lock before working without it.
type snapshotOf struct {
	exists   bool
	state    *wcipb.WorkerControllerInstanceState
	spec     *iface.WorkerControllerInstanceSpec
	statuses map[string]iface.ScalingAlgorithmStatus
}

// read returns the current instance; a missing execution reads as an empty, non-existent one.
func read(ctx context.Context, namespaceID, instanceID string) (snapshotOf, error) {
	out, err := chasm.ReadComponent(ctx, refFor(namespaceID, instanceID), func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (snapshotOf, error) {
		if w.Status == wcipb.WCI_STATUS_PENDING_ACTIVATION {
			return snapshotOf{}, errPendingInvisible
		}
		if w.LifecycleState(cctx).IsClosed() {
			return snapshotOf{}, ErrClosed
		}
		statuses := map[string]iface.ScalingAlgorithmStatus{}
		for id, field := range w.ScalingGroups {
			if g, ok := field.TryGet(cctx); ok {
				status, err := g.algorithmStatus()
				if err != nil {
					return snapshotOf{}, err
				}
				statuses[id] = status
			}
		}
		return snapshotOf{exists: true, state: common.CloneProto(w.WorkerControllerInstanceState), spec: w.spec(), statuses: statuses}, nil
	}, nil)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return snapshotOf{spec: &iface.WorkerControllerInstanceSpec{}}, nil
	}
	return out, err
}

func (h *handler) requestContext(namespaceID, deploymentName, buildID string) (compute.RequestContext, error) {
	name, err := h.namespaces.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return compute.RequestContext{}, err
	}
	return compute.RequestContext{NamespaceID: namespaceID, NamespaceName: name.String(), DeploymentName: deploymentName, DeploymentBuildID: buildID}, nil
}

func (h *handler) algorithmContext(ctx context.Context) context.Context {
	return scalingalgorithm.WithLogger(ctx, log.NewSdkLogger(h.logger))
}

// checkSupported rejects provider types CHASM cannot run, so the caller keeps the WCI a workflow.
func (h *handler) checkSupported(spec *iface.WorkerControllerInstanceSpec) error {
	for _, group := range spec.ScalingGroupSpecs {
		if !h.compute.Supports(string(group.Compute.ProviderType)) {
			return ErrProviderNotSupportedOnCHASM
		}
	}
	return nil
}

// validate runs every check that needs no state change; provider checks run without the lock.
func (h *handler) validate(ctx context.Context, rc compute.RequestContext, spec *iface.WorkerControllerInstanceSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if err := h.checkSupported(spec); err != nil {
		return err
	}
	if err := validateAlgorithms(h.algorithmContext(ctx), spec); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if err := h.compute.ValidateSpec(ctx, rc, spec); err != nil {
		if errors.Is(err, ErrProviderNotSupportedOnCHASM) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return err
		}
		return serviceerror.NewInvalidArgument(err.Error())
	}
	return nil
}

func upsertsToIface(upserts map[string]*wcipb.ScalingGroupSpecUpdate) map[string]iface.ScalingGroupSpecUpdate {
	out := make(map[string]iface.ScalingGroupSpecUpdate, len(upserts))
	for id, u := range upserts {
		out[id] = iface.ScalingGroupSpecUpdate{Spec: migration.SpecToWorkflow(u.GetSpec()), UpdateMask: u.GetUpdateMask()}
	}
	return out
}

func specsToCHASM(spec *iface.WorkerControllerInstanceSpec) map[string]*wcipb.ScalingGroupSpec {
	out := make(map[string]*wcipb.ScalingGroupSpec, len(spec.ScalingGroupSpecs))
	for id, group := range spec.ScalingGroupSpecs {
		out[id] = migration.SpecToCHASM(group)
	}
	return out
}

// UpdateWorkerControllerInstance validates and registers task queues without the lock, then
// applies the spec with update-with-start (create if missing).
func (h *handler) UpdateWorkerControllerInstance(ctx context.Context, req *wcipb.UpdateWorkerControllerInstanceRequest) (resp *wcipb.UpdateWorkerControllerInstanceResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	cur, err := read(ctx, req.GetNamespaceId(), req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	if cur.exists && len(req.GetConflictToken()) > 0 && !bytes.Equal(req.GetConflictToken(), cur.state.GetConflictToken()) {
		return nil, serviceerror.NewFailedPrecondition("conflict token mismatch")
	}
	merge := &iface.UpdateWorkerControllerInstanceRequest{
		UpsertScalingGroups: upsertsToIface(req.GetUpsertScalingGroups()),
		RemoveScalingGroups: req.GetRemoveScalingGroups(),
	}
	updated, err := iface.BuildUpdatedSpec(cur.spec, merge)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}

	// Zero groups left is an implicit delete, as in the workflow.
	if len(updated.ScalingGroupSpecs) == 0 {
		if !cur.exists {
			return &wcipb.UpdateWorkerControllerInstanceResponse{}, nil
		}
		_, _, err = chasm.UpdateComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
			if err := w.CheckMutable(); err != nil {
				return struct{}{}, err
			}
			w.ConflictToken = req.GetConflictToken()
			return struct{}{}, TransitionDelete.Apply(w, mctx, EventDelete{Identity: req.GetIdentity()})
		}, nil)
		return &wcipb.UpdateWorkerControllerInstanceResponse{ConflictToken: req.GetConflictToken()}, err
	}

	rc, err := h.requestContext(req.GetNamespaceId(), req.GetDeploymentName(), req.GetBuildId())
	if err != nil {
		return nil, err
	}
	if err := h.validate(ctx, rc, updated); err != nil {
		return nil, err
	}
	registrations, err := registerTaskQueues(ctx, h.algorithmContext(ctx), h.frontend, h.compute, h.logger, rc, updated, cur.statuses)
	if err != nil {
		return nil, err
	}

	// apply re-merges against the current spec so a concurrent update is not overwritten wholesale.
	apply := func(w *WorkerControllerInstance, mctx chasm.MutableContext) (*wcipb.UpdateWorkerControllerInstanceResponse, error) {
		final, err := iface.BuildUpdatedSpec(w.spec(), merge)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(err.Error())
		}
		if err := final.Validate(); err != nil {
			return nil, err
		}
		w.applySpec(mctx, specsToCHASM(final), registrations)
		w.LastModifierIdentity = req.GetIdentity()
		w.ConflictToken = req.GetConflictToken()
		return &wcipb.UpdateWorkerControllerInstanceResponse{ScalingGroupSpecs: w.ScalingGroupSpecs, ConflictToken: w.ConflictToken}, nil
	}

	var created *WorkerControllerInstance
	var createdResp *wcipb.UpdateWorkerControllerInstanceResponse
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetInstanceId()},
		func(mctx chasm.MutableContext, _ any) (*WorkerControllerInstance, error) {
			// Short-term workaround: the engine drops children added by updateFn on create (see plan.md, 5c).
			created = NewWorkerControllerInstance(mctx, req.GetDeploymentName(), req.GetBuildId(), nil)
			resp, err := apply(created, mctx)
			createdResp = resp
			return created, err
		},
		func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (*wcipb.UpdateWorkerControllerInstanceResponse, error) {
			if w == created {
				return createdResp, nil
			}
			if err := w.CheckMutable(); err != nil {
				return nil, err
			}
			return apply(w, mctx)
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return result.UpdateOutput, nil
}

func (h *handler) DescribeWorkerControllerInstance(ctx context.Context, req *wcipb.DescribeWorkerControllerInstanceRequest) (resp *wcipb.DescribeWorkerControllerInstanceResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	return chasm.ReadComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, cctx chasm.Context, _ any) (*wcipb.DescribeWorkerControllerInstanceResponse, error) {
		if w.Status == wcipb.WCI_STATUS_PENDING_ACTIVATION {
			return nil, errPendingInvisible
		}
		if w.LifecycleState(cctx).IsClosed() {
			return nil, ErrClosed
		}
		observations := make(map[string]*wcipb.TaskQueueObservations, len(w.ScalingGroups))
		for id, field := range w.ScalingGroups {
			if g, ok := field.TryGet(cctx); ok && g.Observations != nil {
				observations[id] = g.Observations
			}
		}
		return &wcipb.DescribeWorkerControllerInstanceResponse{
			DeploymentName:        w.DeploymentName,
			BuildId:               w.BuildId,
			CreateTime:            w.CreateTime,
			ScalingGroupSpecs:     w.ScalingGroupSpecs,
			ConflictToken:         w.ConflictToken,
			LastModifierIdentity:  w.LastModifierIdentity,
			ValidationStatus:      w.ValidationStatus,
			TaskQueueObservations: observations,
		}, nil
	}, nil)
}

func (h *handler) DeleteWorkerControllerInstance(ctx context.Context, req *wcipb.DeleteWorkerControllerInstanceRequest) (resp *wcipb.DeleteWorkerControllerInstanceResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	_, _, err = chasm.UpdateComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		if err := w.CheckMutable(); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, TransitionDelete.Apply(w, mctx, EventDelete{Identity: req.GetIdentity()})
	}, nil)
	if err != nil {
		return nil, err
	}
	return &wcipb.DeleteWorkerControllerInstanceResponse{}, nil
}

// ValidateWorkerControllerInstanceSpec is a dry run: the update's checks, nothing written.
func (h *handler) ValidateWorkerControllerInstanceSpec(ctx context.Context, req *wcipb.ValidateWorkerControllerInstanceSpecRequest) (resp *wcipb.ValidateWorkerControllerInstanceSpecResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	cur, err := read(ctx, req.GetNamespaceId(), req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	updated, err := iface.BuildUpdatedSpec(cur.spec, &iface.UpdateWorkerControllerInstanceRequest{
		UpsertScalingGroups: upsertsToIface(req.GetUpsertScalingGroups()),
		RemoveScalingGroups: req.GetRemoveScalingGroups(),
	})
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	rc, err := h.requestContext(req.GetNamespaceId(), cur.state.GetDeploymentName(), cur.state.GetBuildId())
	if err != nil {
		return nil, err
	}
	if err := h.validate(ctx, rc, updated); err != nil {
		return nil, err
	}
	return &wcipb.ValidateWorkerControllerInstanceSpecResponse{}, nil
}

func (h *handler) RecordTaskAdd(ctx context.Context, req *wcipb.RecordTaskAddRequest) (resp *wcipb.RecordTaskAddResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	_, _, err = chasm.UpdateComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		if err := w.CheckMutable(); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, w.ProcessTaskAdd(mctx, req.GetEvent())
	}, nil)
	if err != nil {
		return nil, err
	}
	return &wcipb.RecordTaskAddResponse{}, nil
}

// CreateFromMigrationState prepares a workflow → CHASM handover: the instance is created pending.
// Repeating it with the same token is a no-op; any other existing instance rejects it.
func (h *handler) CreateFromMigrationState(ctx context.Context, req *wcipb.CreateFromMigrationStateRequest) (resp *wcipb.CreateFromMigrationStateResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	spec := &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{}}
	for id, group := range req.GetState().GetScalingGroupSpecs() {
		spec.ScalingGroupSpecs[id] = migration.SpecToWorkflow(group)
	}
	if err := h.checkSupported(spec); err != nil {
		return nil, err
	}
	if req.GetMigrationToken() == "" || req.GetExpireTime() == nil {
		return nil, serviceerror.NewInvalidArgument("migration token and expire time are required")
	}

	_, err = chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetInstanceId()},
		func(mctx chasm.MutableContext, req *wcipb.CreateFromMigrationStateRequest) (*WorkerControllerInstance, error) {
			return NewWorkerControllerInstanceFromMigration(mctx, req.GetState(), req.GetScalingGroups(), req.GetMigrationToken(), req.GetExpireTime().AsTime()), nil
		},
		req,
	)
	if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
		return &wcipb.CreateFromMigrationStateResponse{}, h.checkSameMigration(ctx, req)
	}
	if err != nil {
		return nil, err
	}
	return &wcipb.CreateFromMigrationStateResponse{}, nil
}

func (h *handler) checkSameMigration(ctx context.Context, req *wcipb.CreateFromMigrationStateRequest) error {
	_, err := chasm.ReadComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, _ chasm.Context, _ any) (struct{}, error) {
		if w.MigrationToken == req.GetMigrationToken() && (w.Status == wcipb.WCI_STATUS_PENDING_ACTIVATION || w.Status == wcipb.WCI_STATUS_RUNNING) {
			return struct{}{}, nil
		}
		return struct{}{}, serviceerror.NewAlreadyExistsf("worker controller instance %q already exists", req.GetInstanceId())
	}, nil)
	return err
}

// ActivateMigration commits a workflow → CHASM handover and replays the queued task-adds with it.
func (h *handler) ActivateMigration(ctx context.Context, req *wcipb.ActivateMigrationRequest) (resp *wcipb.ActivateMigrationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	_, _, err = chasm.UpdateComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		if w.MigrationToken != req.GetMigrationToken() {
			return struct{}{}, ErrMigrationNotPending
		}
		switch w.Status {
		case wcipb.WCI_STATUS_RUNNING:
			// A retried activation.
			return struct{}{}, nil
		case wcipb.WCI_STATUS_PENDING_ACTIVATION:
		default:
			return struct{}{}, ErrMigrationNotPending
		}
		if err := TransitionMigrationActivated.Apply(w, mctx, EventMigrationActivated{}); err != nil {
			return struct{}{}, err
		}
		for _, event := range req.GetPendingTaskAdds() {
			if err := w.ProcessTaskAdd(mctx, event); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	}, nil)
	if err != nil {
		return nil, err
	}
	return &wcipb.ActivateMigrationResponse{}, nil
}

// AbortMigration closes a pending target; a missing or already aborted one is success.
func (h *handler) AbortMigration(ctx context.Context, req *wcipb.AbortMigrationRequest) (resp *wcipb.AbortMigrationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	_, _, err = chasm.UpdateComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		if w.MigrationToken != req.GetMigrationToken() {
			return struct{}{}, ErrMigrationNotPending
		}
		switch w.Status {
		case wcipb.WCI_STATUS_MIGRATION_ABORTED:
			return struct{}{}, nil
		case wcipb.WCI_STATUS_PENDING_ACTIVATION:
			return struct{}{}, TransitionPendingMigrationAborted.Apply(w, mctx, EventPendingMigrationAborted{})
		default:
			return struct{}{}, ErrMigrationNotPending
		}
	}, nil)
	var notFound *serviceerror.NotFound
	if err != nil && !errors.As(err, &notFound) {
		return nil, err
	}
	return &wcipb.AbortMigrationResponse{}, nil
}

// MigrateToWorkflow hands the WCI back to the workflow backend; repeating it while migrating is a no-op.
func (h *handler) MigrateToWorkflow(ctx context.Context, req *wcipb.MigrateToWorkflowRequest) (resp *wcipb.MigrateToWorkflowResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	_, _, err = chasm.UpdateComponent(ctx, refFor(req.GetNamespaceId(), req.GetInstanceId()), func(w *WorkerControllerInstance, mctx chasm.MutableContext, _ any) (struct{}, error) {
		if w.Status == wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW {
			return struct{}{}, nil
		}
		if err := w.CheckMutable(); err != nil {
			return struct{}{}, err
		}
		// Doubles as the workflow start request ID and the activate update ID, so retries de-duplicate.
		token := fmt.Sprintf("chasm:%d", mctx.ExecutionInfo().StateTransitionCount+1)
		return struct{}{}, TransitionMigrateToWorkflow.Apply(w, mctx, EventMigrateToWorkflow{Token: token})
	}, nil)
	if err != nil {
		return nil, err
	}
	return &wcipb.MigrateToWorkflowResponse{}, nil
}

// applySpec sets the spec, reconciles the group children and records a successful validation.
func (w *WorkerControllerInstance) applySpec(ctx chasm.MutableContext, specs map[string]*wcipb.ScalingGroupSpec, registrations map[string]registration) {
	for id := range w.ScalingGroups {
		if _, ok := specs[id]; !ok {
			delete(w.ScalingGroups, id)
		}
	}
	if w.ScalingGroups == nil {
		w.ScalingGroups = make(chasm.Map[string, *ScalingGroup], len(specs))
	}
	for id := range specs {
		if _, ok := w.ScalingGroups[id]; !ok {
			w.ScalingGroups[id] = chasm.NewComponentField(ctx, NewScalingGroup(id))
		}
	}
	w.ScalingGroupSpecs = specs

	for id, reg := range registrations {
		field, ok := w.ScalingGroups[id]
		if !ok {
			continue
		}
		g := field.Get(ctx)
		if reg.status != nil {
			if err := g.setAlgorithmStatus(reg.status); err != nil {
				ctx.Logger().Warn("Failed to persist registration status")
			}
		}
		// The registration already sized the worker set; record it so the next resize starts from it.
		if reg.size != nil && g.Status == wcipb.SCALING_GROUP_STATUS_IDLE {
			g.DesiredSize = *reg.size
			g.AppliedSize = *reg.size
		}
	}
	w.recordValidation(ctx, nil)
}
