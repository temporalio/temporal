package nsrepl

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/fx"
)

// -----------------------------------------------------------------------------
// ApplyLocalTask — strict CAS write to the host cell's metadata store.
// -----------------------------------------------------------------------------

type applyLocalTaskHandlerOptions struct {
	fx.In

	MetadataManager persistence.MetadataManager
	MetricsHandler  metrics.Handler
	Logger          log.Logger
}

type applyLocalTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nsreplpb.ApplyLocalTask]

	metadataManager persistence.MetadataManager
	// TODO(nsrepl): emit metrics for the local apply path. Suggested shape:
	//   - nsrepl_apply_attempts_total{outcome="local"}     counter
	//   - nsrepl_apply_failures_total{outcome="local"}     counter
	//   - nsrepl_apply_duration_seconds{outcome="local"}   histogram
	// metricsHandler is wired through fx but not yet used.
	metricsHandler metrics.Handler
	logger         log.Logger
}

func newApplyLocalTaskHandler(opts applyLocalTaskHandlerOptions) *applyLocalTaskHandler {
	return &applyLocalTaskHandler{
		metadataManager: opts.MetadataManager,
		metricsHandler:  opts.MetricsHandler,
		logger:          opts.Logger,
	}
}

// Validate gates execution: only run if the component is still RUNNING and the
// local apply hasn't already been recorded as committed.
func (h *applyLocalTaskHandler) Validate(
	_ chasm.Context,
	c *NamespaceMutationComponent,
	_ chasm.TaskAttributes,
	_ *nsreplpb.ApplyLocalTask,
) (bool, error) {
	if c.GetStatus() != nsreplpb.COMPONENT_STATUS_RUNNING {
		return false, nil
	}
	if c.GetLocalApply().GetOutcome() != nsreplpb.LOCAL_APPLY_OUTCOME_PENDING {
		return false, nil
	}
	return true, nil
}

// Execute writes to the local metadata store with version-CAS, then transitions
// the component to COMMITTED (and schedules peer fan-out) or FAILED.
func (h *applyLocalTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *nsreplpb.ApplyLocalTask,
) error {
	// Read the mutation payload from component state.
	type loadResult struct {
		Operation   nsreplpb.NamespaceOperation
		Detail      *persistencespb.NamespaceDetail
		ExpectedVer int64
		IsGlobal    bool
	}
	loaded, err := chasm.ReadComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (loadResult, error) {
			m := c.GetMutation()
			return loadResult{
				Operation:   m.GetOperation(),
				Detail:      m.GetNamespaceDetail(),
				ExpectedVer: m.GetExpectedVersion(),
				// Anything that reaches the CHASM transport is a global namespace —
				// the frontend's shouldUseCHASMReplication gate ensures local-only
				// namespaces never get here. Hardcoded rather than read from the
				// mutation to avoid drift.
				IsGlobal: true,
			}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read chasm component details: %w", err)
	}

	// Apply to the local metadata store. CAS conflict, validation, and store-unavailable
	// errors are all surfaced here; CAS conflict is treated as terminal (true conflict).
	switch loaded.Operation {
	case nsreplpb.NAMESPACE_OPERATION_CREATE:
		if _, applyErr := h.metadataManager.CreateNamespace(ctx, &persistence.CreateNamespaceRequest{
			Namespace:         loaded.Detail,
			IsGlobalNamespace: loaded.IsGlobal,
		}); applyErr != nil {
			return h.recordLocalFailure(ctx, ref, applyErr)
		}
	case nsreplpb.NAMESPACE_OPERATION_UPDATE:
		if applyErr := h.metadataManager.UpdateNamespace(ctx, &persistence.UpdateNamespaceRequest{
			Namespace:           loaded.Detail,
			IsGlobalNamespace:   loaded.IsGlobal,
			NotificationVersion: loaded.ExpectedVer,
		}); applyErr != nil {
			return h.recordLocalFailure(ctx, ref, applyErr)
		}
	default:
		return h.recordLocalFailure(ctx, ref, fmt.Errorf("unsupported namespace operation: %v", loaded.Operation))
	}

	// Read back the post-write notification_version from the metadata store. The
	// CAS write itself doesn't return the new version (Create returns ID; Update
	// returns only error), so we query after to get the truth. Used as the
	// component's NewVersion and surfaced in the gRPC response.
	meta, metaErr := h.metadataManager.GetMetadata(ctx)
	if metaErr != nil {
		// We've already committed locally; failing to read back the version is a
		// rare degenerate case. Record local failure so the caller sees the error
		// rather than getting a misleadingly-wrong version.
		return h.recordLocalFailure(ctx, ref, fmt.Errorf("read post-write notification_version: %w", metaErr))
	}
	newVersion := meta.NotificationVersion

	// Commit transition: record success and schedule peer fan-out.
	_, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			return nil, TransitionLocalCommitted.Apply(c, mctx, EventLocalCommitted{
				Time:       mctx.Now(c),
				NewVersion: newVersion,
			})
		},
		nil,
	)
	return err
}

func (h *applyLocalTaskHandler) recordLocalFailure(
	ctx context.Context,
	ref chasm.ComponentRef,
	applyErr error,
) error {
	h.logger.Warn("nsrepl local apply failed",
		tag.NewStringTag("namespace_id", ref.BusinessID),
		tag.Error(applyErr),
	)
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			return nil, TransitionLocalFailed.Apply(c, mctx, EventLocalFailed{
				Time: mctx.Now(c),
				Err:  applyErr,
			})
		},
		nil,
	)
	return err
}

// -----------------------------------------------------------------------------
// ApplyPeerTask — fans out to a peer cell via the ApplyNamespaceMutation admin
// RPC. One task instance per peer cell, scheduled in parallel after the local
// apply commits. Apply-if-higher semantics on the receiver make retries safe.
// -----------------------------------------------------------------------------

type applyPeerTaskHandlerOptions struct {
	fx.In

	ClientBean     serverClient.Bean
	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type applyPeerTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nsreplpb.ApplyPeerTask]

	clientBean serverClient.Bean
	// TODO(nsrepl): emit metrics for the peer apply path. Suggested shape:
	//   - nsrepl_apply_attempts_total{target_cell, source_cell, outcome}    counter
	//   - nsrepl_apply_failures_total{target_cell, source_cell}             counter
	//   - nsrepl_apply_duration_seconds{target_cell, source_cell}           histogram
	// metricsHandler is wired through fx but not yet used.
	//
	// TODO(nsrepl): configure an explicit retry policy for retriable peer
	// failures. Target shape: exponential backoff, ~1 hour total budget,
	// ~20 attempts. Today we return the error and rely on CHASM's default task
	// retry, which works but won't absorb long peer outages as gracefully.
	metricsHandler metrics.Handler
	logger         log.Logger
}

func newApplyPeerTaskHandler(opts applyPeerTaskHandlerOptions) *applyPeerTaskHandler {
	return &applyPeerTaskHandler{
		clientBean:     opts.ClientBean,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

// Validate gates execution: the local apply must have committed, this peer's
// status must still be PENDING, and the attempt number must match. The attempt
// gating prevents stale retries (callback library uses the same pattern).
func (h *applyPeerTaskHandler) Validate(
	_ chasm.Context,
	c *NamespaceMutationComponent,
	_ chasm.TaskAttributes,
	task *nsreplpb.ApplyPeerTask,
) (bool, error) {
	if c.GetStatus() != nsreplpb.COMPONENT_STATUS_RUNNING {
		return false, nil
	}
	if c.GetLocalApply().GetOutcome() != nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED {
		return false, nil
	}
	peer := c.GetPeerApply()[task.GetTargetCell()]
	if peer == nil {
		return false, nil
	}
	if peer.GetOutcome() != nsreplpb.PEER_APPLY_OUTCOME_PENDING {
		return false, nil
	}
	if peer.GetAttemptCount() != task.GetAttempt() {
		return false, nil
	}
	return true, nil
}

// Execute calls the cross-cluster ApplyNamespaceMutation admin RPC against the
// target cell. Classifies the response and records the outcome on the component.
func (h *applyPeerTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *nsreplpb.ApplyPeerTask,
) error {
	// Load the mutation payload from component state.
	type loadResult struct {
		Operation enumsspb.NamespaceOperation
		Detail    *persistencespb.NamespaceDetail
	}
	loaded, readErr := chasm.ReadComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (loadResult, error) {
			m := c.GetMutation()
			return loadResult{
				Operation: convertOperation(m.GetOperation()),
				Detail:    m.GetNamespaceDetail(),
			}, nil
		},
		nil,
	)
	if readErr != nil {
		return fmt.Errorf("read component: %w", readErr)
	}

	// Dial the target cell.
	adminClient, dialErr := h.clientBean.GetRemoteAdminClient(task.GetTargetCell())
	if dialErr != nil {
		return h.recordPeerOutcome(ctx, ref, task, nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE, dialErr)
	}

	// Invoke ApplyNamespaceMutation. The request reuses the existing
	// NamespaceTaskAttributes wire shape so the receiver-side apply-if-higher
	// logic can be reused unchanged.
	req := &adminservice.ApplyNamespaceMutationRequest{
		NamespaceTask: buildNamespaceTaskAttributes(loaded.Operation, loaded.Detail),
	}
	resp, rpcErr := adminClient.ApplyNamespaceMutation(ctx, req)
	if rpcErr != nil {
		outcome := classifyPeerErr(rpcErr)
		return h.recordPeerOutcome(ctx, ref, task, outcome, rpcErr)
	}

	// Map the admin RPC's outcome to the component's per-peer outcome enum.
	outcome := nsreplpb.PEER_APPLY_OUTCOME_APPLIED
	if resp.GetOutcome() == adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE {
		outcome = nsreplpb.PEER_APPLY_OUTCOME_NO_OP_STALE
	}
	return h.recordPeerOutcome(ctx, ref, task, outcome, nil)
}

// recordPeerOutcome writes the outcome of a peer apply to the component state.
// Called for both success and failure paths.
func (h *applyPeerTaskHandler) recordPeerOutcome(
	ctx context.Context,
	ref chasm.ComponentRef,
	task *nsreplpb.ApplyPeerTask,
	outcome nsreplpb.PeerApplyOutcome,
	execErr error,
) error {
	if execErr != nil {
		h.logger.Warn("nsrepl peer apply failed",
			tag.NewStringTag("namespace_id", ref.BusinessID),
			tag.NewStringTag("target_cell", task.GetTargetCell()),
			tag.NewInt32("attempt", task.GetAttempt()),
			tag.Error(execErr),
		)
	}
	_, _, updErr := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			if err := TransitionPeerCompleted.Apply(c, mctx, EventPeerCompleted{
				Time:       mctx.Now(c),
				TargetCell: task.GetTargetCell(),
				Outcome:    outcome,
				Attempts:   task.GetAttempt() + 1,
				Err:        execErr,
			}); err != nil {
				return nil, err
			}
			// If every peer has reached a terminal outcome, move to COMPLETED so
			// retention can clean up the component. (Done as a separate transition
			// because the framework rewrites status to the transition's destination
			// after apply returns.)
			if c.allPeersTerminal() {
				if err := TransitionAllPeersTerminal.Apply(c, mctx, EventAllPeersTerminal{}); err != nil {
					return nil, err
				}
			}
			return nil, nil
		},
		nil,
	)
	if updErr != nil {
		return updErr
	}
	if execErr != nil && outcome == nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE {
		// Returning an error lets CHASM schedule a retry via its task framework.
		// Terminal failures intentionally don't return an error: they end the
		// task and the structured failure log is the operator's signal to act.
		return wrapPeerError(task.GetTargetCell(), execErr)
	}
	return nil
}

// classifyPeerErr maps an admin RPC error to the appropriate peer apply outcome.
// Transient errors (peer unavailable, resource exhausted, deadline) are
// retriable. Argument validation and not-found errors are terminal — retrying
// won't help. Unknown errors default to retriable (safer: apply-if-higher makes
// duplicate writes no-ops).
func classifyPeerErr(err error) nsreplpb.PeerApplyOutcome {
	switch err.(type) {
	case *serviceerror.Unavailable,
		*serviceerror.ResourceExhausted,
		*serviceerror.DeadlineExceeded:
		return nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	case *serviceerror.InvalidArgument,
		*serviceerror.NotFound:
		return nsreplpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
	default:
		return nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	}
}

// convertOperation maps our local NamespaceOperation enum to the OSS one used in
// NamespaceTaskAttributes. Same values, different proto package.
func convertOperation(op nsreplpb.NamespaceOperation) enumsspb.NamespaceOperation {
	switch op {
	case nsreplpb.NAMESPACE_OPERATION_CREATE:
		return enumsspb.NAMESPACE_OPERATION_CREATE
	case nsreplpb.NAMESPACE_OPERATION_UPDATE:
		return enumsspb.NAMESPACE_OPERATION_UPDATE
	default:
		return enumsspb.NAMESPACE_OPERATION_UNSPECIFIED
	}
}

// buildNamespaceTaskAttributes converts the mutation into the NamespaceTaskAttributes
// wire shape that the receiver-side apply-if-higher logic consumes. Mirrors the
// conversion in transmission_task_handler.go's HandleTransmissionTask.
func buildNamespaceTaskAttributes(
	op enumsspb.NamespaceOperation,
	detail *persistencespb.NamespaceDetail,
) *replicationspb.NamespaceTaskAttributes {
	info := detail.GetInfo()
	config := detail.GetConfig()
	replConfig := detail.GetReplicationConfig()

	clusters := make([]*replicationpb.ClusterReplicationConfig, 0, len(replConfig.GetClusters()))
	for _, c := range replConfig.GetClusters() {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{ClusterName: c})
	}

	history := make([]*replicationpb.FailoverStatus, 0, len(replConfig.GetFailoverHistory()))
	for _, s := range replConfig.GetFailoverHistory() {
		history = append(history, &replicationpb.FailoverStatus{
			FailoverTime:    s.GetFailoverTime(),
			FailoverVersion: s.GetFailoverVersion(),
		})
	}

	attrs := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: op,
		Id:                 info.GetId(),
		Info: &namespacepb.NamespaceInfo{
			Name:        info.GetName(),
			State:       info.GetState(),
			Description: info.GetDescription(),
			OwnerEmail:  info.GetOwner(),
			Data:        info.GetData(),
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: config.GetRetention(),
			HistoryArchivalState:          config.GetHistoryArchivalState(),
			HistoryArchivalUri:            config.GetHistoryArchivalUri(),
			VisibilityArchivalState:       config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:         config.GetVisibilityArchivalUri(),
			BadBinaries:                   config.GetBadBinaries(),
			CustomSearchAttributeAliases:  config.GetCustomSearchAttributeAliases(),
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: replConfig.GetActiveClusterName(),
			Clusters:          clusters,
		},
		ConfigVersion:   detail.GetConfigVersion(),
		FailoverVersion: detail.GetFailoverVersion(),
		FailoverHistory: history,
	}
	if replConfig.GetState() == enumspb.REPLICATION_STATE_NORMAL {
		attrs.ReplicationConfig.State = replConfig.GetState()
	}
	return attrs
}

func wrapPeerError(targetCell string, err error) error {
	if _, ok := err.(*serviceerror.Unavailable); ok {
		return fmt.Errorf("peer %s unavailable: %w", targetCell, err)
	}
	return fmt.Errorf("peer %s apply failed: %w", targetCell, err)
}
