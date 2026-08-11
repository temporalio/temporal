package namespacereplication

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
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
	chasm.SideEffectTaskHandlerBase[*namespacereplicationpb.ApplyLocalTask]

	metadataManager persistence.MetadataManager
	// TODO(namespacereplication): emit metrics for the local apply path. Suggested shape:
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
	_ chasm.TaskInvocation,
	_ *namespacereplicationpb.ApplyLocalTask,
) (bool, error) {
	if c.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_RUNNING {
		return false, nil
	}
	if c.GetLocalApply().GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING {
		return false, nil
	}
	return true, nil
}

// Execute writes to the local metadata store with strict version-CAS, then
// transitions the component to COMMITTED (and schedules peer fan-out) or
// FAILED. CAS conflicts are treated as terminal — the caller is expected to
// re-issue the mutation with fresh state (matching legacy behavior). Retrying
// internally with the same mutation payload but a refreshed NotificationVersion
// is unsafe for general UpdateNamespace, because a loser could overwrite a
// winner's fields on unrelated attributes.
func (h *applyLocalTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *namespacereplicationpb.ApplyLocalTask,
) error {
	// Read the mutation payload from component state.
	type loadResult struct {
		Operation   namespacereplicationpb.NamespaceOperation
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

	// Apply to the local metadata store. Any error (CAS conflict, validation,
	// store unavailable) is surfaced as a terminal component failure; the caller
	// retries by re-issuing UpdateNamespace with fresh state.
	switch loaded.Operation {
	case namespacereplicationpb.NAMESPACE_OPERATION_CREATE:
		if _, applyErr := h.metadataManager.CreateNamespace(ctx, &persistence.CreateNamespaceRequest{
			Namespace:         loaded.Detail,
			IsGlobalNamespace: loaded.IsGlobal,
		}); applyErr != nil {
			return h.recordLocalFailure(ctx, ref, applyErr)
		}
	case namespacereplicationpb.NAMESPACE_OPERATION_UPDATE:
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

	// Read back the post-write notification_version. The CAS write doesn't return
	// the new version (Create returns ID; Update returns only error), so we query
	// after to get the truth. Used as the component's NewVersion in the gRPC
	// response.
	meta, metaErr := h.metadataManager.GetMetadata(ctx)
	if metaErr != nil {
		// Rare degenerate case: we committed locally but can't read back the new
		// version. Treat as terminal so the caller sees the error rather than a
		// misleading zero version.
		return h.recordLocalFailure(ctx, ref, fmt.Errorf("read post-write notification_version: %w", metaErr))
	}
	newVersion := meta.NotificationVersion

	// Commit transition: record success and schedule peer fan-out. When there are
	// no peers (single-cluster global namespace) allPeersTerminal() is already true,
	// so complete the component in the same update. This is a separate transition
	// because the framework rewrites the component status to each transition's
	// destination after Apply returns (TransitionLocalCommitted's is RUNNING), so a
	// COMPLETED set inside it would be clobbered — the same reason peer completion
	// needs its own transition.
	_, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			if err := TransitionLocalCommitted.Apply(c, mctx, EventLocalCommitted{
				Time:       mctx.Now(c),
				NewVersion: newVersion,
			}); err != nil {
				return nil, err
			}
			if c.allPeersTerminal() {
				return nil, TransitionAllPeersTerminal.Apply(c, mctx, EventAllPeersTerminal{})
			}
			return nil, nil
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
	errType := classifyLocalErr(applyErr)
	h.logger.Warn("namespacereplication local apply failed",
		tag.NewStringTag("namespace_id", ref.BusinessID),
		tag.NewStringTag("error_type", errType),
		tag.Error(applyErr),
	)
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			return nil, TransitionLocalFailed.Apply(c, mctx, EventLocalFailed{
				Time:    mctx.Now(c),
				Err:     applyErr,
				ErrType: errType,
			})
		},
		nil,
	)
	return err
}

// Local-apply failure classes. These are carried in the persisted failure's
// ApplicationFailureInfo.Type (see TransitionLocalFailed) and mapped back to a
// caller-facing gRPC error in the TriggerNamespaceMutation poll predicate (see
// localApplyError). Kept as plain strings rather than a proto enum: this is a
// purely internal producer/consumer discriminator within the namespacereplication package,
// not part of any wire contract.
const (
	// localFailureUnavailable: CAS conflict or transient store failure. Retriable
	// — the frontend's retry interceptor and SDK/activity retry re-issue the
	// mutation with fresh state, matching legacy metadataMgr.UpdateNamespace.
	localFailureUnavailable = "Unavailable"
	// localFailureInvalidArgument: bad request / validation. Terminal.
	localFailureInvalidArgument = "InvalidArgument"
	// localFailureAlreadyExists: CreateNamespace hit an existing namespace (name
	// or id collision). Terminal — retrying a create never succeeds. Mapped back
	// to serviceerror.NamespaceAlreadyExists so the caller sees the same error
	// class legacy RegisterNamespace returned from metadataMgr.CreateNamespace.
	localFailureAlreadyExists = "AlreadyExists"
	// localFailureInternal: degenerate cases (unsupported operation, post-write
	// read-back failure, etc.). Terminal.
	localFailureInternal = "Internal"
)

// classifyLocalErr maps a local-apply error to the caller-facing gRPC error
// class, mirroring legacy metadataMgr semantics: a CAS conflict or transient
// store failure is Unavailable (retriable), an invalid argument is terminal, a
// CreateNamespace collision is AlreadyExists (terminal, matching legacy
// RegisterNamespace), and anything else is a degenerate Internal failure.
// Symmetric with classifyPeerErr on the peer path.
//
// Uses errors.As rather than a bare type switch so the classification survives
// error wrapping: some callers (e.g. the post-write read-back path) hand this a
// fmt.Errorf("...: %w", storeErr), and a wrapped *serviceerror.Unavailable must
// still be recognized as retriable rather than falling through to Internal.
func classifyLocalErr(err error) string {
	var (
		unavailable       *serviceerror.Unavailable
		resourceExhausted *serviceerror.ResourceExhausted
		deadlineExceeded  *serviceerror.DeadlineExceeded
		invalidArgument   *serviceerror.InvalidArgument
		alreadyExists     *serviceerror.NamespaceAlreadyExists
	)
	switch {
	case errors.As(err, &unavailable),
		errors.As(err, &resourceExhausted),
		errors.As(err, &deadlineExceeded):
		return localFailureUnavailable
	case errors.As(err, &invalidArgument):
		return localFailureInvalidArgument
	case errors.As(err, &alreadyExists):
		return localFailureAlreadyExists
	default:
		return localFailureInternal
	}
}

// -----------------------------------------------------------------------------
// ApplyPeerTask — fans out to a peer cell via the ApplyNamespaceMutation admin
// RPC. One task instance per peer cell, scheduled in parallel after the local
// apply commits. Apply-if-higher semantics on the receiver make retries safe.
// -----------------------------------------------------------------------------

type applyPeerTaskHandlerOptions struct {
	fx.In

	PeerApplier    PeerApplier
	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type applyPeerTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*namespacereplicationpb.ApplyPeerTask]

	// peerApplier is the pluggable transport that delivers a mutation to a peer
	// cell. The default OSS impl uses the ApplyNamespaceMutation admin RPC; this
	// handler owns the surrounding policy (retry, error classification, per-peer
	// state, completion) independent of which transport is injected.
	peerApplier PeerApplier
	// TODO(namespacereplication): emit metrics for the peer apply path. Suggested shape:
	//   - nsrepl_apply_attempts_total{target_cell, source_cell, outcome}    counter
	//   - nsrepl_apply_failures_total{target_cell, source_cell}             counter
	//   - nsrepl_apply_duration_seconds{target_cell, source_cell}           histogram
	// metricsHandler is wired through fx but not yet used.
	//
	// Retriable peer failures are retried with capped exponential backoff over a
	// 7-day budget by recordPeerOutcome + TransitionPeerRetry (see statemachine.go),
	// not by CHASM's default task retry.
	metricsHandler metrics.Handler
	logger         log.Logger
}

func newApplyPeerTaskHandler(opts applyPeerTaskHandlerOptions) *applyPeerTaskHandler {
	return &applyPeerTaskHandler{
		peerApplier:    opts.PeerApplier,
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
	_ chasm.TaskInvocation,
	task *namespacereplicationpb.ApplyPeerTask,
) (bool, error) {
	if c.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_RUNNING {
		return false, nil
	}
	if c.GetLocalApply().GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED {
		return false, nil
	}
	peer := c.GetPeerApply()[task.GetTargetCell()]
	if peer == nil {
		return false, nil
	}
	if peer.GetOutcome() != namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING {
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
	task *namespacereplicationpb.ApplyPeerTask,
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

	// Deliver the mutation to the peer via the pluggable transport. Any error
	// (dial failure or apply failure) is classified here into retriable vs
	// terminal, so the retry/gating policy stays in this package regardless of
	// which transport the deployment injected.
	result, applyErr := h.peerApplier.Apply(ctx, task.GetTargetCell(), loaded.Operation, loaded.Detail)
	if applyErr != nil {
		return h.recordPeerOutcome(ctx, ref, task, classifyPeerErr(applyErr), applyErr)
	}

	return h.recordPeerOutcome(ctx, ref, task, peerOutcomeFromResult(result), nil)
}

// peerOutcomeFromResult maps a transport-neutral PeerApplyResult onto the
// persisted per-peer outcome. All three are terminal (see allPeersTerminal):
// NotAdmitted is a terminal non-failure, kept distinct from Applied so the
// component never records a peer write that didn't happen.
func peerOutcomeFromResult(result PeerApplyResult) namespacereplicationpb.PeerApplyOutcome {
	switch result {
	case PeerApplyResultNoOpStale:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE
	case PeerApplyResultNotAdmitted:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_NOT_ADMITTED
	default:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED
	}
}

// recordPeerOutcome writes the outcome of a peer apply to the component state.
// Called for both success and failure paths.
func (h *applyPeerTaskHandler) recordPeerOutcome(
	ctx context.Context,
	ref chasm.ComponentRef,
	task *namespacereplicationpb.ApplyPeerTask,
	outcome namespacereplicationpb.PeerApplyOutcome,
	execErr error,
) error {
	if execErr != nil {
		h.logger.Warn("namespacereplication peer apply failed",
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
			now := mctx.Now(c)
			nextAttempt := task.GetAttempt() + 1

			// Retriable failure: keep the peer PENDING and reschedule with capped
			// exponential backoff until the total retry budget (measured from the
			// first failed attempt) is exhausted. This lets the mutation converge
			// once a temporarily-unreachable peer recovers. Only after the budget
			// is spent do we give up as FAILED_TERMINAL so the component can still
			// complete.
			if outcome == namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE {
				peer := c.GetPeerApply()[task.GetTargetCell()]
				firstAt := now
				if peer.GetFirstAttemptAt() != nil {
					firstAt = peer.GetFirstAttemptAt().AsTime()
				}
				if now.Sub(firstAt) < peerRetryBudget {
					return nil, TransitionPeerRetry.Apply(c, mctx, EventPeerRetry{
						Time:       now,
						TargetCell: task.GetTargetCell(),
						Attempt:    nextAttempt,
						Err:        execErr,
					})
				}
				// Budget exhausted: fall through and record a terminal failure.
				outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
			}

			if err := TransitionPeerCompleted.Apply(c, mctx, EventPeerCompleted{
				Time:       now,
				TargetCell: task.GetTargetCell(),
				Outcome:    outcome,
				Attempts:   nextAttempt,
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
	return updErr
}

// classifyPeerErr maps an admin RPC error to the appropriate peer apply outcome.
// Transient errors (peer unavailable, resource exhausted, deadline) are
// retriable. Argument validation, not-found, and unimplemented (peer binary too
// old to serve ApplyNamespaceMutation) errors are terminal — retrying won't
// help. Unknown errors default to retriable (safer: apply-if-higher makes
// duplicate writes no-ops).
func classifyPeerErr(err error) namespacereplicationpb.PeerApplyOutcome {
	switch err.(type) {
	case *serviceerror.Unavailable,
		*serviceerror.ResourceExhausted,
		*serviceerror.DeadlineExceeded:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	case *serviceerror.InvalidArgument,
		*serviceerror.NotFound,
		*serviceerror.Unimplemented:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
	default:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	}
}

// convertOperation maps our local NamespaceOperation enum to the OSS one used in
// NamespaceTaskAttributes. Same values, different proto package.
func convertOperation(op namespacereplicationpb.NamespaceOperation) enumsspb.NamespaceOperation {
	switch op {
	case namespacereplicationpb.NAMESPACE_OPERATION_CREATE:
		return enumsspb.NAMESPACE_OPERATION_CREATE
	case namespacereplicationpb.NAMESPACE_OPERATION_UPDATE:
		return enumsspb.NAMESPACE_OPERATION_UPDATE
	default:
		return enumsspb.NAMESPACE_OPERATION_UNSPECIFIED
	}
}
