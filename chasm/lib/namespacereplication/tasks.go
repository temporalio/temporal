package namespacereplication

import (
	"context"
	"errors"
	"fmt"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/softassert"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

// -----------------------------------------------------------------------------
// ApplyLocalTask — strict CAS write to the host cell's metadata store.
// -----------------------------------------------------------------------------

type applyLocalTaskHandlerOptions struct {
	fx.In

	MetadataManager persistence.MetadataManager
	ClusterMetadata cluster.Metadata
	Config          *config
	Logger          log.Logger
}

type applyLocalTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*namespacereplicationpb.ApplyLocalTask]

	metadataManager persistence.MetadataManager
	currentCluster  string
	config          *config
	logger          log.Logger
}

type localApplyResolution int

const (
	localApplyResolutionUnresolved localApplyResolution = iota
	localApplyResolutionCommitted
	localApplyResolutionFailed
	localApplyResolutionIndeterminate
)

type localMutationPersistenceState int

const (
	localMutationPersistenceStateAbsent localMutationPersistenceState = iota
	localMutationPersistenceStateMatching
	localMutationPersistenceStateDifferent
)

func newApplyLocalTaskHandler(opts applyLocalTaskHandlerOptions) *applyLocalTaskHandler {
	return &applyLocalTaskHandler{
		metadataManager: opts.MetadataManager,
		currentCluster:  opts.ClusterMetadata.GetCurrentClusterName(),
		config:          opts.Config,
		logger:          opts.Logger,
	}
}

// Validate gates execution: only run if the component is still RUNNING, the
// local apply is pending, and the persisted retry generation matches the task.
func (h *applyLocalTaskHandler) Validate(
	_ chasm.Context,
	c *NamespaceMutationComponent,
	_ chasm.TaskInvocation,
	task *namespacereplicationpb.ApplyLocalTask,
) (bool, error) {
	if c.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_RUNNING {
		return false, nil
	}
	if c.GetLocalApply().GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING {
		return false, nil
	}
	return c.GetLocalApply().GetAttemptCount() == task.GetAttempt(), nil
}

// Execute writes to the local metadata store with strict version-CAS, then
// transitions the component to a terminal local outcome. Ambiguous write
// results are reconciled against the current namespace state and retried with a
// durable timer. A newer namespace version fences an UPDATE whose exact outcome
// can no longer be determined; later mutations converge current peers, while a
// peer removed only by the fenced mutation may require operator repair.
func (h *applyLocalTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *namespacereplicationpb.ApplyLocalTask,
) error {
	// Read the mutation payload from component state.
	type loadResult struct {
		Operation         enumsspb.NamespaceOperation
		Detail            *persistencespb.NamespaceDetail
		ExpectedVer       int64
		IsGlobalNamespace bool
	}
	loaded, err := chasm.ReadComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (loadResult, error) {
			m := c.GetMutation()
			return loadResult{
				Operation:         m.GetOperation(),
				Detail:            common.CloneProto(m.GetNamespaceDetail()),
				ExpectedVer:       m.GetExpectedVersion(),
				IsGlobalNamespace: m.GetIsGlobalNamespace(),
			}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read chasm component details: %w", err)
	}
	if !loaded.IsGlobalNamespace {
		return h.recordLocalFailure(
			ctx,
			ref,
			loaded.Detail.GetInfo().GetId(),
			task,
			serviceerror.NewInvalidArgument("CHASM namespace replication only supports global namespaces"),
		)
	}

	// The metadata write and component transition cannot share a transaction, so
	// reconcile ambiguous write results before deciding whether to fail or commit
	// the component.
	var applyErr error
	switch loaded.Operation {
	case enumsspb.NAMESPACE_OPERATION_CREATE:
		_, applyErr = h.metadataManager.CreateNamespace(ctx, &persistence.CreateNamespaceRequest{
			Namespace:         loaded.Detail,
			IsGlobalNamespace: loaded.IsGlobalNamespace,
		})
	case enumsspb.NAMESPACE_OPERATION_UPDATE:
		applyErr = h.metadataManager.UpdateNamespace(ctx, &persistence.UpdateNamespaceRequest{
			Namespace:           loaded.Detail,
			IsGlobalNamespace:   loaded.IsGlobalNamespace,
			NotificationVersion: loaded.ExpectedVer,
		})
	default:
		return h.recordLocalFailure(
			ctx,
			ref,
			loaded.Detail.GetInfo().GetId(),
			task,
			fmt.Errorf("unsupported namespace operation: %v", loaded.Operation),
		)
	}
	if applyErr != nil {
		var resolution localApplyResolution
		var reconcileErr error
		switch loaded.Operation {
		case enumsspb.NAMESPACE_OPERATION_CREATE:
			resolution, reconcileErr = h.reconcileCreateLocalApply(ctx, loaded.Detail, applyErr)
		case enumsspb.NAMESPACE_OPERATION_UPDATE:
			resolution, reconcileErr = h.reconcileUpdateLocalApply(
				ctx,
				loaded.Detail,
				loaded.ExpectedVer,
				ref.BusinessID,
				applyErr,
			)
		default:
			return h.recordLocalFailure(
				ctx,
				ref,
				loaded.Detail.GetInfo().GetId(),
				task,
				fmt.Errorf("unsupported namespace operation: %v", loaded.Operation),
			)
		}
		if reconcileErr != nil {
			// Persist the retry before acknowledging this task so queue-local retry
			// limits cannot strand the component in PENDING.
			return h.recordLocalRetry(
				ctx,
				ref,
				loaded.Detail.GetInfo().GetId(),
				task,
				fmt.Errorf("reconcile local namespace mutation after %v: %w", applyErr, reconcileErr),
			)
		}
		switch resolution {
		case localApplyResolutionCommitted:
			h.logger.Info(
				"namespacereplication recovered committed local apply",
				tag.WorkflowNamespaceID(loaded.Detail.GetInfo().GetId()),
				tag.NewStringTag("business_id", ref.BusinessID),
			)
			return h.commitLocal(ctx, ref, task)
		case localApplyResolutionUnresolved:
			// A read immediately after a timed-out write can still see the old
			// value even when that write later commits.
			return h.recordLocalRetry(
				ctx,
				ref,
				loaded.Detail.GetInfo().GetId(),
				task,
				fmt.Errorf("local namespace mutation outcome remains unresolved: %w", applyErr),
			)
		case localApplyResolutionFailed:
			return h.recordLocalFailure(ctx, ref, loaded.Detail.GetInfo().GetId(), task, applyErr)
		case localApplyResolutionIndeterminate:
			return h.recordLocalIndeterminate(
				ctx,
				ref,
				loaded.Detail.GetInfo().GetId(),
				task,
				fmt.Errorf("local namespace mutation outcome was fenced by a newer version: %w", applyErr),
			)
		default:
			return fmt.Errorf("unknown local namespace mutation resolution: %v", resolution)
		}
	}

	// Commit transition: record success and schedule peer fan-out. When there are
	// no peers (single-cluster global namespace) allPeersTerminal() is already true,
	// so complete the component in the same update. This is a separate transition
	// because the framework rewrites the component status to each transition's
	// destination after Apply returns (TransitionLocalCommitted's is RUNNING), so a
	// COMPLETED set inside it would be clobbered — the same reason peer completion
	// needs its own transition.
	return h.commitLocal(ctx, ref, task)
}

// reconcileCreateLocalApply resolves a failed CREATE attempt by comparing the
// current namespace identity with the request. Finding a global namespace by
// the requested ID proves that an earlier attempt committed, even if a later
// UPDATE changed its mutable state or name. A name lookup that resolves to a
// different ID proves a collision. If both identity lookups are absent, an
// earlier timed-out attempt can still commit, so the durable task must retry.
func (h *applyLocalTaskHandler) reconcileCreateLocalApply(
	ctx context.Context,
	detail *persistencespb.NamespaceDetail,
	applyErr error,
) (localApplyResolution, error) {
	errType := classifyLocalErr(applyErr)
	if errType != localFailureUnavailable && errType != localFailureAlreadyExists {
		return localApplyResolutionFailed, nil
	}

	persistedState, err := h.readCreateLocalMutationPersistenceState(ctx, detail)
	if err != nil {
		return localApplyResolutionUnresolved, err
	}
	switch persistedState {
	case localMutationPersistenceStateMatching:
		return localApplyResolutionCommitted, nil
	case localMutationPersistenceStateDifferent:
		// A different ID or name means the original CREATE snapshot is no longer
		// current. Fail rather than fan out stale state.
		return localApplyResolutionFailed, nil
	case localMutationPersistenceStateAbsent:
		// The current attempt may have definitely failed, but an earlier timed-out
		// attempt can still commit. Absence alone cannot resolve the mutation.
		return localApplyResolutionUnresolved, nil
	default:
		return localApplyResolutionUnresolved, fmt.Errorf(
			"unknown local namespace persistence state: %v",
			persistedState,
		)
	}
}

// reconcileUpdateLocalApply resolves a failed UPDATE attempt by reading the
// cell-global notification counter before the namespace row. If the counter has
// not passed this mutation's CAS version, an in-flight write may still commit
// and the result remains unresolved. Once the counter has passed that version,
// the namespace row determines whether this mutation committed or was fenced.
func (h *applyLocalTaskHandler) reconcileUpdateLocalApply(
	ctx context.Context,
	detail *persistencespb.NamespaceDetail,
	expectedVersion int64,
	businessID string,
	applyErr error,
) (localApplyResolution, error) {
	if classifyLocalErr(applyErr) != localFailureUnavailable {
		return localApplyResolutionFailed, nil
	}

	// Read the counter first. If an in-flight write commits after this read, the
	// counter observed here remains at expectedVersion and the result stays
	// unresolved instead of being misclassified from a later namespace-row read.
	metadata, err := h.metadataManager.GetMetadata(ctx)
	if err != nil {
		return localApplyResolutionUnresolved, err
	}
	if metadata.NotificationVersion <= expectedVersion {
		return localApplyResolutionUnresolved, nil
	}

	namespaceID := detail.GetInfo().GetId()
	if namespaceID == "" {
		return localApplyResolutionFailed, nil
	}

	response, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: namespaceID})
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			// Once the counter has passed expectedVersion, retrying can no longer
			// win the CAS. A deletion may make the namespace absent from this
			// ID-based lookup and cannot be represented by this UPDATE snapshot.
			return localApplyResolutionFailed, nil
		}
		return localApplyResolutionUnresolved, err
	}
	if !response.IsGlobalNamespace || response.Namespace.GetInfo().GetId() != namespaceID {
		return localApplyResolutionFailed, nil
	}

	observedVersion := response.NotificationVersion
	if observedVersion > expectedVersion {
		// A newer row can mean either this mutation lost its CAS or an ambiguous
		// earlier attempt committed and was subsequently overwritten. The available
		// state cannot distinguish those histories. Later mutations converge current
		// peers, but a peer removed only by this mutation may require operator repair.
		h.logger.Warn(
			"namespacereplication local update outcome fenced by newer namespace version",
			tag.WorkflowNamespaceID(namespaceID),
			tag.NewStringTag("business_id", businessID),
			tag.NewStringTag("resolution", "fenced_ambiguous"),
			tag.NewInt64("expected_notification_version", expectedVersion),
			tag.NewInt64("namespace_notification_version", observedVersion),
			tag.NewInt64("metadata_notification_version", metadata.NotificationVersion),
		)
		return localApplyResolutionIndeterminate, nil
	}
	if observedVersion < expectedVersion {
		return localApplyResolutionFailed, nil
	}
	if namespaceDetailsEqualAfterPersistenceRead(detail, response.Namespace, h.currentCluster) {
		return localApplyResolutionCommitted, nil
	}
	return localApplyResolutionFailed, nil
}

// Classify the current namespace identity for a CREATE mutation. Finding a
// global namespace by the requested ID proves that the create committed even if
// a later update renamed it. A fallback lookup by name must still match the ID.
func (h *applyLocalTaskHandler) readCreateLocalMutationPersistenceState(
	ctx context.Context,
	detail *persistencespb.NamespaceDetail,
) (localMutationPersistenceState, error) {
	namespaceID := detail.GetInfo().GetId()
	if namespaceID == "" {
		return localMutationPersistenceStateAbsent, nil
	}

	foundByID := true
	response, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: namespaceID})
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); !ok {
			return localMutationPersistenceStateAbsent, err
		}
		if detail.GetInfo().GetName() == "" {
			return localMutationPersistenceStateAbsent, nil
		}
		foundByID = false
		response, err = h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			Name: detail.GetInfo().GetName(),
		})
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			return localMutationPersistenceStateAbsent, nil
		}
		if err != nil {
			return localMutationPersistenceStateAbsent, err
		}
	}

	// The CHASM transport is global-only. IsGlobalNamespace is not part of
	// NamespaceDetail, so check it separately.
	if !response.IsGlobalNamespace {
		return localMutationPersistenceStateDifferent, nil
	}
	if response.Namespace.GetInfo().GetId() == namespaceID &&
		(foundByID || response.Namespace.GetInfo().GetName() == detail.GetInfo().GetName()) {
		return localMutationPersistenceStateMatching, nil
	}
	return localMutationPersistenceStateDifferent, nil
}

// namespaceDetailsEqualAfterPersistenceRead compares a mutation payload with a
// MetadataManager read. MetadataManager materializes persistence defaults that
// may have been absent from the payload written by an earlier attempt; those
// represent the same namespace state and must not turn an ambiguous success into
// a terminal failure during reconciliation.
func namespaceDetailsEqualAfterPersistenceRead(
	expected *persistencespb.NamespaceDetail,
	actual *persistencespb.NamespaceDetail,
	currentCluster string,
) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	expected = common.CloneProto(expected)
	actual = common.CloneProto(actual)
	normalizeNamespaceDetailAfterPersistenceRead(expected, currentCluster)
	normalizeNamespaceDetailAfterPersistenceRead(actual, currentCluster)
	return proto.Equal(expected, actual)
}

// Keep this in sync with the defaults materialized by
// persistence.ConvertInternalGetNamespaceResponse.
func normalizeNamespaceDetailAfterPersistenceRead(
	detail *persistencespb.NamespaceDetail,
	currentCluster string,
) {
	if detail.Info != nil && detail.Info.Data == nil {
		detail.Info.Data = map[string]string{}
	}
	if detail.Config != nil &&
		(detail.Config.BadBinaries == nil || detail.Config.BadBinaries.Binaries == nil) {
		detail.Config.BadBinaries = &namespacepb.BadBinaries{
			Binaries: map[string]*namespacepb.BadBinaryInfo{},
		}
	}
	if detail.ReplicationConfig != nil {
		detail.ReplicationConfig.ActiveClusterName = persistence.GetOrUseDefaultActiveCluster(
			currentCluster,
			detail.ReplicationConfig.ActiveClusterName,
		)
		detail.ReplicationConfig.Clusters = persistence.GetOrUseDefaultClusters(
			currentCluster,
			detail.ReplicationConfig.Clusters,
		)
	}
}

func (h *applyLocalTaskHandler) commitLocal(
	ctx context.Context,
	ref chasm.ComponentRef,
	task *namespacereplicationpb.ApplyLocalTask,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			if !localTaskIsCurrent(c, task) {
				return nil, nil
			}
			if err := TransitionLocalCommitted.Apply(c, mctx, EventLocalCommitted{
				Time: mctx.Now(c),
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

func (h *applyLocalTaskHandler) recordLocalRetry(
	ctx context.Context,
	ref chasm.ComponentRef,
	namespaceID string,
	task *namespacereplicationpb.ApplyLocalTask,
	retryErr error,
) error {
	expiration := h.config.localRetryExpiration()
	h.logger.Warn(
		"namespacereplication local apply remains unresolved",
		tag.WorkflowNamespaceID(namespaceID),
		tag.NewStringTag("business_id", ref.BusinessID),
		tag.NewInt32("attempt", task.GetAttempt()),
		tag.Error(retryErr),
	)
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			if !localTaskIsCurrent(c, task) {
				return nil, nil
			}

			now := mctx.Now(c)
			completedAttempts := task.GetAttempt() + 1
			firstAt := now
			if c.GetLocalApply().GetFirstAttemptAt() != nil {
				firstAt = c.GetLocalApply().GetFirstAttemptAt().AsTime()
			}
			deadline := firstAt.Add(expiration)
			if !now.Before(deadline) {
				return nil, TransitionLocalIndeterminate.Apply(c, mctx, EventLocalIndeterminate{
					Time:     now,
					Attempts: completedAttempts,
					Err: fmt.Errorf(
						"local namespace mutation outcome remained unresolved after %v: %w",
						expiration,
						retryErr,
					),
				})
			}

			delay := min(localRetryBackoff(completedAttempts), deadline.Sub(now))
			return nil, TransitionLocalRetry.Apply(c, mctx, EventLocalRetry{
				Time:     now,
				Attempts: completedAttempts,
				Delay:    delay,
				Err:      retryErr,
			})
		},
		nil,
	)
	return err
}

func (h *applyLocalTaskHandler) recordLocalFailure(
	ctx context.Context,
	ref chasm.ComponentRef,
	namespaceID string,
	task *namespacereplicationpb.ApplyLocalTask,
	applyErr error,
) error {
	errType := classifyLocalErr(applyErr)
	h.logger.Warn("namespacereplication local apply failed",
		tag.WorkflowNamespaceID(namespaceID),
		tag.NewStringTag("business_id", ref.BusinessID),
		tag.NewStringTag("error_type", errType),
		tag.Error(applyErr),
	)
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			if !localTaskIsCurrent(c, task) {
				return nil, nil
			}
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

func (h *applyLocalTaskHandler) recordLocalIndeterminate(
	ctx context.Context,
	ref chasm.ComponentRef,
	namespaceID string,
	task *namespacereplicationpb.ApplyLocalTask,
	applyErr error,
) error {
	h.logger.Warn(
		"namespacereplication local apply outcome is indeterminate",
		tag.WorkflowNamespaceID(namespaceID),
		tag.NewStringTag("business_id", ref.BusinessID),
		tag.Error(applyErr),
	)
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			if !localTaskIsCurrent(c, task) {
				return nil, nil
			}
			return nil, TransitionLocalIndeterminate.Apply(c, mctx, EventLocalIndeterminate{
				Time:     mctx.Now(c),
				Attempts: task.GetAttempt() + 1,
				Err:      applyErr,
			})
		},
		nil,
	)
	return err
}

func localTaskIsCurrent(
	c *NamespaceMutationComponent,
	task *namespacereplicationpb.ApplyLocalTask,
) bool {
	return c.GetStatus() == namespacereplicationpb.COMPONENT_STATUS_RUNNING &&
		c.GetLocalApply().GetOutcome() == namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING &&
		c.GetLocalApply().GetAttemptCount() == task.GetAttempt()
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
	// localFailureInternal: degenerate cases such as an unsupported operation.
	// Terminal.
	localFailureInternal = "Internal"
)

// classifyLocalErr maps a local-apply error to the caller-facing gRPC error
// class, mirroring legacy metadataMgr semantics: a CAS conflict or transient
// store failure is Unavailable (retriable), an invalid argument is terminal, a
// CreateNamespace collision is AlreadyExists (terminal, matching legacy
// RegisterNamespace), and anything else is a degenerate Internal failure.
// Symmetric with classifyPeerErr on the peer path.
func classifyLocalErr(err error) string {
	if _, ok := errors.AsType[*serviceerror.Unavailable](err); ok {
		return localFailureUnavailable
	}
	if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
		return localFailureUnavailable
	}
	if _, ok := errors.AsType[*serviceerror.DeadlineExceeded](err); ok {
		return localFailureUnavailable
	}
	if _, ok := errors.AsType[*persistence.ConditionFailedError](err); ok {
		return localFailureUnavailable
	}
	if _, ok := errors.AsType[*persistence.TimeoutError](err); ok {
		return localFailureUnavailable
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return localFailureUnavailable
	}
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return localFailureInvalidArgument
	}
	if _, ok := errors.AsType[*serviceerror.NamespaceAlreadyExists](err); ok {
		return localFailureAlreadyExists
	}
	return localFailureInternal
}

// -----------------------------------------------------------------------------
// ApplyPeerTask — fans out to a peer cell via the ApplyNamespaceMutation admin
// RPC. One task instance per peer cell, scheduled in parallel after the local
// apply commits. Apply-if-higher semantics on the receiver make retries safe.
// -----------------------------------------------------------------------------

type applyPeerTaskHandlerOptions struct {
	fx.In

	PeerApplier PeerApplier
	Config      *config
	Logger      log.Logger
}

type applyPeerTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*namespacereplicationpb.ApplyPeerTask]

	// peerApplier is the pluggable transport that delivers a mutation to a peer
	// cell. The default OSS impl uses the ApplyNamespaceMutation admin RPC; this
	// handler owns the surrounding policy (retry, error classification, per-peer
	// state, completion) independent of which transport is injected.
	peerApplier PeerApplier
	// Retriable peer failures are retried with capped exponential backoff over a
	// configurable budget by recordPeerOutcome + TransitionPeerRetry (see
	// statemachine.go), not by CHASM's default task retry.
	config *config
	logger log.Logger
}

func newApplyPeerTaskHandler(opts applyPeerTaskHandlerOptions) *applyPeerTaskHandler {
	return &applyPeerTaskHandler{
		peerApplier: opts.PeerApplier,
		config:      opts.Config,
		logger:      opts.Logger,
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
		softassert.Fail(
			h.logger,
			"peer apply task exists before local apply committed",
			tag.WorkflowNamespaceID(c.GetMutation().GetNamespaceDetail().GetInfo().GetId()),
			tag.NewStringTag("target_cell", task.GetTargetCell()),
			tag.NewStringTag("local_apply_outcome", c.GetLocalApply().GetOutcome().String()),
		)
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
				Operation: m.GetOperation(),
				Detail:    common.CloneProto(m.GetNamespaceDetail()),
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
		saveErr := h.recordPeerOutcome(
			ctx,
			ref,
			loaded.Detail.GetInfo().GetId(),
			task,
			classifyPeerErr(applyErr),
			applyErr,
		)
		if isPeerDestinationDown(applyErr) {
			// Signal the outbound queue's per-destination circuit breaker without
			// introducing a second retry path. The queue unwraps this and returns
			// saveErr: nil acknowledges this task after the CHASM backoff was saved;
			// a non-nil saveErr retries the task because its state was not saved.
			return queueserrors.NewDestinationDownError(applyErr.Error(), saveErr)
		}
		return saveErr
	}

	outcome, resultErr := peerOutcomeFromResult(result)
	if resultErr != nil {
		return h.recordPeerOutcome(
			ctx,
			ref,
			loaded.Detail.GetInfo().GetId(),
			task,
			classifyPeerErr(resultErr),
			resultErr,
		)
	}
	return h.recordPeerOutcome(ctx, ref, loaded.Detail.GetInfo().GetId(), task, outcome, nil)
}

// peerOutcomeFromResult maps a transport-neutral PeerApplyResult onto the
// persisted per-peer outcome. All three are terminal (see allPeersTerminal):
// NotAdmitted is a terminal non-failure, kept distinct from Applied so the
// component never records a peer write that didn't happen.
func peerOutcomeFromResult(result PeerApplyResult) (namespacereplicationpb.PeerApplyOutcome, error) {
	switch result {
	case PeerApplyResultApplied:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, nil
	case PeerApplyResultNoOpStale:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE, nil
	case PeerApplyResultNotAdmitted:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_NOT_ADMITTED, nil
	default:
		return namespacereplicationpb.PEER_APPLY_OUTCOME_UNSPECIFIED,
			serviceerror.NewInternal(fmt.Sprintf("unknown peer apply result: %d", result))
	}
}

// recordPeerOutcome writes the outcome of a peer apply to the component state.
// Called for both success and failure paths.
func (h *applyPeerTaskHandler) recordPeerOutcome(
	ctx context.Context,
	ref chasm.ComponentRef,
	namespaceID string,
	task *namespacereplicationpb.ApplyPeerTask,
	outcome namespacereplicationpb.PeerApplyOutcome,
	execErr error,
) error {
	retryPolicy := h.config.peerRetryPolicy()
	if execErr != nil {
		h.logger.Warn("namespacereplication peer apply failed",
			tag.WorkflowNamespaceID(namespaceID),
			tag.NewStringTag("business_id", ref.BusinessID),
			tag.NewStringTag("target_cell", task.GetTargetCell()),
			tag.NewInt32("attempt", task.GetAttempt()),
			tag.Error(execErr),
		)
	}
	_, _, updErr := chasm.UpdateComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			peer := c.GetPeerApply()[task.GetTargetCell()]
			if c.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_RUNNING ||
				c.GetLocalApply().GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED ||
				peer.GetOutcome() != namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING ||
				peer.GetAttemptCount() != task.GetAttempt() {
				return nil, nil
			}

			now := mctx.Now(c)
			completedAttempts := task.GetAttempt() + 1

			// Retriable failure: wait in FAILED_RETRIABLE and reschedule with capped
			// exponential backoff until the total retry budget (measured from the
			// first failed attempt) is exhausted. This lets the mutation converge
			// once a temporarily-unreachable peer recovers. Only after the budget
			// is spent do we give up as FAILED_TERMINAL so the component can still
			// complete.
			if outcome == namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE {
				firstAt := now
				if peer.GetFirstAttemptAt() != nil {
					firstAt = peer.GetFirstAttemptAt().AsTime()
				}
				if now.Sub(firstAt) < retryPolicy.expirationInterval {
					return nil, TransitionPeerRetry.Apply(c, mctx, EventPeerRetry{
						Time:       now,
						TargetCell: task.GetTargetCell(),
						Attempts:   completedAttempts,
						Delay:      peerRetryBackoff(completedAttempts, retryPolicy),
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
				Attempts:   completedAttempts,
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
	if _, ok := errors.AsType[*serviceerror.Unavailable](err); ok {
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	}
	if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	}
	if _, ok := errors.AsType[*serviceerror.DeadlineExceeded](err); ok {
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	}
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
	}
	if _, ok := errors.AsType[*serviceerror.Unimplemented](err); ok {
		return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
	}
	return namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
}

func isPeerDestinationDown(err error) bool {
	if _, ok := errors.AsType[*serviceerror.Unavailable](err); ok {
		return true
	}
	if _, ok := errors.AsType[*serviceerror.DeadlineExceeded](err); ok {
		return true
	}
	if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
		return false
	}
	if _, ok := errors.AsType[serviceerror.ServiceError](err); ok {
		return false
	}
	return true
}

// -----------------------------------------------------------------------------
// ApplyPeerBackoffTask — waits outside the outbound queue, then re-enqueues the
// destination-tagged peer task for the next attempt.
// -----------------------------------------------------------------------------

type applyPeerBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func newApplyPeerBackoffTaskHandler() *applyPeerBackoffTaskHandler {
	return &applyPeerBackoffTaskHandler{}
}

func (h *applyPeerBackoffTaskHandler) Validate(
	_ chasm.Context,
	c *NamespaceMutationComponent,
	_ chasm.TaskInvocation,
	task *namespacereplicationpb.ApplyPeerBackoffTask,
) (bool, error) {
	if c.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_RUNNING {
		return false, nil
	}
	if c.GetLocalApply().GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED {
		return false, nil
	}
	peer := c.GetPeerApply()[task.GetTargetCell()]
	if peer == nil || peer.GetOutcome() != namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE {
		return false, nil
	}
	return peer.GetAttemptCount() == task.GetAttempt(), nil
}

func (h *applyPeerBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	c *NamespaceMutationComponent,
	_ chasm.TaskAttributes,
	task *namespacereplicationpb.ApplyPeerBackoffTask,
) error {
	return TransitionPeerRetryReady.Apply(c, ctx, EventPeerRetryReady{
		TargetCell: task.GetTargetCell(),
		Attempt:    task.GetAttempt(),
	})
}
