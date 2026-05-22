package migration

import (
	"context"
	"fmt"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc/metadata"
)

type (
	// adaptiveInjectBatchRequest drives the inject phase of one batch: emit
	// replication tasks for every execution at the configured RPS. The
	// workflow holds its per-lane slot through inject+verify so adjacent
	// batches on the same lane serialize.
	adaptiveInjectBatchRequest struct {
		Namespace      string
		NamespaceID    string
		TargetClusters []string

		Executions []*ExecutionInfo
		RPS        float64
	}

	// adaptiveVerifyBatchRequest drives the skip-ahead verify phase. The
	// wall-budget DeadlineMs is not a failure — whatever doesn't verify by
	// then is returned as Pending and re-queued by the workflow.
	adaptiveVerifyBatchRequest struct {
		Namespace             string
		NamespaceID           string
		TargetClusterEndpoint string
		TargetClusterName     string

		Executions       []*ExecutionInfo
		DeadlineMs       int64
		VerifyIntervalMs int64
	}

	adaptiveVerifyBatchResponse struct {
		Verified int64
		Pending  []*ExecutionInfo
	}
)

// InjectBatch emits replication tasks for every execution at the configured
// per-batch RPS. Inject and verify are split into separate activities so
// the workflow has per-phase visibility and the AIMD controller can see
// which phase contributes to outcomes.
func (a *activities) InjectBatch(ctx context.Context, request *adaptiveInjectBatchRequest) error {
	ctx = a.setCallerInfoForServerAPI(ctx, namespace.ID(request.NamespaceID))
	start := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.Timer(metrics.GenerateReplicationTasksLatency.Name()).Record(time.Since(start))
	}()

	rps := request.RPS
	if rps <= 0 {
		rps = 1
	}
	rateLimiter := quotas.NewRateLimiter(rps, int(math.Ceil(rps)))

	namespaceName, err := a.NamespaceRegistry.GetNamespaceName(namespace.ID(request.NamespaceID))
	if err != nil {
		return err
	}

	generateViaFrontend := a.generateMigrationTaskViaFrontend()
	startIndex := 0
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &startIndex); err == nil {
			startIndex = startIndex + 1
		}
	}
	for i := startIndex; i < len(request.Executions); i++ {
		we := request.Executions[i]
		if err := a.generateWorkflowReplicationTask(
			ctx,
			rateLimiter,
			namespaceName.String(),
			request.NamespaceID,
			we,
			request.TargetClusters,
			generateViaFrontend,
		); err != nil {
			// Tolerate NotFound: a deleted workflow won't land on the
			// target and verify will skip it via checkSkipWorkflowExecution.
			if !common.IsNotFoundError(err) {
				return fmt.Errorf("generate replication task for %s/%s: %w", we.BusinessID, we.RunID, err)
			}
			a.Logger.Warn("force-replication ignore replication task due to NotFoundServiceError",
				tag.WorkflowNamespaceID(request.NamespaceID),
				tag.WorkflowID(we.BusinessID),
				tag.WorkflowRunID(we.RunID),
				tag.Error(err))
		}
		activity.RecordHeartbeat(ctx, i)
	}
	return nil
}

// VerifyBatch skip-ahead verifies each execution against the target
// cluster, returning everything that didn't verify by DeadlineMs as
// Pending. Used for both fresh batches (chained after InjectBatch) and
// retry rounds (re-dispatched alone with the already-injected pending list).
func (a *activities) VerifyBatch(ctx context.Context, request *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
	ctx = a.setCallerInfoForServerAPI(ctx, namespace.ID(request.NamespaceID))
	start := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.Timer(metrics.VerifyReplicationTasksLatency.Name()).Record(time.Since(start))
	}()
	return a.skipAheadVerify(ctx, request)
}

// skipAheadVerify scans all unverified executions on every pass — a hot
// WF ID at the head of the batch can't starve verification of the rest.
// No in-activity no-progress timer; the wall-budget deadline returns
// pending to the workflow, which is the failure-detection authority.
//
// Per-execution outcomes resolve to one of three terminal states:
//   - verified (DescribeMutableState OK, workflowVerifier OK)
//   - skipped (NotFound + zombie / past-retention; counts as verified
//     for completion purposes)
//   - pending (BUSY_WORKFLOW, NotFound + not skippable, or any retryable error)
func (a *activities) skipAheadVerify(ctx context.Context, request *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(interceptor.DCRedirectionContextHeaderName, "false"))

	interval := time.Duration(request.VerifyIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	deadline := time.Duration(request.DeadlineMs) * time.Millisecond
	if deadline <= 0 {
		deadline = 30 * time.Second
	}

	remoteAdminClient, err := a.clientBean.GetRemoteAdminClient(request.TargetClusterName)
	if err != nil {
		return nil, err
	}
	nsEntry, err := a.NamespaceRegistry.GetNamespace(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}

	// Reuse the per-execution request shape; only the surrounding loop
	// shape differs between skip-ahead and the in-order verifier.
	singleReq := &verifyReplicationTasksRequest{
		Namespace:             request.Namespace,
		NamespaceID:           request.NamespaceID,
		TargetClusterEndpoint: request.TargetClusterEndpoint,
		TargetClusterName:     request.TargetClusterName,
	}

	verified := make([]bool, len(request.Executions))
	var verifiedCount int64

	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		// Replication has a lag; sleep before scanning so the first pass
		// isn't all not-found.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}

		for i, we := range request.Executions {
			if verified[i] {
				continue
			}
			// Heartbeat per exec so a worker crash is detected within
			// HeartbeatTimeout rather than StartToCloseTimeout. The SDK
			// batches the actual network calls, so the cost is bounded
			// even with large batches.
			activity.RecordHeartbeat(ctx, nil)
			isVerified, err := a.skipAheadVerifySingle(ctx, singleReq, remoteAdminClient, nsEntry, we)
			if err != nil {
				return nil, err
			}
			if isVerified {
				verified[i] = true
				verifiedCount++
			}
		}
		if int(verifiedCount) >= len(request.Executions) {
			break
		}
	}

	pending := make([]*ExecutionInfo, 0)
	for i, we := range request.Executions {
		if !verified[i] {
			pending = append(pending, we)
		}
	}
	return &adaptiveVerifyBatchResponse{
		Verified: verifiedCount,
		Pending:  pending,
	}, nil
}

// skipAheadVerifySingle returns true when verification is terminal
// (verified or skipped); false when the execution should be retried.
func (a *activities) skipAheadVerifySingle(
	ctx context.Context,
	request *verifyReplicationTasksRequest,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	execution *ExecutionInfo,
) (bool, error) {
	s := time.Now()
	archetype, err := a.archetypeIDToName(ctx, execution.ArchetypeID)
	if err != nil {
		return false, err
	}
	mu, err := remoteAdminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: request.Namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution.BusinessID,
			RunId:      execution.RunID,
		},
		Archetype:       archetype,
		ArchetypeId:     execution.ArchetypeID,
		SkipForceReload: true,
	})
	a.forceReplicationMetricsHandler.Timer(metrics.VerifyDescribeMutableStateLatency.Name()).Record(time.Since(s))

	switch e := err.(type) {
	case nil:
		result, verr := a.workflowVerifier(ctx, request, remoteAdminClient, a.adminClient, ns, execution, mu)
		if verr != nil {
			return false, verr
		}
		// Only `verified` increments the success counter; `skipped`
		// (zombie / past-retention) terminates verification but isn't
		// counted as a confirmed replication.
		if result.status == verified {
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskSuccess.Name()).Record(1)
		}
		if result.status == verified || result.status == skipped {
			return true, nil
		}
		return false, nil

	case *serviceerror.NotFound:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskNotFound.Name()).Record(1)
		// checkSkipWorkflowExecution returns verified=skipped for zombie /
		// past-retention executions (which are legitimately absent on the
		// target). Treat that as a terminal verify; anything else stays
		// pending.
		result, serr := a.checkSkipWorkflowExecution(ctx, request, execution, ns)
		if serr != nil {
			return false, serr
		}
		if result.status == verified || result.status == skipped {
			return true, nil
		}
		return false, nil

	case *serviceerror.NamespaceNotFound:
		return false, temporal.NewNonRetryableApplicationError("failed to describe workflow from the remote cluster", "NamespaceNotFound", err)

	case *serviceerror.ResourceExhausted:
		if e.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
			// Apply is mid-flight on the passive — a small sign of progress
			// but not yet verified. Pending until the next scan.
			return false, nil
		}
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace), metrics.ServiceErrorTypeTag(err)).Counter(metrics.VerifyReplicationTaskFailed.Name()).Record(1)
		return false, fmt.Errorf("failed to describe workflow from the remote cluster: %w", err)

	default:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace), metrics.ServiceErrorTypeTag(err)).Counter(metrics.VerifyReplicationTaskFailed.Name()).Record(1)
		return false, fmt.Errorf("failed to describe workflow from the remote cluster: %w", err)
	}
}
