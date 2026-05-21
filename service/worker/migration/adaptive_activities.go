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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc/metadata"
)

type (
	// adaptiveInjectBatchRequest drives the inject phase of one batch: emit
	// replication tasks for every execution at the configured RPS. Returns
	// no per-execution data; the verify phase is a separate activity. The
	// workflow holds its per-lane slot through both phases so adjacent
	// batches on the same lane serialize (drip-feed for the slow lane,
	// matching V1/V2 pacing for the fast lane).
	adaptiveInjectBatchRequest struct {
		Namespace      string
		NamespaceID    string
		TargetClusters []string

		Executions       []*ExecutionInfo
		RPS              float64
		GetParentInfoRPS float64
	}

	// adaptiveVerifyBatchRequest drives the skip-ahead verify phase for a
	// previously-injected batch. The wall-budget DeadlineMs is not a
	// failure — whatever doesn't verify by then is returned as Pending and
	// the workflow re-queues it on the appropriate lane.
	adaptiveVerifyBatchRequest struct {
		Namespace             string
		NamespaceID           string
		TargetClusterEndpoint string
		TargetClusterName     string

		Executions       []*ExecutionInfo
		DeadlineMs       int64
		VerifyIntervalMs int64
	}

	// adaptiveVerifyBatchResponse reports outcomes. Pending is the list of
	// executions that didn't verify before the deadline.
	adaptiveVerifyBatchResponse struct {
		Verified int64
		Pending  []*ExecutionInfo
	}
)

// InjectBatch is the inject phase of one V3 batch: emit replication tasks
// for every execution at the configured per-batch RPS. The workflow
// dispatches this then immediately chains a VerifyBatch for the same
// executions while holding the lane slot. Splitting inject and verify into
// separate activities (vs combining them) gives the workflow per-phase
// visibility and lets the AIMD controller see which phase contributes to
// outcomes.
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
			// Same NotFound tolerance as the legacy GenerateReplicationTasks
			// — a deleted workflow won't land on the target, and verify will
			// skip it via checkSkipWorkflowExecution.
			if !common.IsNotFoundError(err) {
				return fmt.Errorf("generate replication task for %s/%s: %w", we.BusinessID, we.RunID, err)
			}
		}
		activity.RecordHeartbeat(ctx, i)
	}
	return nil
}

// VerifyBatch is the verify phase of one V3 batch. Skip-ahead verifies
// each execution against the target cluster, returning everything that
// didn't verify by DeadlineMs as Pending. Used for both fresh batches
// (chained after InjectBatch) and retry rounds (workflow re-dispatches
// just this activity with the already-injected pending list).
func (a *activities) VerifyBatch(ctx context.Context, request *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
	ctx = a.setCallerInfoForServerAPI(ctx, namespace.ID(request.NamespaceID))
	start := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.Timer(metrics.VerifyReplicationTasksLatency.Name()).Record(time.Since(start))
	}()
	return a.skipAheadVerify(ctx, request)
}

// skipAheadVerify is the shared skip-ahead verify loop. Differences from
// verifyReplicationTasks (the V1/V2 verifier):
//
//  1. Continues past not-yet-verified slots instead of breaking at the first
//     one. Every iteration scans all unverified executions, so a hot WF ID at
//     the head of the batch can't starve verification of the rest.
//  2. No no-progress timer in the activity — the wall-budget deadline returns
//     pending to the workflow, which is the failure-detection authority.
//
// Per-execution outcomes are turned into one of three terminal states:
//   - verified (DescribeMutableState returned OK, workflowVerifier OK)
//   - skipped (DescribeMutableState returned NotFound and checkSkipWorkflowExecution
//     decided this is a zombie / past-retention / etc — counts as verified for
//     completion purposes)
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

	// Build the v1-compatible request shape verifySingleReplicationTask
	// expects. The activity surface is different (skip-ahead vs strict
	// in-order) but the per-execution check is the same.
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
			outcome, err := a.skipAheadVerifySingle(ctx, singleReq, remoteAdminClient, nsEntry, we)
			if err != nil {
				return nil, err
			}
			if outcome == verifyOutcomeVerified {
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

type verifyOutcome int

const (
	verifyOutcomeBusy verifyOutcome = iota
	verifyOutcomeMissing
	verifyOutcomeVerified
)

// skipAheadVerifySingle mirrors the per-execution branches of
// verifySingleReplicationTask but returns a tri-state outcome the skip-ahead
// loop can act on without breaking.
func (a *activities) skipAheadVerifySingle(
	ctx context.Context,
	request *verifyReplicationTasksRequest,
	remoteAdminClient adminservice.AdminServiceClient,
	ns *namespace.Namespace,
	execution *ExecutionInfo,
) (verifyOutcome, error) {
	s := time.Now()
	archetype, err := a.archetypeIDToName(ctx, execution.ArchetypeID)
	if err != nil {
		return verifyOutcomeBusy, err
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
			return verifyOutcomeBusy, verr
		}
		if result.status == verified || result.status == skipped {
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskSuccess.Name()).Record(1)
			return verifyOutcomeVerified, nil
		}
		return verifyOutcomeMissing, nil

	case *serviceerror.NotFound:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskNotFound.Name()).Record(1)
		// checkSkipWorkflowExecution returns verified=skipped for zombie /
		// past-retention executions (which are legitimately absent on the
		// target). Treat that as a terminal verify; anything else stays
		// pending.
		result, serr := a.checkSkipWorkflowExecution(ctx, request, execution, ns)
		if serr != nil {
			return verifyOutcomeBusy, serr
		}
		if result.status == verified || result.status == skipped {
			return verifyOutcomeVerified, nil
		}
		return verifyOutcomeMissing, nil

	case *serviceerror.NamespaceNotFound:
		return verifyOutcomeBusy, temporal.NewNonRetryableApplicationError("failed to describe workflow from the remote cluster", "NamespaceNotFound", err)

	case *serviceerror.ResourceExhausted:
		if e.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
			// Apply is mid-flight on the passive — a small sign of progress
			// but not yet verified. Pending until the next scan.
			return verifyOutcomeBusy, nil
		}
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace), metrics.ServiceErrorTypeTag(err)).Counter(metrics.VerifyReplicationTaskFailed.Name()).Record(1)
		return verifyOutcomeBusy, fmt.Errorf("failed to describe workflow from the remote cluster: %w", err)

	default:
		a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace), metrics.ServiceErrorTypeTag(err)).Counter(metrics.VerifyReplicationTaskFailed.Name()).Record(1)
		return verifyOutcomeBusy, fmt.Errorf("failed to describe workflow from the remote cluster: %w", err)
	}
}
