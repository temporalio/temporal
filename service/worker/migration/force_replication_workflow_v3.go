package migration

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
)

// ForceReplicationWorkflowV3 is the adaptive forced-replication workflow. It
// addresses the failure mode V1/V2 hit under heavy WF-ID reuse, where the
// in-activity 30-minute no-progress timer is starved by a stuck head-of-batch
// even when the rest of the system is making progress.
//
// Two design changes relative to V1/V2:
//
//  1. Inject + skip-ahead verify run in one activity (GenerateAndVerifyBatch).
//     The verify scan continues past busy/missing slots instead of breaking,
//     so a hot WF ID at the head of a batch cannot starve the rest. The
//     activity returns whatever didn't verify within its wall-budget deadline
//     as Pending — that's not a failure, just a re-queue trigger.
//
//  2. The no-progress detector lives in the workflow. The aggregate verified
//     count across all batches is the authoritative progress signal; the
//     workflow fails only when that count stops advancing for
//     NoProgressTimeoutSeconds. A single stuck batch cannot mask cluster-wide
//     progress.
//
// On top of that, a two-lane router diverts executions identified as slow
// (sticky-quarantined by WF-ID or shard) into a slow lane that drip-feeds at
// reduced RPS and polls verify less often, reducing DescribeMutableState
// load on the passive and preventing the receiver-side
// ReplicationReceiverMaxOutstandingTaskCount gate from tripping.

type (
	// AdaptiveForceReplicationParams is the workflow input. It extends the
	// V1/V2 ForceReplicationParams shape with the adaptive knobs so a
	// caller migrating off V1/V2 has to add only the new fields.
	AdaptiveForceReplicationParams struct {
		// Shared with V1/V2.
		Namespace               string `validate:"required"`
		Query                   string `validate:"required"`
		ConcurrentActivityCount int
		OverallRps              float64
		GetParentInfoRPS        float64
		ListWorkflowsPageSize   int
		PageCountPerExecution   int
		NextPageToken           []byte

		// EnableVerification controls whether each batch's VerifyBatch
		// activity runs after InjectBatch. When false (inject-only mode),
		// the workflow's no-progress detector watches inject completion
		// instead of verify completion — useful when verify infra is
		// broken or when you want a best-effort replication without
		// blocking on the passive cluster. Note: quarantine and slow
		// lane are no-ops without verify pending signals, and AIMD only
		// sees clean inject batches so it ramps up monotonically.
		EnableVerification      bool
		TargetClusterEndpoint   string
		TargetClusterName       string
		VerifyIntervalInSeconds int `validate:"gte=0"`

		LastCloseTime                      time.Time
		LastStartTime                      time.Time
		ContinuedAsNewCount                int
		TaskQueueUserDataReplicationParams TaskQueueUserDataReplicationParams
		ReplicatedWorkflowCount            int64
		TotalForceReplicateWorkflowCount   int64

		TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus

		// Adaptive-specific knobs.

		// BatchDeadlineSeconds is the wall-budget each fast-lane batch gets to
		// verify its executions before returning what didn't verify as
		// pending. Not a failure condition — just a re-queue trigger. Slow
		// lane uses SlowLaneDeadlineSeconds.
		BatchDeadlineSeconds int

		// NoProgressTimeoutSeconds is the workflow-level no-progress detector.
		// If total verified count across all batches doesn't advance for this
		// long, the workflow fails non-retryably. Same operator-facing
		// semantic as V1/V2's defaultNoProgressNotRetryableTimeout but
		// observed at workflow scope.
		NoProgressTimeoutSeconds int

		// Slow-lane knobs. Slow lane is sticky-quarantined WF IDs and shards.
		SlowLaneRPS                   float64
		SlowLaneConcurrency           int
		SlowLaneBatchDeadlineSeconds  int
		SlowLaneVerifyIntervalSeconds int

		// Quarantine thresholds. After a WF ID has been observed in pending
		// across WFIDQuarantineThreshold batches, all future runs of that WF
		// ID go to the slow lane. After a shard has accumulated
		// ShardQuarantineThreshold pending occurrences (any WF ID hashed
		// onto it), the whole shard goes to slow lane.
		WFIDQuarantineThreshold  int
		ShardQuarantineThreshold int

		// Continue-as-new carry-over. Sticky quarantine state survives CAN
		// so a hot WF ID identified in cycle N is still slow-laned in cycle
		// N+1.
		QuarantinedWFIDs   []string
		QuarantinedShards  []int32
		WFIDPendingCounts  map[string]int
		ShardPendingCounts map[int32]int

		// HistoryShardCount is the source cluster's shard count. The
		// workflow uses it to map WF-ID → shard for the shard-pending
		// counter and shard quarantine.
		HistoryShardCount int32

		// AIMDEnabled gates the per-batch additive-increase /
		// multiplicative-decrease RPS controller. When enabled (default),
		// the workflow ramps each lane's effective per-batch RPS up after
		// every clean batch and cuts sharply on any pending — same shape
		// as TCP congestion control. Goal: hands-off operation where
		// healthy namespaces ramp toward upstream throughput on their
		// own, and contended ones settle at a sustainable rate without
		// operator tuning. *bool so a caller can set false explicitly
		// (zero-value of bool would mean "off", ambiguous with unset).
		AIMDEnabled *bool

		// AIMDIncreaseStep is the additive bump per clean batch, as a
		// fraction of the lane's INITIAL per-batch RPS so the curve is
		// invariant to the configured rate. Default 0.10 (+10% of
		// initial per clean batch).
		AIMDIncreaseStep float64

		// AIMDDecreaseFactor is the multiplicative cut on any batch
		// that returns pending. New RPS = current × factor. Default 0.5
		// (halve on pending). Lower values back off harder.
		AIMDDecreaseFactor float64

		// AIMDMinRPSFactor is the floor as a multiple of initial
		// per-batch RPS. Default 0.1.
		AIMDMinRPSFactor float64

		// AIMDMaxRPSFactor is the ceiling as a multiple of initial
		// per-batch RPS. Default 2.0 (ramp up to 2× configured). Set to
		// 1.0 to enforce OverallRps as a hard cap.
		AIMDMaxRPSFactor float64
	}

	// AdaptiveForceReplicationStatus is what the status query returns. Same
	// fields as V1/V2 plus quarantine counts so an operator can see how
	// much adaptive routing has kicked in.
	AdaptiveForceReplicationStatus struct {
		ForceReplicationStatus
		QuarantinedWFIDCount  int
		QuarantinedShardCount int
	}
)

const (
	forceReplicationWorkflowV3Name = "force-replication-v3"

	adaptiveForceReplicationStatusQueryType = "adaptive-force-replication-status"

	// Metric names for quarantine state transitions. All three are counters
	// tagged with the namespace; operators alert on a sudden spike of
	// quarantines (workload going off the rails) or the absence of
	// recoveries (shard stays hot for the whole run).
	adaptiveForceRepWFQuarantinedMetric    = "adaptive_force_replication_wf_quarantined_count"
	adaptiveForceRepShardQuarantinedMetric = "adaptive_force_replication_shard_quarantined_count"
	adaptiveForceRepShardRecoveredMetric   = "adaptive_force_replication_shard_recovered_count"

	defaultAdaptiveBatchDeadlineSeconds       = 60
	defaultAdaptiveNoProgressTimeoutSeconds   = 30 * 60 // matches V1/V2's defaultNoProgressNotRetryableTimeout
	defaultAdaptiveSlowLaneConcurrency        = 1
	defaultAdaptiveWFIDQuarantineThreshold    = 3
	defaultAdaptiveShardQuarantineThreshold   = 10
	defaultAdaptiveSlowLaneDeadlineMultiplier = 3
	defaultAdaptiveSlowLaneIntervalMultiplier = 3
	defaultAdaptiveSlowLaneRPSDivisor         = 10

	defaultAdaptiveAIMDIncreaseStep   = 0.10
	defaultAdaptiveAIMDDecreaseFactor = 0.5
	defaultAdaptiveAIMDMinRPSFactor   = 0.10
	defaultAdaptiveAIMDMaxRPSFactor   = 2.0

	// Metric names for AIMD RPS adjustments. Counter per direction so a
	// dashboard can render a heatmap of "this workload is bouncing
	// between ramp-up and back-off". Tagged with namespace + lane.
	adaptiveForceRepRPSIncreasedMetric = "adaptive_force_replication_rps_increased_count"
	adaptiveForceRepRPSDecreasedMetric = "adaptive_force_replication_rps_decreased_count"
)

// ForceReplicationWorkflowV3 is the adaptive variant. See package doc above
// for the design rationale.
func ForceReplicationWorkflowV3(ctx workflow.Context, params AdaptiveForceReplicationParams) error {
	startPageToken := params.NextPageToken

	if err := validateAndSetAdaptiveParams(&params); err != nil {
		return err
	}

	// state is populated after metadata + TQ-user-data kickoff; the query
	// handler reads from it once available so live counters (verified,
	// quarantine) are visible to operators querying mid-run. Before state
	// is built we fall back to the carry-over fields in params.
	var state *adaptiveWorkflowState
	_ = workflow.SetQueryHandler(ctx, adaptiveForceReplicationStatusQueryType, func() (AdaptiveForceReplicationStatus, error) {
		verified := params.ReplicatedWorkflowCount
		qWF := len(params.QuarantinedWFIDs)
		qShard := len(params.QuarantinedShards)
		if state != nil {
			verified = state.totalVerified
			qWF = len(state.quarantinedWF)
			qShard = len(state.quarantinedShard)
		}
		return AdaptiveForceReplicationStatus{
			ForceReplicationStatus: ForceReplicationStatus{
				LastCloseTime:                      params.LastCloseTime,
				LastStartTime:                      params.LastStartTime,
				ContinuedAsNewCount:                params.ContinuedAsNewCount,
				TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
				TotalWorkflowCount:                 params.TotalForceReplicateWorkflowCount,
				ReplicatedWorkflowCount:            verified,
				PageTokenForRestart:                startPageToken,
			},
			QuarantinedWFIDCount:  qWF,
			QuarantinedShardCount: qShard,
		}, nil
	})

	// V3 declares its own retry policy so it doesn't share the V1/V2 global
	// `forceReplicationActivityRetryPolicy`. The SDK's
	// applyRetryPolicyDefaults mutates the pointed-to policy on first use,
	// which races when V1 and V3 workflows run concurrently in the same
	// process (e.g. the test suites both with t.Parallel). One fresh
	// policy per workflow execution eliminates the shared mutable state.
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}

	if params.TotalForceReplicateWorkflowCount == 0 {
		wfCount, err := v3CountWorkflowForReplication(ctx, params.Namespace, params.Query, retryPolicy)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	metadataResp, err := v3GetClusterMetadata(ctx, params.Namespace, retryPolicy)
	if err != nil {
		return err
	}

	if !params.TaskQueueUserDataReplicationStatus.Done {
		if err := kickoffTaskQueueUserDataReplication(ctx, &params); err != nil {
			return err
		}
	}

	state = newAdaptiveWorkflowState(ctx, &params, metadataResp.NamespaceID, retryPolicy)
	if err := state.runOnePagedCycle(ctx); err != nil {
		return err
	}

	if params.NextPageToken == nil {
		// Final cycle: drain retries and wait for the TQ-user-data child.
		if err := state.drainRetries(ctx); err != nil {
			return err
		}
		if err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done }); err != nil {
			return err
		}
		if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
			return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
		}
		return nil
	}

	params.ContinuedAsNewCount++
	// Snapshot quarantine state into params so the next CAN cycle keeps
	// routing decisions consistent with what we've learned so far.
	params.QuarantinedWFIDs = state.snapshotQuarantinedWFIDs()
	params.QuarantinedShards = state.snapshotQuarantinedShards()
	params.WFIDPendingCounts = state.wfIDPending
	params.ShardPendingCounts = state.shardPending
	params.ReplicatedWorkflowCount = state.totalVerified
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflowV3, params)
}

func validateAndSetAdaptiveParams(params *AdaptiveForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: Namespace is required", "InvalidArgument", nil)
	}
	if params.EnableVerification && len(params.TargetClusterEndpoint) == 0 && len(params.TargetClusterName) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: TargetClusterEndpoint or TargetClusterName is required with verification enabled", "InvalidArgument", nil)
	}
	if params.ConcurrentActivityCount <= 0 {
		params.ConcurrentActivityCount = 1
	}
	if params.OverallRps <= 0 {
		params.OverallRps = float64(params.ConcurrentActivityCount)
	}
	if params.GetParentInfoRPS <= 0 {
		params.GetParentInfoRPS = float64(params.ConcurrentActivityCount)
	}
	if params.ListWorkflowsPageSize <= 0 {
		params.ListWorkflowsPageSize = defaultListWorkflowsPageSize
	}
	if params.PageCountPerExecution <= 0 {
		params.PageCountPerExecution = defaultPageCountPerExecution
	}
	if params.PageCountPerExecution > maxPageCountPerExecution {
		params.PageCountPerExecution = maxPageCountPerExecution
	}
	if params.VerifyIntervalInSeconds <= 0 {
		params.VerifyIntervalInSeconds = defaultVerifyIntervalInSeconds
	}
	if params.BatchDeadlineSeconds <= 0 {
		params.BatchDeadlineSeconds = defaultAdaptiveBatchDeadlineSeconds
	}
	if params.NoProgressTimeoutSeconds <= 0 {
		params.NoProgressTimeoutSeconds = defaultAdaptiveNoProgressTimeoutSeconds
	}
	if params.SlowLaneRPS <= 0 {
		params.SlowLaneRPS = params.OverallRps / float64(defaultAdaptiveSlowLaneRPSDivisor)
		if params.SlowLaneRPS < 1 {
			params.SlowLaneRPS = 1
		}
	}
	if params.SlowLaneConcurrency <= 0 {
		params.SlowLaneConcurrency = defaultAdaptiveSlowLaneConcurrency
	}
	if params.SlowLaneBatchDeadlineSeconds <= 0 {
		params.SlowLaneBatchDeadlineSeconds = params.BatchDeadlineSeconds * defaultAdaptiveSlowLaneDeadlineMultiplier
	}
	if params.SlowLaneVerifyIntervalSeconds <= 0 {
		params.SlowLaneVerifyIntervalSeconds = params.VerifyIntervalInSeconds * defaultAdaptiveSlowLaneIntervalMultiplier
	}
	if params.WFIDQuarantineThreshold <= 0 {
		params.WFIDQuarantineThreshold = defaultAdaptiveWFIDQuarantineThreshold
	}
	if params.ShardQuarantineThreshold <= 0 {
		params.ShardQuarantineThreshold = defaultAdaptiveShardQuarantineThreshold
	}
	if params.HistoryShardCount <= 0 {
		// Defensive — should always be set by the caller (or learned from
		// GetMetadata, see below). Without it, shard quarantine is disabled
		// because every WF ID hashes to shard 0.
		params.HistoryShardCount = 1
	}
	applyAdaptiveAIMDDefaults(params)
	return nil
}

// applyAdaptiveAIMDDefaults factors the AIMD-knob defaulting out of
// validateAndSetAdaptiveParams to keep that function under the
// cognitive-complexity threshold. Defaults are conservative: enabled,
// +10% per clean batch, halve on any pending, floor 10%/ceiling 200% of
// initial per-batch RPS.
func applyAdaptiveAIMDDefaults(params *AdaptiveForceReplicationParams) {
	if params.AIMDEnabled == nil {
		// *bool so an explicit false from a caller survives.
		t := true
		params.AIMDEnabled = &t
	}
	if params.AIMDIncreaseStep <= 0 {
		params.AIMDIncreaseStep = defaultAdaptiveAIMDIncreaseStep
	}
	if params.AIMDDecreaseFactor <= 0 {
		params.AIMDDecreaseFactor = defaultAdaptiveAIMDDecreaseFactor
	}
	if params.AIMDMinRPSFactor <= 0 {
		params.AIMDMinRPSFactor = defaultAdaptiveAIMDMinRPSFactor
	}
	if params.AIMDMaxRPSFactor <= 0 {
		params.AIMDMaxRPSFactor = defaultAdaptiveAIMDMaxRPSFactor
	}
}

// kickoffTaskQueueUserDataReplication starts the task-queue user-data
// replication child workflow exactly once (on the first non-CAN-continued
// cycle) and listens for its completion signal. V1/V2's
// maybeKickoffTaskQueueUserDataReplication gates this behind a GetVersion
// marker so existing in-flight workflows that predate the feature don't
// suddenly try to spawn a child; V3 is net-new so no histories pre-exist —
// the gate would only burn a marker event per cycle without affecting
// behavior.
func kickoffTaskQueueUserDataReplication(ctx workflow.Context, params *AdaptiveForceReplicationParams) error {
	workflow.Go(ctx, func(ctx workflow.Context) {
		doneCh := workflow.GetSignalChannel(ctx, taskQueueUserDataReplicationDoneSignalType)
		var errStr string
		// We don't care if there's more data to receive.
		_ = doneCh.Receive(ctx, &errStr)
		params.TaskQueueUserDataReplicationStatus.FailureMessage = errStr
		params.TaskQueueUserDataReplicationStatus.Done = true
	})

	// Only start the child on the first cycle so it doesn't get re-created
	// every continue-as-new.
	if params.ContinuedAsNewCount > 0 {
		return nil
	}

	options := workflow.ChildWorkflowOptions{
		WorkflowID:        fmt.Sprintf("%s-task-queue-user-data-replicator", workflow.GetInfo(ctx).WorkflowExecution.ID),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
	childCtx := workflow.WithChildOptions(ctx, options)
	input := TaskQueueUserDataReplicationParamsWithNamespace{
		TaskQueueUserDataReplicationParams: params.TaskQueueUserDataReplicationParams,
		Namespace:                          params.Namespace,
	}
	child := workflow.ExecuteChildWorkflow(childCtx, ForceTaskQueueUserDataReplicationWorkflow, input)
	var childExecution workflow.Execution
	return child.GetChildWorkflowExecution().Get(ctx, &childExecution)
}

// v3CountWorkflowForReplication mirrors V1's countWorkflowForReplication
// but takes a workflow-local retry policy instead of reading the
// V1/V2 global. See the comment in ForceReplicationWorkflowV3 about the
// SDK in-place defaulting race.
func v3CountWorkflowForReplication(ctx workflow.Context, namespace, query string, retryPolicy *temporal.RetryPolicy) (int64, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         retryPolicy,
	}
	var a *activities
	var output countWorkflowResponse
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, ao),
		a.CountWorkflow,
		&workflowservice.CountWorkflowExecutionsRequest{
			Namespace: namespace,
			Query:     query,
		}).Get(ctx, &output); err != nil {
		return 0, err
	}
	return output.WorkflowCount, nil
}

// v3GetClusterMetadata mirrors V1's getClusterMetadata; same rationale as
// v3CountWorkflowForReplication.
func v3GetClusterMetadata(ctx workflow.Context, namespace string, retryPolicy *temporal.RetryPolicy) (MetadataResponse, error) {
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	actx := workflow.WithLocalActivityOptions(ctx, lao)
	var a *activities
	var metadataResp MetadataResponse
	err := workflow.ExecuteLocalActivity(actx, a.GetMetadata, MetadataRequest{Namespace: namespace}).Get(ctx, &metadataResp)
	return metadataResp, err
}

// adaptiveWorkflowState bundles the mutable state the workflow coroutines
// touch. Workflow coroutines are cooperatively scheduled (yield only at SDK
// calls), so plain maps and slices are safe without mutexes.
type adaptiveWorkflowState struct {
	params      *AdaptiveForceReplicationParams
	namespaceID string

	// retryPolicy is workflow-scoped (not the V1/V2 global) so concurrent
	// workflow executions don't race on the SDK's in-place policy
	// defaulting.
	retryPolicy *temporal.RetryPolicy

	fastSem workflow.Channel
	slowSem workflow.Channel

	wfIDPending      map[string]int
	shardPending     map[int32]int
	quarantinedWF    map[string]struct{}
	quarantinedShard map[int32]struct{}

	fastPending []*ExecutionInfo
	slowPending []*ExecutionInfo

	totalVerified  int64
	lastVerified   int64
	lastProgressAt time.Time

	// AIMD controller state: per-lane current RPS plus the precomputed
	// step / floor / ceiling for each. currentFastRPS / currentSlowRPS
	// start at the configured initial rate and adjust after every
	// batch outcome via adjustRPS.
	currentFastRPS float64
	currentSlowRPS float64
	fastStep       float64
	slowStep       float64
	fastMinRPS     float64
	slowMinRPS     float64
	fastMaxRPS     float64
	slowMaxRPS     float64

	lastActivityErr error
}

func newAdaptiveWorkflowState(ctx workflow.Context, params *AdaptiveForceReplicationParams, namespaceID string, retryPolicy *temporal.RetryPolicy) *adaptiveWorkflowState {
	s := &adaptiveWorkflowState{
		params:           params,
		namespaceID:      namespaceID,
		retryPolicy:      retryPolicy,
		wfIDPending:      cloneStringIntMap(params.WFIDPendingCounts),
		shardPending:     cloneInt32IntMap(params.ShardPendingCounts),
		quarantinedWF:    sliceToStringSet(params.QuarantinedWFIDs),
		quarantinedShard: sliceToInt32Set(params.QuarantinedShards),
		totalVerified:    params.ReplicatedWorkflowCount,
		lastVerified:     params.ReplicatedWorkflowCount,
		lastProgressAt:   workflow.Now(ctx),
	}
	s.fastSem = workflow.NewBufferedChannel(ctx, params.ConcurrentActivityCount)
	for range params.ConcurrentActivityCount {
		s.fastSem.Send(ctx, true)
	}
	s.slowSem = workflow.NewBufferedChannel(ctx, params.SlowLaneConcurrency)
	for range params.SlowLaneConcurrency {
		s.slowSem.Send(ctx, true)
	}

	// AIMD initialization. The "initial per-batch RPS" is the
	// configured aggregate divided by lane concurrency — same shape as
	// V1/V2's per-activity rate. Step / floor / ceiling are derived
	// from initial × the configured factors so the curve is invariant
	// to OverallRps.
	initialFastRPS := params.OverallRps / float64(params.ConcurrentActivityCount)
	initialSlowRPS := params.SlowLaneRPS / float64(params.SlowLaneConcurrency)
	if initialSlowRPS <= 0 {
		initialSlowRPS = 1
	}
	s.currentFastRPS = initialFastRPS
	s.currentSlowRPS = initialSlowRPS
	s.fastStep = params.AIMDIncreaseStep * initialFastRPS
	s.slowStep = params.AIMDIncreaseStep * initialSlowRPS
	s.fastMinRPS = params.AIMDMinRPSFactor * initialFastRPS
	s.slowMinRPS = params.AIMDMinRPSFactor * initialSlowRPS
	s.fastMaxRPS = params.AIMDMaxRPSFactor * initialFastRPS
	s.slowMaxRPS = params.AIMDMaxRPSFactor * initialSlowRPS
	if s.fastMinRPS < 1 {
		s.fastMinRPS = 1
	}
	if s.slowMinRPS < 1 {
		s.slowMinRPS = 1
	}
	return s
}

// runOnePagedCycle does up to PageCountPerExecution pages of listing +
// dispatching GenerateAndVerifyBatch. Each page is split between fast and
// slow lanes based on current quarantine state. Returns when listing is
// exhausted or the page-count cap is reached; pending lists carry over to
// the retry phase or to the next CAN cycle.
func (s *adaptiveWorkflowState) runOnePagedCycle(ctx workflow.Context) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         s.retryPolicy,
	}
	listCtx := workflow.WithActivityOptions(ctx, ao)

	var targetClusters []string
	if s.params.TargetClusterName != "" {
		targetClusters = []string{s.params.TargetClusterName}
	}

	var a *activities
	for pages := 0; pages < s.params.PageCountPerExecution; pages++ {
		listReq := &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     s.params.Namespace,
			PageSize:      int32(s.params.ListWorkflowsPageSize),
			NextPageToken: s.params.NextPageToken,
			Query:         s.params.Query,
		}
		var listResp listWorkflowsResponse
		if err := workflow.ExecuteActivity(listCtx, a.ListWorkflows, listReq).Get(ctx, &listResp); err != nil {
			return err
		}
		s.params.NextPageToken = listResp.NextPageToken
		s.params.LastCloseTime = listResp.LastCloseTime
		s.params.LastStartTime = listResp.LastStartTime

		fast, slow := s.splitByLane(listResp.Executions)
		if len(fast) > 0 {
			s.dispatchInjectThenVerify(ctx, fast, false, targetClusters)
		}
		if len(slow) > 0 {
			s.dispatchInjectThenVerify(ctx, slow, true, targetClusters)
		}

		if s.params.NextPageToken == nil {
			break
		}
	}

	s.drainSemaphores(ctx)
	if err := s.checkProgress(ctx); err != nil {
		return err
	}
	if s.lastActivityErr != nil {
		return s.lastActivityErr
	}
	return nil
}

// drainRetries runs retry rounds until both pending lists are empty or the
// no-progress detector fires. Each round increases the per-batch deadline so
// the tail has progressively more time to drain on the passive.
func (s *adaptiveWorkflowState) drainRetries(ctx workflow.Context) error {
	for round := 0; len(s.fastPending) > 0 || len(s.slowPending) > 0; round++ {
		toRetryFast := s.fastPending
		toRetrySlow := s.slowPending
		s.fastPending = nil
		s.slowPending = nil

		// Re-classify fast retries — a WF ID may have crossed the
		// quarantine threshold since its original batch ran.
		var reFast, reSlow []*ExecutionInfo
		for _, ex := range toRetryFast {
			if s.isQuarantined(ex.BusinessID) {
				reSlow = append(reSlow, ex)
			} else {
				reFast = append(reFast, ex)
			}
		}
		reSlow = append(reSlow, toRetrySlow...)

		s.refillSemaphores(ctx)

		// Scale per-batch deadline by round on both lanes so the tail gets
		// progressively more time to drain. Cap so we don't end up with
		// hour-long deadlines on round 50 of a pathological run.
		const maxDeadlineMultiplier = 10
		mult := round + 2
		if mult > maxDeadlineMultiplier {
			mult = maxDeadlineMultiplier
		}
		fastDeadlineMs := int64(s.params.BatchDeadlineSeconds) * 1000 * int64(mult)
		slowDeadlineMs := int64(s.params.SlowLaneBatchDeadlineSeconds) * 1000 * int64(mult)

		// Fast-lane retry batches use the standard page size.
		for i := 0; i < len(reFast); i += s.params.ListWorkflowsPageSize {
			end := i + s.params.ListWorkflowsPageSize
			if end > len(reFast) {
				end = len(reFast)
			}
			s.dispatchVerifyOnly(ctx, reFast[i:end], false, fastDeadlineMs)
		}
		// Slow-lane batches are smaller so concurrency=1 doesn't pin a
		// single huge batch and starve the lane.
		slowBatchSize := s.params.ListWorkflowsPageSize / 4
		if slowBatchSize < 25 {
			slowBatchSize = 25
		}
		for i := 0; i < len(reSlow); i += slowBatchSize {
			end := i + slowBatchSize
			if end > len(reSlow) {
				end = len(reSlow)
			}
			s.dispatchVerifyOnly(ctx, reSlow[i:end], true, slowDeadlineMs)
		}

		s.drainSemaphores(ctx)
		if err := s.checkProgress(ctx); err != nil {
			return err
		}
		if s.lastActivityErr != nil {
			return s.lastActivityErr
		}
	}
	return nil
}

// dispatchInjectThenVerify chains InjectBatch + VerifyBatch on the
// requested lane, holding the lane's single slot through BOTH phases.
// This is the drip-feed contract: adjacent batches on the same lane
// serialize so the apply pipeline has time to drain between bursts.
// Slot release happens via defer regardless of which phase failed.
func (s *adaptiveWorkflowState) dispatchInjectThenVerify(ctx workflow.Context, execs []*ExecutionInfo, slow bool, targetClusters []string) {
	sem := s.fastSem
	rps := s.currentFastRPS
	deadlineMs := int64(s.params.BatchDeadlineSeconds) * 1000
	intervalMs := int64(s.params.VerifyIntervalInSeconds) * 1000
	if slow {
		sem = s.slowSem
		rps = s.currentSlowRPS
		if rps < 1 {
			rps = 1
		}
		deadlineMs = int64(s.params.SlowLaneBatchDeadlineSeconds) * 1000
		intervalMs = int64(s.params.SlowLaneVerifyIntervalSeconds) * 1000
	}
	lane := "fast"
	if slow {
		lane = "slow"
	}
	var slot bool
	sem.Receive(ctx, &slot)
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer sem.Send(ctx, true)
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Hour,
			HeartbeatTimeout:    time.Second * 60,
			RetryPolicy:         s.retryPolicy,
		}
		actx := workflow.WithActivityOptions(ctx, ao)
		var a *activities

		injReq := &adaptiveInjectBatchRequest{
			Namespace:        s.params.Namespace,
			NamespaceID:      s.namespaceID,
			TargetClusters:   targetClusters,
			Executions:       execs,
			RPS:              rps,
			GetParentInfoRPS: s.params.GetParentInfoRPS / float64(s.params.ConcurrentActivityCount),
		}
		if err := workflow.ExecuteActivity(actx, a.InjectBatch, injReq).Get(ctx, nil); err != nil {
			s.lastActivityErr = err
			s.routePending(execs)
			s.adjustRPS(ctx, lane, true)
			return
		}
		if !s.params.EnableVerification {
			// Inject-only mode: skip VerifyBatch and treat inject success
			// as terminal for this batch. Counts toward totalVerified so
			// the no-progress detector still has a signal (it now means
			// "inject is making progress" rather than "verify is making
			// progress"). No quarantine signal is possible in this mode
			// because there's no pending list to drive it — the slow
			// lane stays empty and the fast lane handles everything.
			s.totalVerified += int64(len(execs))
			s.adjustRPS(ctx, lane, false)
			return
		}
		verReq := &adaptiveVerifyBatchRequest{
			Namespace:             s.params.Namespace,
			NamespaceID:           s.namespaceID,
			TargetClusterEndpoint: s.params.TargetClusterEndpoint,
			TargetClusterName:     s.params.TargetClusterName,
			Executions:            execs,
			DeadlineMs:            deadlineMs,
			VerifyIntervalMs:      intervalMs,
		}
		var resp adaptiveVerifyBatchResponse
		if err := workflow.ExecuteActivity(actx, a.VerifyBatch, verReq).Get(ctx, &resp); err != nil {
			s.lastActivityErr = err
			s.routePending(execs)
			s.adjustRPS(ctx, lane, true)
			return
		}
		s.totalVerified += resp.Verified
		s.recordBatchOutcome(ctx, execs, resp.Pending)
		s.adjustRPS(ctx, lane, len(resp.Pending) > 0)
	})
}

// dispatchVerifyOnly is the retry-path dispatcher. No injection — these
// executions were already injected on their original pass; we just wait
// for apply to drain on the target. Uses the lane's slot semaphore so
// retries serialize against fresh batches on the same lane.
func (s *adaptiveWorkflowState) dispatchVerifyOnly(ctx workflow.Context, execs []*ExecutionInfo, slow bool, deadlineMs int64) {
	sem := s.fastSem
	intervalMs := int64(s.params.VerifyIntervalInSeconds) * 1000
	if slow {
		sem = s.slowSem
		intervalMs = int64(s.params.SlowLaneVerifyIntervalSeconds) * 1000
	}
	lane := "fast"
	if slow {
		lane = "slow"
	}
	var slot bool
	sem.Receive(ctx, &slot)
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer sem.Send(ctx, true)
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Hour,
			HeartbeatTimeout:    time.Second * 60,
			RetryPolicy:         s.retryPolicy,
		}
		actx := workflow.WithActivityOptions(ctx, ao)
		var a *activities
		req := &adaptiveVerifyBatchRequest{
			Namespace:             s.params.Namespace,
			NamespaceID:           s.namespaceID,
			TargetClusterEndpoint: s.params.TargetClusterEndpoint,
			TargetClusterName:     s.params.TargetClusterName,
			Executions:            execs,
			DeadlineMs:            deadlineMs,
			VerifyIntervalMs:      intervalMs,
		}
		var resp adaptiveVerifyBatchResponse
		if err := workflow.ExecuteActivity(actx, a.VerifyBatch, req).Get(ctx, &resp); err != nil {
			s.lastActivityErr = err
			s.routePending(execs)
			s.adjustRPS(ctx, lane, true)
			return
		}
		s.totalVerified += resp.Verified
		s.recordBatchOutcome(ctx, execs, resp.Pending)
		// Verify-only outcome feeds the controller too: a clean retry is
		// evidence apply has drained and the lane can support more
		// throughput; a still-pending retry is the strongest signal to
		// back off.
		s.adjustRPS(ctx, lane, len(resp.Pending) > 0)
	})
}

// recordBatchOutcome updates the WF-ID and shard counters in light of the
// batch's outcome, then routes pending executions onto their lane.
//
// Two design points worth knowing:
//
//  1. Shard counters skip increments for already-quarantined WF-IDs. A hot
//     WF-ID is already going to the slow lane; counting its repeated
//     pendings against the shard would inflate the shard counter and trip
//     shard quarantine off the very signal that's already been handled.
//     Shard quarantine only fires when MULTIPLE WF-IDs on the same shard
//     are independently struggling — a genuine "this shard is congested"
//     signal distinct from WF-ID hotness.
//
//  2. Shard counters decay on cleanly-verified executions (and shard
//     quarantine releases with hysteresis at threshold/2). Shard hotness
//     is transient — once the WF-IDs causing it move on, the shard is
//     fine again — so making it sticky like WF-ID quarantine would
//     misroute traffic indefinitely. WF-ID quarantine stays sticky
//     because temporal-locality argues a hot WF-ID stays hot.
func (s *adaptiveWorkflowState) recordBatchOutcome(ctx workflow.Context, dispatched, pending []*ExecutionInfo) {
	pendingSet := make(map[string]struct{}, len(pending))
	for _, ex := range pending {
		pendingSet[ex.BusinessID+"/"+ex.RunID] = struct{}{}
	}

	for _, ex := range pending {
		wasQuarantinedWF := false
		if _, ok := s.quarantinedWF[ex.BusinessID]; ok {
			wasQuarantinedWF = true
		}

		s.wfIDPending[ex.BusinessID]++
		if !wasQuarantinedWF && s.wfIDPending[ex.BusinessID] >= s.params.WFIDQuarantineThreshold {
			s.quarantinedWF[ex.BusinessID] = struct{}{}
			s.emitTransition(ctx, adaptiveForceRepWFQuarantinedMetric,
				"adaptive: quarantined WF-ID",
				"wfId", ex.BusinessID,
				"pendingCount", s.wfIDPending[ex.BusinessID])
			wasQuarantinedWF = true
		}

		if wasQuarantinedWF {
			// Already attributed to WF-ID hotness — don't double-count
			// against the shard.
			continue
		}
		sh := s.shardOf(ex.BusinessID)
		s.shardPending[sh]++
		if _, already := s.quarantinedShard[sh]; !already && s.shardPending[sh] >= s.params.ShardQuarantineThreshold {
			s.quarantinedShard[sh] = struct{}{}
			s.emitTransition(ctx, adaptiveForceRepShardQuarantinedMetric,
				"adaptive: quarantined shard",
				"shard", sh,
				"pendingCount", s.shardPending[sh])
		}
	}

	releaseAt := s.params.ShardQuarantineThreshold / 2
	if releaseAt < 1 {
		releaseAt = 1
	}
	for _, ex := range dispatched {
		if _, isPending := pendingSet[ex.BusinessID+"/"+ex.RunID]; isPending {
			continue
		}
		sh := s.shardOf(ex.BusinessID)
		if s.shardPending[sh] > 0 {
			s.shardPending[sh]--
		}
		if _, ok := s.quarantinedShard[sh]; ok && s.shardPending[sh] <= releaseAt {
			delete(s.quarantinedShard, sh)
			s.emitTransition(ctx, adaptiveForceRepShardRecoveredMetric,
				"adaptive: released shard quarantine",
				"shard", sh,
				"pendingCount", s.shardPending[sh])
		}
	}

	s.routePending(pending)
}

// routePending puts each execution onto whichever lane its current
// quarantine state says. Called from both the success path (with the
// activity's Pending list) and the failure path (with the whole batch).
func (s *adaptiveWorkflowState) routePending(execs []*ExecutionInfo) {
	for _, ex := range execs {
		if s.isQuarantined(ex.BusinessID) {
			s.slowPending = append(s.slowPending, ex)
		} else {
			s.fastPending = append(s.fastPending, ex)
		}
	}
}

func (s *adaptiveWorkflowState) splitByLane(execs []*ExecutionInfo) (fast, slow []*ExecutionInfo) {
	for _, ex := range execs {
		if s.isQuarantined(ex.BusinessID) {
			slow = append(slow, ex)
		} else {
			fast = append(fast, ex)
		}
	}
	return
}

func (s *adaptiveWorkflowState) isQuarantined(wfID string) bool {
	if _, ok := s.quarantinedWF[wfID]; ok {
		return true
	}
	if _, ok := s.quarantinedShard[s.shardOf(wfID)]; ok {
		return true
	}
	return false
}

// shardOf returns the history shard that owns a workflow ID, using the same
// (namespaceID, workflowID) → shard hash production uses everywhere else.
// Matching that mapping is the whole point: shard-quarantine decisions then
// land on the same shard whose apply queue is actually backed up.
func (s *adaptiveWorkflowState) shardOf(wfID string) int32 {
	return common.WorkflowIDToHistoryShard(s.namespaceID, wfID, s.params.HistoryShardCount)
}

// emitTransition is the single call point for "something changed quarantine
// state". Logs the event and increments the corresponding counter so
// operators have both human-readable timelines and aggregate dashboards.
// Tags every metric with the namespace; without that, two concurrent
// migrations on the same worker would alias their counters together.
func (s *adaptiveWorkflowState) emitTransition(ctx workflow.Context, metricName, message string, kvs ...any) {
	workflow.GetLogger(ctx).Info(message, kvs...)
	tags := map[string]string{
		metrics.OperationTagName: metrics.MigrationWorkflowScope,
		NamespaceTagName:         s.params.Namespace,
	}
	workflow.GetMetricsHandler(ctx).WithTags(tags).Counter(metricName).Inc(1)
}

// adjustRPS runs one AIMD step on the lane's current rate after a batch
// outcome. Clean batches additively raise the rate (slow ramp toward
// upstream throughput on healthy traffic); any pending multiplicatively
// cuts it (fast back-off the moment something goes wrong). Floor and
// ceiling factors keep the controller bounded. Adjustment is a no-op
// when AIMD is disabled or when the clamp prevents a change — only
// genuine transitions emit metrics and logs.
func (s *adaptiveWorkflowState) adjustRPS(ctx workflow.Context, lane string, hadPending bool) {
	if s.params.AIMDEnabled == nil || !*s.params.AIMDEnabled {
		return
	}
	var current *float64
	var minRPS, maxRPS, step float64
	switch lane {
	case "slow":
		current = &s.currentSlowRPS
		minRPS, maxRPS, step = s.slowMinRPS, s.slowMaxRPS, s.slowStep
	default:
		current = &s.currentFastRPS
		minRPS, maxRPS, step = s.fastMinRPS, s.fastMaxRPS, s.fastStep
	}
	prev := *current
	if hadPending {
		*current = *current * s.params.AIMDDecreaseFactor
		if *current < minRPS {
			*current = minRPS
		}
	} else {
		*current = *current + step
		if *current > maxRPS {
			*current = maxRPS
		}
	}
	if *current == prev {
		return
	}
	metricName := adaptiveForceRepRPSIncreasedMetric
	if hadPending {
		metricName = adaptiveForceRepRPSDecreasedMetric
	}
	workflow.GetLogger(ctx).Info("adaptive: AIMD adjusted RPS",
		"lane", lane,
		"prev", prev,
		"current", *current,
		"hadPending", hadPending)
	tags := map[string]string{
		metrics.OperationTagName: metrics.MigrationWorkflowScope,
		NamespaceTagName:         s.params.Namespace,
		"lane":                   lane,
	}
	workflow.GetMetricsHandler(ctx).WithTags(tags).Counter(metricName).Inc(1)
}

func (s *adaptiveWorkflowState) checkProgress(ctx workflow.Context) error {
	if s.totalVerified > s.lastVerified {
		s.lastVerified = s.totalVerified
		s.lastProgressAt = workflow.Now(ctx)
		return nil
	}
	noProgress := time.Duration(s.params.NoProgressTimeoutSeconds) * time.Second
	if workflow.Now(ctx).Sub(s.lastProgressAt) > noProgress {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("adaptive force-rep no progress for %v: %d verified, %d fast pending, %d slow pending, %d WF-IDs quarantined, %d shards quarantined",
				noProgress, s.totalVerified, len(s.fastPending), len(s.slowPending),
				len(s.quarantinedWF), len(s.quarantinedShard)),
			"AdaptiveForceReplicationNoProgress", nil)
	}
	return nil
}

func (s *adaptiveWorkflowState) drainSemaphores(ctx workflow.Context) {
	for range s.params.ConcurrentActivityCount {
		var slot bool
		s.fastSem.Receive(ctx, &slot)
	}
	for range s.params.SlowLaneConcurrency {
		var slot bool
		s.slowSem.Receive(ctx, &slot)
	}
}

func (s *adaptiveWorkflowState) refillSemaphores(ctx workflow.Context) {
	for range s.params.ConcurrentActivityCount {
		s.fastSem.Send(ctx, true)
	}
	for range s.params.SlowLaneConcurrency {
		s.slowSem.Send(ctx, true)
	}
}

func (s *adaptiveWorkflowState) snapshotQuarantinedWFIDs() []string {
	out := make([]string, 0, len(s.quarantinedWF))
	for k := range s.quarantinedWF {
		out = append(out, k)
	}
	return out
}

func (s *adaptiveWorkflowState) snapshotQuarantinedShards() []int32 {
	out := make([]int32, 0, len(s.quarantinedShard))
	for k := range s.quarantinedShard {
		out = append(out, k)
	}
	return out
}

func cloneStringIntMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneInt32IntMap(m map[int32]int) map[int32]int {
	out := make(map[int32]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func sliceToStringSet(s []string) map[string]struct{} {
	out := make(map[string]struct{}, len(s))
	for _, v := range s {
		out[v] = struct{}{}
	}
	return out
}

func sliceToInt32Set(s []int32) map[int32]struct{} {
	out := make(map[int32]struct{}, len(s))
	for _, v := range s {
		out[v] = struct{}{}
	}
	return out
}
