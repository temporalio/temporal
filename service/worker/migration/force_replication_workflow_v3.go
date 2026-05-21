package migration

import (
	"encoding/json"
	"fmt"
	"slices"
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
//  1. Inject and skip-ahead verify run as separate activities chained on the
//     same lane slot. VerifyBatch continues past busy/missing slots instead of
//     breaking, so a hot WF ID at the head of a batch cannot starve the rest,
//     and returns whatever didn't verify within its wall-budget deadline as
//     Pending — not a failure, just a re-queue trigger.
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
		Query                   string
		ConcurrentActivityCount int
		OverallRps              float64
		GetParentInfoRPS        float64
		ListWorkflowsPageSize   int
		PageCountPerExecution   int
		NextPageToken           []byte

		// EnableVerification controls whether each batch's VerifyBatch
		// activity runs after InjectBatch. When false (inject-only mode),
		// inject success is the progress signal — quarantine and slow lane
		// stay idle and AIMD ramps up monotonically.
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

		// BatchDeadlineSeconds is the wall-budget each fast-lane batch gets
		// to verify before returning unverified executions as pending. Slow
		// lane uses SlowLaneBatchDeadlineSeconds.
		BatchDeadlineSeconds int

		// NoProgressTimeoutSeconds fails the workflow non-retryably when
		// totalVerified hasn't advanced for this long.
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

		// FastPending and SlowPending carry unverified executions across
		// CAN. Without them the page cursor advances past pending
		// entries and they're never reprocessed. See pendingList for
		// the on-wire form.
		FastPending pendingList
		SlowPending pendingList

		// DrainOnly is set when listing has finished but drainRetries
		// bailed on GetContinueAsNewSuggested. The next cycle skips
		// listing and resumes draining the carried pending.
		DrainOnly bool

		// AIMDEnabled gates the per-batch additive-increase /
		// multiplicative-decrease RPS controller. *bool so an explicit
		// false from a caller is distinguishable from unset (defaults to
		// true).
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
	// fields as V1/V2 plus quarantine counts and live AIMD RPS so an
	// operator can see how much adaptive routing has kicked in.
	AdaptiveForceReplicationStatus struct {
		ForceReplicationStatus
		QuarantinedWFIDCount  int
		QuarantinedShardCount int
		CurrentFastRPS        float64
		CurrentSlowRPS        float64
	}
)

const (
	forceReplicationWorkflowV3Name = "force-replication-v3"

	adaptiveForceReplicationStatusQueryType = "adaptive-force-replication-status"

	// Quarantine transition counters, tagged with the namespace. Operators
	// alert on a spike of quarantines or absence of recoveries.
	forceRepWFQuarantinedMetric    = "force_replication_wf_quarantined_count"
	forceRepShardQuarantinedMetric = "force_replication_shard_quarantined_count"
	forceRepShardRecoveredMetric   = "force_replication_shard_recovered_count"

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

	// Caps on cross-CAN payload growth. Two axes (count and bytes), checked
	// at two sites: the early-CAN thresholds break out of the page loop;
	// the hard caps drain inline before snapshotting. Either tripping wins.
	// Bytes cap keeps us under the SDK's 512KB blob warn after the rest of
	// the params struct is accounted for.
	earlyCANPendingThreshold = 3000
	maxPendingCarryAcrossCAN = 6000
	earlyCANCarryBytes       = 256 * 1024
	maxCANCarryBytes         = 450 * 1024

	// AIMD adjustment counters, tagged with namespace and lane. Counter
	// per direction so dashboards can render ramp-up vs. back-off.
	forceRepRPSIncreasedMetric = "force_replication_rps_increased_count"
	forceRepRPSDecreasedMetric = "force_replication_rps_decreased_count"
)

// pendingList carries unverified executions across CAN with a compact
// JSON form. The wire format groups by BusinessID:
//
//	{"<bid>": [["<rid>", <aid>], ["<rid>", <aid>]], ...}
//
// vs the per-entry struct form this saves field-name overhead and
// amortizes the BusinessID across runs — common in the WF-ID-reuse
// case the pending list is sized for. Slice order after unmarshal is
// deterministic (sorted by BusinessID, then preserved within each
// group from the JSON array), so workflow replay produces the same
// activity-dispatch order as the original execution.
type pendingList []*ExecutionInfo

func (pl pendingList) MarshalJSON() ([]byte, error) {
	if len(pl) == 0 {
		return []byte("{}"), nil
	}
	grouped := make(map[string][][]any, len(pl))
	for _, e := range pl {
		grouped[e.BusinessID] = append(grouped[e.BusinessID], []any{e.RunID, e.ArchetypeID})
	}
	return json.Marshal(grouped)
}

func (pl *pendingList) UnmarshalJSON(b []byte) error {
	var grouped map[string][][]json.RawMessage
	if err := json.Unmarshal(b, &grouped); err != nil {
		return err
	}
	bids := make([]string, 0, len(grouped))
	for bid := range grouped {
		bids = append(bids, bid)
	}
	slices.Sort(bids)
	out := make(pendingList, 0)
	for _, bid := range bids {
		for _, tup := range grouped[bid] {
			if len(tup) != 2 {
				return fmt.Errorf("pendingList: expected 2-element [rid, aid] tuple for %q, got %d", bid, len(tup))
			}
			var rid string
			if err := json.Unmarshal(tup[0], &rid); err != nil {
				return fmt.Errorf("pendingList: decode RunID for %q: %w", bid, err)
			}
			var aid uint32
			if err := json.Unmarshal(tup[1], &aid); err != nil {
				return fmt.Errorf("pendingList: decode ArchetypeID for %q: %w", bid, err)
			}
			out = append(out, &ExecutionInfo{BusinessID: bid, RunID: rid, ArchetypeID: aid})
		}
	}
	*pl = out
	return nil
}

// ForceReplicationWorkflowV3 is the adaptive variant. See package doc above
// for the design rationale.
func ForceReplicationWorkflowV3(ctx workflow.Context, params AdaptiveForceReplicationParams) error {
	startPageToken := params.NextPageToken

	if err := validateAndSetAdaptiveParams(&params); err != nil {
		return err
	}

	// Query handler reads from state when available so live counters are
	// visible mid-run; before state is built, falls back to carry-over params.
	var state *adaptiveWorkflowState
	_ = workflow.SetQueryHandler(ctx, adaptiveForceReplicationStatusQueryType, func() (AdaptiveForceReplicationStatus, error) {
		verified := params.ReplicatedWorkflowCount
		qWF := len(params.QuarantinedWFIDs)
		qShard := len(params.QuarantinedShards)
		var fastRPS, slowRPS float64
		if state != nil {
			verified = state.totalVerified
			qWF = len(state.quarantinedWF)
			qShard = len(state.quarantinedShard)
			fastRPS = state.currentFastRPS
			slowRPS = state.currentSlowRPS
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
			CurrentFastRPS:        fastRPS,
			CurrentSlowRPS:        slowRPS,
		}, nil
	})

	// Workflow-scoped retry policy: the SDK's applyRetryPolicyDefaults
	// mutates the pointed-to policy in place, which races when concurrent
	// workflows share a global.
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

	state = newAdaptiveWorkflowState(ctx, &params, metadataResp.NamespaceID, metadataResp.ShardCount, retryPolicy)
	if err := state.runOnePagedCycle(ctx); err != nil {
		return err
	}

	if params.DrainOnly || params.NextPageToken == nil {
		// Final-cycle / drain-only path: try to drain, but bail if the
		// SDK signals we're approaching the history budget so we CAN
		// and resume draining next cycle.
		canSuggested, err := state.drainRetries(ctx)
		if err != nil {
			return err
		}
		if !canSuggested {
			// Pending fully drained — wait for the TQ-user-data child
			// and complete.
			if err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done }); err != nil {
				return err
			}
			if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
				return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
			}
			return nil
		}
		// History budget tripped mid-drain. Set DrainOnly so the next
		// cycle skips listing and resumes draining; fall through to the
		// CAN snapshot below.
		params.DrainOnly = true
	} else if len(state.fastPending)+len(state.slowPending) > maxPendingCarryAcrossCAN ||
		state.estimateCANPayloadBytes() > maxCANCarryBytes {
		// Overshoot past the early-CAN trigger pushed pending over the
		// hard cap; drain inline before snapshotting.
		if _, err := state.drainRetries(ctx); err != nil {
			return err
		}
	}

	params.ContinuedAsNewCount++
	params.QuarantinedWFIDs = state.snapshotQuarantinedWFIDs()
	params.QuarantinedShards = state.snapshotQuarantinedShards()
	params.WFIDPendingCounts = state.wfIDPending
	params.ShardPendingCounts = state.shardPending
	params.FastPending = pendingList(state.fastPending)
	params.SlowPending = pendingList(state.slowPending)
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
	applyAdaptiveAIMDDefaults(params)
	return nil
}

// applyAdaptiveAIMDDefaults sets conservative AIMD defaults: enabled,
// +10% per clean batch, halve on pending, floor 10% / ceiling 200% of
// initial per-batch RPS.
func applyAdaptiveAIMDDefaults(params *AdaptiveForceReplicationParams) {
	if params.AIMDEnabled == nil {
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
// replication child workflow exactly once on the first cycle and listens
// for its completion signal.
func kickoffTaskQueueUserDataReplication(ctx workflow.Context, params *AdaptiveForceReplicationParams) error {
	workflow.Go(ctx, func(ctx workflow.Context) {
		doneCh := workflow.GetSignalChannel(ctx, taskQueueUserDataReplicationDoneSignalType)
		var errStr string
		_ = doneCh.Receive(ctx, &errStr)
		params.TaskQueueUserDataReplicationStatus.FailureMessage = errStr
		params.TaskQueueUserDataReplicationStatus.Done = true
	})

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
	params            *AdaptiveForceReplicationParams
	namespaceID       string
	historyShardCount int32

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

func newAdaptiveWorkflowState(ctx workflow.Context, params *AdaptiveForceReplicationParams, namespaceID string, historyShardCount int32, retryPolicy *temporal.RetryPolicy) *adaptiveWorkflowState {
	s := &adaptiveWorkflowState{
		params:            params,
		namespaceID:       namespaceID,
		historyShardCount: historyShardCount,
		retryPolicy:       retryPolicy,
		wfIDPending:      cloneStringIntMap(params.WFIDPendingCounts),
		shardPending:     cloneInt32IntMap(params.ShardPendingCounts),
		quarantinedWF:    sliceToStringSet(params.QuarantinedWFIDs),
		quarantinedShard: sliceToInt32Set(params.QuarantinedShards),
		// Copy the carry-over rather than alias so the next CAN's
		// snapshot doesn't observe lists mutated by this cycle.
		fastPending:    append([]*ExecutionInfo(nil), params.FastPending...),
		slowPending:    append([]*ExecutionInfo(nil), params.SlowPending...),
		totalVerified:  params.ReplicatedWorkflowCount,
		lastVerified:   params.ReplicatedWorkflowCount,
		lastProgressAt: workflow.Now(ctx),
	}
	// state now owns the live pending lists; params will be repopulated
	// from state before continue-as-new.
	params.FastPending = nil
	params.SlowPending = nil
	s.fastSem = workflow.NewBufferedChannel(ctx, params.ConcurrentActivityCount)
	for range params.ConcurrentActivityCount {
		s.fastSem.Send(ctx, true)
	}
	s.slowSem = workflow.NewBufferedChannel(ctx, params.SlowLaneConcurrency)
	for range params.SlowLaneConcurrency {
		s.slowSem.Send(ctx, true)
	}

	// Initial per-batch RPS = aggregate / lane concurrency. Step / floor /
	// ceiling scale off initial so the controller is invariant to OverallRps.
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
// dispatching InjectBatch then VerifyBatch. Each page is split between
// fast and slow lanes based on current quarantine state. Returns when
// listing is exhausted, the page-count cap is reached, or combined
// pending exceeds earlyCANPendingThreshold. Pending lists carry over to
// drainRetries on the final cycle, or via the CAN snapshot to the next
// cycle.
func (s *adaptiveWorkflowState) runOnePagedCycle(ctx workflow.Context) error {
	if s.params.DrainOnly {
		// No listing to do; drain the pre-filled semaphores so drainRetries
		// starts from its expected empty state.
		s.drainSemaphores(ctx)
		return nil
	}

	// ListWorkflows doesn't heartbeat, so no HeartbeatTimeout.
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		RetryPolicy:         s.retryPolicy,
	}
	listCtx := workflow.WithActivityOptions(ctx, ao)

	var targetClusters []string
	if s.params.TargetClusterName != "" {
		targetClusters = []string{s.params.TargetClusterName}
	}

	var a *activities
	for pages := 0; pages < s.params.PageCountPerExecution; pages++ {
		// Bail before dispatching more work if a prior batch's activity
		// failed all retries. Without this, one broken namespace grinds
		// through every remaining page before surfacing the error.
		if s.lastActivityErr != nil {
			break
		}
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

		// Break early when pending hits the count or byte threshold so the
		// CAN input stays bounded. In-flight batches can still overshoot
		// during drainSemaphores below; the workflow-level hard cap
		// catches that.
		if len(s.fastPending)+len(s.slowPending) > earlyCANPendingThreshold ||
			s.estimateCANPayloadBytes() > earlyCANCarryBytes {
			break
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

// drainRetries runs retry rounds until both pending lists are empty, the
// no-progress detector fires, or the SDK signals that the workflow's
// history budget is close to exhausted. Each round increases the
// per-batch deadline so the tail has progressively more time to drain
// on the passive.
//
// Returns canSuggested=true when GetContinueAsNewSuggested fires mid-drain;
// the caller is expected to CAN with DrainOnly=true so the next cycle
// picks up where this one left off.
func (s *adaptiveWorkflowState) drainRetries(ctx workflow.Context) (canSuggested bool, err error) {
	for round := 0; len(s.fastPending) > 0 || len(s.slowPending) > 0; round++ {
		// Check before working so we don't burn another round when history
		// budget is already approaching the warn threshold.
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			return true, nil
		}
		// Yield between rounds so workflow time advances even if verify
		// returns immediately.
		if round > 0 {
			if err := workflow.Sleep(ctx, time.Second); err != nil {
				return false, err
			}
		}

		reFast, reSlow := s.reclassifyPendingForRetry()
		s.refillSemaphores(ctx)

		fastDeadlineMs, slowDeadlineMs := s.retryDeadlinesForRound(round)
		s.dispatchRetryChunks(ctx, reFast, false, s.params.ListWorkflowsPageSize, fastDeadlineMs)
		s.dispatchRetryChunks(ctx, reSlow, true, slowLaneRetryBatchSize(s.params.ListWorkflowsPageSize), slowDeadlineMs)

		s.drainSemaphores(ctx)
		if err := s.checkProgress(ctx); err != nil {
			return false, err
		}
		if s.lastActivityErr != nil {
			return false, s.lastActivityErr
		}
	}
	return false, nil
}

// reclassifyPendingForRetry consumes the current pending lists, then
// re-routes fast-lane entries onto the slow lane if their WF-ID has
// crossed the quarantine threshold since the original batch ran.
func (s *adaptiveWorkflowState) reclassifyPendingForRetry() (reFast, reSlow []*ExecutionInfo) {
	toRetryFast := s.fastPending
	toRetrySlow := s.slowPending
	s.fastPending = nil
	s.slowPending = nil

	for _, ex := range toRetryFast {
		if s.isQuarantined(ex.BusinessID) {
			reSlow = append(reSlow, ex)
		} else {
			reFast = append(reFast, ex)
		}
	}
	reSlow = append(reSlow, toRetrySlow...)
	return
}

// retryDeadlinesForRound scales the per-batch deadline by round number
// so the tail gets progressively more time to drain. Capped so a
// pathological run doesn't end up with hour-long deadlines.
func (s *adaptiveWorkflowState) retryDeadlinesForRound(round int) (fastMs, slowMs int64) {
	const maxDeadlineMultiplier = 10
	mult := round + 2
	if mult > maxDeadlineMultiplier {
		mult = maxDeadlineMultiplier
	}
	fastMs = int64(s.params.BatchDeadlineSeconds) * 1000 * int64(mult)
	slowMs = int64(s.params.SlowLaneBatchDeadlineSeconds) * 1000 * int64(mult)
	return
}

// dispatchRetryChunks dispatches verify-only batches over execs in
// chunks of batchSize.
func (s *adaptiveWorkflowState) dispatchRetryChunks(ctx workflow.Context, execs []*ExecutionInfo, slow bool, batchSize int, deadlineMs int64) {
	for i := 0; i < len(execs); i += batchSize {
		end := i + batchSize
		if end > len(execs) {
			end = len(execs)
		}
		s.dispatchVerifyOnly(ctx, execs[i:end], slow, deadlineMs)
	}
}

// slowLaneRetryBatchSize keeps slow-lane retry batches smaller than
// fast-lane so concurrency=1 doesn't pin a single huge batch and
// starve the lane.
func slowLaneRetryBatchSize(fastPageSize int) int {
	size := fastPageSize / 4
	if size < 25 {
		size = 25
	}
	return size
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
	var slot bool
	sem.Receive(ctx, &slot)
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer sem.Send(ctx, true)
		// HeartbeatTimeout < StartToCloseTimeout so worker crashes
		// surface in minutes; verify gets the longer 2m to absorb a
		// full scan pass on big batches.
		injectAO := workflow.ActivityOptions{
			StartToCloseTimeout: time.Hour,
			HeartbeatTimeout:    time.Second * 60,
			RetryPolicy:         s.retryPolicy,
		}
		verifyAO := workflow.ActivityOptions{
			StartToCloseTimeout: time.Hour,
			HeartbeatTimeout:    time.Minute * 2,
			RetryPolicy:         s.retryPolicy,
		}
		injectCtx := workflow.WithActivityOptions(ctx, injectAO)
		verifyCtx := workflow.WithActivityOptions(ctx, verifyAO)
		var a *activities

		injReq := &adaptiveInjectBatchRequest{
			Namespace:        s.params.Namespace,
			NamespaceID:      s.namespaceID,
			TargetClusters:   targetClusters,
			Executions:       execs,
			RPS:              rps,
			GetParentInfoRPS: s.params.GetParentInfoRPS / float64(s.params.ConcurrentActivityCount),
		}
		if err := workflow.ExecuteActivity(injectCtx, a.InjectBatch, injReq).Get(ctx, nil); err != nil {
			s.lastActivityErr = err
			s.routePending(execs)
			s.adjustRPS(ctx, slow, true)
			return
		}
		if !s.params.EnableVerification {
			// Inject-only mode: count inject success toward totalVerified
			// so the no-progress detector still has a signal.
			s.totalVerified += int64(len(execs))
			s.adjustRPS(ctx, slow, false)
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
		if err := workflow.ExecuteActivity(verifyCtx, a.VerifyBatch, verReq).Get(ctx, &resp); err != nil {
			s.lastActivityErr = err
			s.routePending(execs)
			s.adjustRPS(ctx, slow, true)
			return
		}
		s.totalVerified += resp.Verified
		s.recordBatchOutcome(ctx, execs, resp.Pending)
		s.adjustRPS(ctx, slow, len(resp.Pending) > 0)
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
	var slot bool
	sem.Receive(ctx, &slot)
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer sem.Send(ctx, true)
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Hour,
			HeartbeatTimeout:    time.Minute * 2,
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
			s.adjustRPS(ctx, slow, true)
			return
		}
		s.totalVerified += resp.Verified
		s.recordBatchOutcome(ctx, execs, resp.Pending)
		s.adjustRPS(ctx, slow, len(resp.Pending) > 0)
	})
}

// recordBatchOutcome updates the WF-ID and shard counters and routes pending
// executions onto their lane.
//
// Shard counters skip increments for already-quarantined WF-IDs so a hot
// WF-ID doesn't double-count and trip shard quarantine off its own signal;
// shard quarantine should only fire when MULTIPLE WF-IDs on the same shard
// are independently struggling. Shard counters then decay on clean verifies
// (release at threshold/2 hysteresis) because shard hotness is transient,
// whereas WF-ID quarantine stays sticky.
func (s *adaptiveWorkflowState) recordBatchOutcome(ctx workflow.Context, dispatched, pending []*ExecutionInfo) {
	type execKey struct{ bid, rid string }
	pendingSet := make(map[execKey]struct{}, len(pending))
	for _, ex := range pending {
		pendingSet[execKey{ex.BusinessID, ex.RunID}] = struct{}{}
	}

	for _, ex := range pending {
		wasQuarantinedWF := false
		if _, ok := s.quarantinedWF[ex.BusinessID]; ok {
			wasQuarantinedWF = true
		}

		s.wfIDPending[ex.BusinessID]++
		if !wasQuarantinedWF && s.wfIDPending[ex.BusinessID] >= s.params.WFIDQuarantineThreshold {
			s.quarantinedWF[ex.BusinessID] = struct{}{}
			s.emitTransition(ctx, forceRepWFQuarantinedMetric,
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
			s.emitTransition(ctx, forceRepShardQuarantinedMetric,
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
		if _, isPending := pendingSet[execKey{ex.BusinessID, ex.RunID}]; isPending {
			continue
		}
		sh := s.shardOf(ex.BusinessID)
		if s.shardPending[sh] > 0 {
			s.shardPending[sh]--
		}
		if _, ok := s.quarantinedShard[sh]; ok && s.shardPending[sh] <= releaseAt {
			delete(s.quarantinedShard, sh)
			s.emitTransition(ctx, forceRepShardRecoveredMetric,
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
	return common.WorkflowIDToHistoryShard(s.namespaceID, wfID, s.historyShardCount)
}

// emitTransition is the single call point for "something changed quarantine
// state". Logs and increments a namespace-tagged counter so two
// concurrent migrations on the same worker don't alias counters together.
func (s *adaptiveWorkflowState) emitTransition(ctx workflow.Context, metricName, message string, kvs ...any) {
	workflow.GetLogger(ctx).Info(message, kvs...)
	tags := map[string]string{
		metrics.OperationTagName: metrics.MigrationWorkflowScope,
		NamespaceTagName:         s.params.Namespace,
	}
	workflow.GetMetricsHandler(ctx).WithTags(tags).Counter(metricName).Inc(1)
}

// adjustRPS runs one AIMD step on the lane's current rate: clean batches
// additively raise, pending multiplicatively cuts. Clamped to min/max
// factors; only genuine transitions emit metrics.
func (s *adaptiveWorkflowState) adjustRPS(ctx workflow.Context, slow bool, hadPending bool) {
	if s.params.AIMDEnabled == nil || !*s.params.AIMDEnabled {
		return
	}
	current := &s.currentFastRPS
	minRPS, maxRPS, step := s.fastMinRPS, s.fastMaxRPS, s.fastStep
	lane := "fast"
	if slow {
		current = &s.currentSlowRPS
		minRPS, maxRPS, step = s.slowMinRPS, s.slowMaxRPS, s.slowStep
		lane = "slow"
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
	metricName := forceRepRPSIncreasedMetric
	if hadPending {
		metricName = forceRepRPSDecreasedMetric
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

// estimateCANPayloadBytes returns the marshalled size of the input-growing
// CAN fields (pending lists + user-keyed quarantine state). Shard-keyed
// state is bounded by shard count and excluded.
func (s *adaptiveWorkflowState) estimateCANPayloadBytes() int {
	//workflowcheck:ignore (json.Marshal sorts map keys; pendingList wire form is deterministic; result used only for len())
	b, _ := json.Marshal(struct {
		Fast              pendingList
		Slow              pendingList
		QuarantinedWFIDs  []string
		WFIDPendingCounts map[string]int
	}{
		Fast:              pendingList(s.fastPending),
		Slow:              pendingList(s.slowPending),
		QuarantinedWFIDs:  s.snapshotQuarantinedWFIDs(),
		WFIDPendingCounts: s.wfIDPending,
	})
	return len(b)
}

// snapshotQuarantinedWFIDs returns the quarantined WF-ID set as a sorted
// slice; sorting makes the CAN input deterministic across replays.
func (s *adaptiveWorkflowState) snapshotQuarantinedWFIDs() []string {
	out := make([]string, 0, len(s.quarantinedWF))
	//workflowcheck:ignore (output is sorted below, so map iteration order does not affect history)
	for k := range s.quarantinedWF {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func (s *adaptiveWorkflowState) snapshotQuarantinedShards() []int32 {
	out := make([]int32, 0, len(s.quarantinedShard))
	//workflowcheck:ignore (output is sorted below, so map iteration order does not affect history)
	for k := range s.quarantinedShard {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func cloneStringIntMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	//workflowcheck:ignore (cloning into a new map of the same contents; iteration order does not affect history)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneInt32IntMap(m map[int32]int) map[int32]int {
	out := make(map[int32]int, len(m))
	//workflowcheck:ignore (cloning into a new map of the same contents; iteration order does not affect history)
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
