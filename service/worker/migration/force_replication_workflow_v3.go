package migration

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
)

// ForceReplicationWorkflowV3 is the adaptive forced-replication workflow.
// Two design properties matter for understanding the rest of the file:
//
//  1. Inject and skip-ahead verify run as separate activities chained on the
//     same lane slot. VerifyBatch continues past busy/missing slots, so a hot
//     WF ID at the head of a batch cannot starve the rest, and returns
//     whatever didn't verify within its wall-budget deadline as Pending —
//     not a failure, just a re-queue trigger.
//
//  2. The no-progress detector lives in the workflow. The aggregate verified
//     count across all batches is the authoritative progress signal; the
//     workflow fails only when that count stops advancing for
//     NoProgressTimeoutSeconds. A single stuck batch cannot mask cluster-wide
//     progress.
//
// On top of that, a two-lane router diverts executions on backed-up shards
// (sticky-quarantined) into a slow lane that drip-feeds at reduced RPS and
// polls verify less often, reducing DescribeMutableState load on the passive
// and preventing the receiver-side ReplicationReceiverMaxOutstandingTaskCount
// gate from tripping.
//
// Detection signal: shard quarantine. Every pending BID from a verify batch
// charges its hosting shard's counter (per-batch dedupe). When a shard
// accumulates ShardQuarantineThreshold pendings, every BID hashed onto that
// shard routes to the slow lane. The counter decays once per batch per
// shard that had clean traffic and zero pendings in the same batch, with a
// threshold/2 hysteresis floor on release. A hot WF-ID is just one reason a
// shard backs up; routing at the shard level catches both the "one hot key"
// and "many cool keys collectively saturate" cases without per-key
// bookkeeping. The transition log carries the triggering BID so
// per-workflow observability survives the shard-only model.

type (
	AdaptiveForceReplicationParams struct {
		Namespace               string `validate:"required"`
		Query                   string
		ConcurrentActivityCount int
		OverallRps              float64
		ListWorkflowsPageSize   int
		PageCountPerExecution   int
		NextPageToken           []byte

		// EnableVerification runs VerifyBatch after InjectBatch. When
		// false (inject-only mode), quarantine, slow lane, AIMD back-off,
		// and the no-progress detector all stay idle, and
		// ReplicatedWorkflowCount in the status query reflects injected,
		// not confirmed-replicated, executions.
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

		// LastProgressAt is the workflow time of the last totalVerified
		// advance, carried across CAN so the no-progress detector spans
		// cycles. Zero on the first cycle falls back to workflow.Now.
		LastProgressAt time.Time

		TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus

		// NamespaceID and HistoryShardCount are cached from a GetMetadata
		// call on the first cycle and carried across CAN. A caller may set
		// HistoryShardCount explicitly to override the source cluster's
		// shard count (e.g. when the target side has a different shard
		// count); leaving either zero on the initial call falls back to
		// the source cluster's value.
		NamespaceID       string
		HistoryShardCount int32

		// BatchDeadlineSeconds is the wall-budget each fast-lane batch gets
		// to verify before returning unverified executions as pending. Slow
		// lane uses SlowLaneBatchDeadlineSeconds.
		BatchDeadlineSeconds int

		// NoProgressTimeoutSeconds fails the workflow non-retryably when
		// totalVerified hasn't advanced for this long.
		NoProgressTimeoutSeconds int

		// Slow-lane knobs. Slow lane is sticky-quarantined shards.
		SlowLaneRPS                   float64
		SlowLaneConcurrency           int
		SlowLaneBatchDeadlineSeconds  int
		SlowLaneVerifyIntervalSeconds int

		// ShardQuarantineThreshold is the pending count at which a shard
		// routes into the slow lane. Higher values absorb more transient
		// hotness before paying the slow-lane cost; lower values react
		// sooner at the risk of flapping.
		ShardQuarantineThreshold int

		// Continue-as-new carry-over for shard-keyed quarantine state.
		// Bounded by HistoryShardCount, safe to carry.
		QuarantinedShards  []int32
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

		// AIMDDisabled turns off the per-batch additive-increase /
		// multiplicative-decrease RPS controller. Inverted so the zero
		// value leaves AIMD on (the production default) and callers
		// explicitly opt out with true.
		AIMDDisabled bool

		// AIMDIncreaseStep is the additive bump per clean batch, as a
		// fraction of the lane's initial per-batch RPS so the curve is
		// invariant to the configured rate. Default 0.10.
		AIMDIncreaseStep float64

		// AIMDDecreaseFactor is the multiplicative cut on a batch that
		// returns pending: new RPS = current × factor. Default 0.5.
		AIMDDecreaseFactor float64

		// AIMDMinRPSFactor is the floor as a multiple of initial per-batch
		// RPS. Effective floor is max(factor × initial, 1) — a sub-1 RPS
		// floor is clamped to 1 so the controller can't stall the lane.
		// Default 0.1.
		AIMDMinRPSFactor float64

		// AIMDMaxRPSFactor is the ceiling as a multiple of initial
		// per-batch RPS. Default 2.0. Set to 1.0 to enforce OverallRps
		// as a hard cap.
		AIMDMaxRPSFactor float64
	}

	// AdaptiveForceReplicationStatus is what the status query returns.
	// Quarantine count and live AIMD RPS let an operator see how much
	// adaptive routing has kicked in.
	AdaptiveForceReplicationStatus struct {
		ForceReplicationStatus
		QuarantinedShardCount int
		CurrentFastRPS        float64
		CurrentSlowRPS        float64
	}
)

const (
	// Workflow identifiers.
	forceReplicationWorkflowV3Name          = "force-replication-v3"
	adaptiveForceReplicationStatusQueryType = "adaptive-force-replication-status"

	// Defaults applied in validateAndSetAdaptiveParams when a caller
	// leaves the corresponding param at its zero value.
	defaultAdaptiveBatchDeadlineSeconds       = 60
	defaultAdaptiveNoProgressTimeoutSeconds   = 30 * 60
	defaultAdaptiveSlowLaneConcurrency        = 1
	defaultAdaptiveShardQuarantineThreshold   = 10
	defaultAdaptiveSlowLaneDeadlineMultiplier = 3
	defaultAdaptiveSlowLaneIntervalMultiplier = 3
	defaultAdaptiveSlowLaneRPSDivisor         = 10
	defaultAdaptiveAIMDIncreaseStep           = 0.10
	defaultAdaptiveAIMDDecreaseFactor         = 0.5
	defaultAdaptiveAIMDMinRPSFactor           = 0.10
	defaultAdaptiveAIMDMaxRPSFactor           = 2.0

	// Caps on cross-CAN payload growth. Two axes (count and bytes),
	// checked at two sites: the early-CAN thresholds break out of the
	// page loop; the hard caps drain inline before snapshotting. Either
	// tripping wins. Bytes cap keeps us under the SDK's 512KB blob warn
	// after the rest of the params struct is accounted for.
	earlyCANPendingThreshold = 3000
	maxPendingCarryAcrossCAN = 6000
	earlyCANCarryBytes       = 256 * 1024
	maxCANCarryBytes         = 450 * 1024
)

// pendingList carries unverified executions across CAN. The wire form
// groups by BusinessID to amortize repeated BIDs (common in the WF-ID-
// reuse case):
//
//	{"<bid>": [["<rid>", <aid>], ["<rid>", <aid>]], ...}
//
// Slice order after unmarshal is deterministic (BIDs sorted, runs preserved
// within each group), so workflow replay produces the same dispatch order.
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
				return fmt.Errorf("pendingList %q: expected 2-element [rid, aid] tuple, got %d", bid, len(tup))
			}
			var rid string
			if err := json.Unmarshal(tup[0], &rid); err != nil {
				return fmt.Errorf("pendingList %q: decode RunID: %w", bid, err)
			}
			var aid uint32
			if err := json.Unmarshal(tup[1], &aid); err != nil {
				return fmt.Errorf("pendingList %q: decode ArchetypeID: %w", bid, err)
			}
			out = append(out, &ExecutionInfo{BusinessID: bid, RunID: rid, ArchetypeID: aid})
		}
	}
	*pl = out
	return nil
}

func ForceReplicationWorkflowV3(ctx workflow.Context, params AdaptiveForceReplicationParams) error {
	startPageToken := params.NextPageToken

	if err := validateAndSetAdaptiveParams(&params); err != nil {
		return err
	}

	var state *adaptiveWorkflowState
	registerAdaptiveStatusQuery(ctx, &params, startPageToken, &state)

	retryPolicy := newForceReplicationRetryPolicy()

	if params.TotalForceReplicateWorkflowCount == 0 {
		wfCount, err := countWorkflowForReplication(ctx, params.Namespace, params.Query, retryPolicy)
		if err != nil {
			return err
		}
		params.TotalForceReplicateWorkflowCount = wfCount
	}

	if err := resolveAdaptiveMetadata(ctx, &params, retryPolicy); err != nil {
		return err
	}

	if !params.TaskQueueUserDataReplicationStatus.Done {
		if err := kickoffTaskQueueUserDataReplication(ctx, &params); err != nil {
			return err
		}
	}

	state = newAdaptiveWorkflowState(ctx, &params, retryPolicy)
	if err := state.runOnePagedCycle(ctx); err != nil {
		return err
	}

	complete, err := state.finalizeBeforeCAN(ctx)
	if err != nil {
		return err
	}
	if complete {
		return awaitTaskQueueUserDataDone(ctx, &params)
	}

	params.ContinuedAsNewCount++
	params.QuarantinedShards = state.snapshotQuarantinedShards()
	params.ShardPendingCounts = state.shardPending
	params.FastPending = pendingList(state.fastPending)
	params.SlowPending = pendingList(state.slowPending)
	params.ReplicatedWorkflowCount = state.totalVerified
	params.LastProgressAt = state.lastProgressAt
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflowV3, params)
}

// registerAdaptiveStatusQuery installs the status query handler. The
// handler reads from state when it's been built so live counters are
// visible mid-run, and falls back to the carry-over params before then.
func registerAdaptiveStatusQuery(ctx workflow.Context, params *AdaptiveForceReplicationParams, startPageToken []byte, stateRef **adaptiveWorkflowState) {
	_ = workflow.SetQueryHandler(ctx, adaptiveForceReplicationStatusQueryType, func() (AdaptiveForceReplicationStatus, error) {
		verified := params.ReplicatedWorkflowCount
		qShard := len(params.QuarantinedShards)
		var fastRPS, slowRPS float64
		if s := *stateRef; s != nil {
			verified = s.totalVerified
			qShard = len(s.quarantinedShard)
			fastRPS = s.currentFastRPS
			slowRPS = s.currentSlowRPS
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
			QuarantinedShardCount: qShard,
			CurrentFastRPS:        fastRPS,
			CurrentSlowRPS:        slowRPS,
		}, nil
	})
}

// resolveAdaptiveMetadata fills NamespaceID and HistoryShardCount from a
// GetMetadata call when either is unset, leaving caller-provided values
// untouched (callers may override HistoryShardCount when the target has
// a different shard count).
func resolveAdaptiveMetadata(ctx workflow.Context, params *AdaptiveForceReplicationParams, retryPolicy *temporal.RetryPolicy) error {
	if params.NamespaceID != "" && params.HistoryShardCount != 0 {
		return nil
	}
	metadataResp, err := getClusterMetadata(ctx, params.Namespace, retryPolicy)
	if err != nil {
		return err
	}
	if params.NamespaceID == "" {
		params.NamespaceID = metadataResp.NamespaceID
	}
	if params.HistoryShardCount == 0 {
		params.HistoryShardCount = metadataResp.ShardCount
	}
	return nil
}

// finalizeBeforeCAN runs end-of-cycle drain logic. Returns complete=true
// when the workflow should await TQ-user-data and complete; complete=false
// means the caller should snapshot state and continue-as-new. On the
// final/drain-only path a mid-drain history-budget trip flips DrainOnly
// on so the next cycle resumes draining.
func (s *adaptiveWorkflowState) finalizeBeforeCAN(ctx workflow.Context) (complete bool, err error) {
	params := s.params
	if params.DrainOnly || params.NextPageToken == nil {
		canSuggested, drainErr := s.drainRetries(ctx)
		if drainErr != nil {
			return false, drainErr
		}
		if !canSuggested {
			return true, nil
		}
		params.DrainOnly = true
		return false, nil
	}
	if len(s.fastPending)+len(s.slowPending) > maxPendingCarryAcrossCAN ||
		s.estimateCANPayloadBytes() > maxCANCarryBytes {
		// Overshoot past the early-CAN trigger pushed pending over the
		// hard cap; drain inline before snapshotting.
		if _, drainErr := s.drainRetries(ctx); drainErr != nil {
			return false, drainErr
		}
	}
	return false, nil
}

func awaitTaskQueueUserDataDone(ctx workflow.Context, params *AdaptiveForceReplicationParams) error {
	if err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done }); err != nil {
		return err
	}
	if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
		return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
	}
	return nil
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
	if params.ShardQuarantineThreshold <= 0 {
		params.ShardQuarantineThreshold = defaultAdaptiveShardQuarantineThreshold
	}
	applyAdaptiveAIMDDefaults(params)
	return nil
}

// applyAdaptiveAIMDDefaults sets conservative AIMD defaults: enabled,
// +10% per clean batch, halve on pending, floor 10% / ceiling 200% of
// initial per-batch RPS. Split out of validateAndSetAdaptiveParams to
// keep that function's cyclomatic complexity under the lint threshold.
func applyAdaptiveAIMDDefaults(params *AdaptiveForceReplicationParams) {
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

// Workflow coroutines yield only at SDK calls, so plain maps and slices
// on adaptiveWorkflowState are safe without mutexes.
type adaptiveWorkflowState struct {
	params *AdaptiveForceReplicationParams

	retryPolicy *temporal.RetryPolicy

	fastSem workflow.Channel
	slowSem workflow.Channel

	shardPending     map[int32]int
	quarantinedShard map[int32]struct{}

	fastPending []*ExecutionInfo
	slowPending []*ExecutionInfo

	totalVerified  int64
	lastVerified   int64
	lastProgressAt time.Time

	// AIMD controller: per-lane current RPS plus the precomputed step,
	// floor, and ceiling. currentFastRPS / currentSlowRPS adjust after
	// every batch outcome via adjustRPS.
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

func newAdaptiveWorkflowState(ctx workflow.Context, params *AdaptiveForceReplicationParams, retryPolicy *temporal.RetryPolicy) *adaptiveWorkflowState {
	s := &adaptiveWorkflowState{
		params:           params,
		retryPolicy:      retryPolicy,
		shardPending:     cloneInt32IntMap(params.ShardPendingCounts),
		quarantinedShard: sliceToInt32Set(params.QuarantinedShards),
		// Copy the carry-over rather than alias so the next CAN's
		// snapshot doesn't observe lists mutated by this cycle.
		fastPending:   append([]*ExecutionInfo(nil), params.FastPending...),
		slowPending:   append([]*ExecutionInfo(nil), params.SlowPending...),
		totalVerified: params.ReplicatedWorkflowCount,
		lastVerified:  params.ReplicatedWorkflowCount,
		// Carry lastProgressAt across CAN so the no-progress detector
		// spans cycles. Zero means first cycle — use now.
		lastProgressAt: params.LastProgressAt,
	}
	if s.lastProgressAt.IsZero() {
		s.lastProgressAt = workflow.Now(ctx)
	}
	// state now owns the live pending lists; params will be repopulated
	// from state before continue-as-new.
	params.FastPending = nil
	params.SlowPending = nil
	s.fastSem = workflow.NewBufferedChannel(ctx, params.ConcurrentActivityCount)
	s.slowSem = workflow.NewBufferedChannel(ctx, params.SlowLaneConcurrency)
	// Pre-fill the semaphores only on the listing path. The DrainOnly
	// path skips listing and goes straight into drainRetries, which
	// refills the sems at the top of each round; pre-filling there
	// would just be cycled out again by an unnecessary drain.
	if !params.DrainOnly {
		for range params.ConcurrentActivityCount {
			s.fastSem.Send(ctx, true)
		}
		for range params.SlowLaneConcurrency {
			s.slowSem.Send(ctx, true)
		}
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

// runOnePagedCycle lists workflows in pages and dispatches inject+verify
// for each. Returns when listing is exhausted, the page-count cap is
// reached, or the early-CAN pending threshold trips.
func (s *adaptiveWorkflowState) runOnePagedCycle(ctx workflow.Context) error {
	if s.params.DrainOnly {
		// No listing to do; semaphores were left empty by the constructor
		// so drainRetries can refill cleanly. Nothing to do here.
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
		// Break early when the SDK signals history budget is approaching
		// the warn threshold so the CAN snapshot fits. Without this,
		// PageCountPerExecution (up to 1000) can blow past the warn
		// before any of the pending-list thresholds trip.
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
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

// drainRetries reverifies pending until both lanes are empty, with the
// per-batch deadline scaling each round so the tail gets more time.
// Returns canSuggested=true when GetContinueAsNewSuggested fires
// mid-drain; the caller is expected to CAN with DrainOnly=true so the
// next cycle resumes the drain.
func (s *adaptiveWorkflowState) drainRetries(ctx workflow.Context) (canSuggested bool, err error) {
	for round := 0; len(s.fastPending) > 0 || len(s.slowPending) > 0; round++ {
		// Check before working so we don't burn another round when history
		// budget is already approaching the warn threshold.
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			return true, nil
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

// reclassifyPendingForRetry routes each pending entry by current
// shard-quarantine state. The slow→fast direction matters: without
// it, a workflow whose hosting shard's quarantine has since been
// released would stay pinned in the slow lane through every
// drainRetries round.
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
	for _, ex := range toRetrySlow {
		if s.isQuarantined(ex.BusinessID) {
			reSlow = append(reSlow, ex)
		} else {
			reFast = append(reFast, ex)
		}
	}
	return
}

// retryDeadlinesForRound scales the per-batch deadline by round number
// so the tail gets progressively more time to drain. Capped so a
// pathological run doesn't end up with hour-long deadlines.
func (s *adaptiveWorkflowState) retryDeadlinesForRound(round int) (fastMs, slowMs int64) {
	const maxDeadlineMultiplier = 10
	mult := min(round+2, maxDeadlineMultiplier)
	fastMs = int64(s.params.BatchDeadlineSeconds) * 1000 * int64(mult)
	slowMs = int64(s.params.SlowLaneBatchDeadlineSeconds) * 1000 * int64(mult)
	return
}

// dispatchRetryChunks dispatches verify-only batches over execs in
// chunks of batchSize.
func (s *adaptiveWorkflowState) dispatchRetryChunks(ctx workflow.Context, execs []*ExecutionInfo, slow bool, batchSize int, deadlineMs int64) {
	for i := 0; i < len(execs); i += batchSize {
		end := min(i+batchSize, len(execs))
		s.dispatchVerifyOnly(ctx, execs[i:end], slow, deadlineMs)
	}
}

// slowLaneRetryBatchSize keeps slow-lane retry batches smaller than
// fast-lane so concurrency=1 doesn't pin a single huge batch and
// starve the lane.
func slowLaneRetryBatchSize(fastPageSize int) int {
	return max(fastPageSize/4, 25)
}

// dispatchInjectThenVerify holds the lane's slot across both InjectBatch
// and VerifyBatch — the drip-feed contract: adjacent batches on the same
// lane serialize so the apply pipeline drains between bursts.
func (s *adaptiveWorkflowState) dispatchInjectThenVerify(ctx workflow.Context, execs []*ExecutionInfo, slow bool, targetClusters []string) {
	sem := s.fastSem
	deadlineMs := int64(s.params.BatchDeadlineSeconds) * 1000
	intervalMs := int64(s.params.VerifyIntervalInSeconds) * 1000
	if slow {
		sem = s.slowSem
		deadlineMs = int64(s.params.SlowLaneBatchDeadlineSeconds) * 1000
		intervalMs = int64(s.params.SlowLaneVerifyIntervalSeconds) * 1000
	}
	sem.Receive(ctx, nil)
	// Read the lane's current RPS after the slot is held so the prior
	// batch's AIMD adjustment has already applied.
	rps := s.currentFastRPS
	if slow {
		rps = s.currentSlowRPS
	}
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
			Namespace:      s.params.Namespace,
			NamespaceID:    s.params.NamespaceID,
			TargetClusters: targetClusters,
			Executions:     execs,
			RPS:            rps,
		}
		if err := workflow.ExecuteActivity(injectCtx, a.InjectBatch, injReq).Get(ctx, nil); err != nil {
			// lastActivityErr causes the workflow to fail (no CAN), so
			// don't bother routing pending or adjusting AIMD — neither
			// is observable after a failure return.
			s.lastActivityErr = err
			return
		}
		if !s.params.EnableVerification {
			// Inject-only mode: totalVerified surfaces injected count in
			// the status query. No-progress detector is bypassed in
			// checkProgress since there's no arrival signal.
			s.totalVerified += int64(len(execs))
			s.adjustRPS(ctx, slow, false)
			return
		}
		verReq := &adaptiveVerifyBatchRequest{
			Namespace:             s.params.Namespace,
			NamespaceID:           s.params.NamespaceID,
			TargetClusterEndpoint: s.params.TargetClusterEndpoint,
			TargetClusterName:     s.params.TargetClusterName,
			Executions:            execs,
			DeadlineMs:            deadlineMs,
			VerifyIntervalMs:      intervalMs,
		}
		var resp adaptiveVerifyBatchResponse
		if err := workflow.ExecuteActivity(verifyCtx, a.VerifyBatch, verReq).Get(ctx, &resp); err != nil {
			s.lastActivityErr = err
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
	sem.Receive(ctx, nil)
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
			NamespaceID:           s.params.NamespaceID,
			TargetClusterEndpoint: s.params.TargetClusterEndpoint,
			TargetClusterName:     s.params.TargetClusterName,
			Executions:            execs,
			DeadlineMs:            deadlineMs,
			VerifyIntervalMs:      intervalMs,
		}
		var resp adaptiveVerifyBatchResponse
		if err := workflow.ExecuteActivity(actx, a.VerifyBatch, req).Get(ctx, &resp); err != nil {
			s.lastActivityErr = err
			return
		}
		s.totalVerified += resp.Verified
		s.recordBatchOutcome(ctx, execs, resp.Pending)
		s.adjustRPS(ctx, slow, len(resp.Pending) > 0)
	})
}

// recordBatchOutcome updates the shard pending counter and routes
// pending execs to lanes. Counters move once per pending BusinessID
// per batch (per-batch dedupe) so a single heavy-reuse batch can't
// single-handedly tip a shard over threshold off one runaway BID. On
// batches that produce clean traffic for a shard with zero pending
// BIDs in the same batch, the shard counter decays by 1 — the "no
// pending in this batch" gate is what keeps a burst of cold neighbors
// from drowning out a hot BID's signal.
func (s *adaptiveWorkflowState) recordBatchOutcome(ctx workflow.Context, dispatched, pending []*ExecutionInfo) {
	// Dedupe by BusinessID, preserving first-seen order for replay
	// determinism. The pending/dispatched slices come straight from
	// activity results / inputs and are themselves deterministic.
	pendingBIDs := make(map[string]struct{}, len(pending))
	pendingBIDOrder := make([]string, 0, len(pending))
	for _, ex := range pending {
		if _, seen := pendingBIDs[ex.BusinessID]; seen {
			continue
		}
		pendingBIDs[ex.BusinessID] = struct{}{}
		pendingBIDOrder = append(pendingBIDOrder, ex.BusinessID)
	}

	pendingShards := make(map[int32]struct{}, len(pendingBIDOrder))
	for _, bid := range pendingBIDOrder {
		pendingShards[s.shardOf(bid)] = struct{}{}
	}

	s.incrementPendingCounters(ctx, pendingBIDOrder)
	s.decrementCleanCounters(ctx, dispatched, pendingBIDs, pendingShards)
	s.routePending(pending)
}

// incrementPendingCounters charges each deduped pending BID's shard
// counter and quarantines the shard once it crosses threshold. The
// triggering BID is included in the transition log so per-workflow
// observability ("which workflow tipped shard 3 over?") survives even
// though the quarantine unit is the shard, not the BID.
func (s *adaptiveWorkflowState) incrementPendingCounters(ctx workflow.Context, pendingBIDOrder []string) {
	for _, bid := range pendingBIDOrder {
		sh := s.shardOf(bid)
		if _, alreadyQ := s.quarantinedShard[sh]; alreadyQ {
			continue
		}
		s.shardPending[sh]++
		if s.shardPending[sh] >= s.params.ShardQuarantineThreshold {
			s.quarantinedShard[sh] = struct{}{}
			s.emitTransition(ctx, metrics.ForceReplicationShardQuarantinedCount.Name(),
				"adaptive: quarantined shard",
				"shard", sh,
				"triggerWfId", bid,
				"pendingCount", s.shardPending[sh])
		}
	}
}

// decrementCleanCounters decays shard counters using a per-batch rule:
// decay a shard by 1 if this batch had clean dispatched traffic on it
// AND zero pending BIDs on it. The "no pending in this batch" gate is
// the drown-out protection — one hot BID on a shard with many cold
// neighbors shouldn't be erased by the cold neighbors' clean outcomes
// when the hot BID also showed pending in the same batch.
func (s *adaptiveWorkflowState) decrementCleanCounters(ctx workflow.Context, dispatched []*ExecutionInfo, pendingBIDs map[string]struct{}, pendingShards map[int32]struct{}) {
	shardReleaseAt := max(s.params.ShardQuarantineThreshold/2, 1)
	cleanShards := make(map[int32]struct{})
	seenClean := make(map[string]struct{}, len(dispatched))
	for _, ex := range dispatched {
		if _, isPending := pendingBIDs[ex.BusinessID]; isPending {
			continue
		}
		if _, already := seenClean[ex.BusinessID]; already {
			continue
		}
		seenClean[ex.BusinessID] = struct{}{}
		cleanShards[s.shardOf(ex.BusinessID)] = struct{}{}
	}
	for sh := range cleanShards {
		if _, hadPending := pendingShards[sh]; hadPending {
			continue
		}
		if s.shardPending[sh] > 0 {
			s.shardPending[sh]--
		}
		s.maybeReleaseShardQuarantine(ctx, sh, shardReleaseAt)
	}
}

func (s *adaptiveWorkflowState) maybeReleaseShardQuarantine(ctx workflow.Context, sh int32, releaseAt int) {
	if _, ok := s.quarantinedShard[sh]; !ok || s.shardPending[sh] > releaseAt {
		return
	}
	delete(s.quarantinedShard, sh)
	s.emitTransition(ctx, metrics.ForceReplicationShardRecoveredCount.Name(),
		"adaptive: released shard quarantine",
		"shard", sh,
		"pendingCount", s.shardPending[sh])
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
	_, ok := s.quarantinedShard[s.shardOf(wfID)]
	return ok
}

// shardOf returns the history shard that owns a workflow ID, using the same
// (namespaceID, workflowID) → shard hash production uses everywhere else.
// Matching that mapping is the whole point: shard-quarantine decisions then
// land on the same shard whose apply queue is actually backed up.
func (s *adaptiveWorkflowState) shardOf(wfID string) int32 {
	return common.WorkflowIDToHistoryShard(s.params.NamespaceID, wfID, s.params.HistoryShardCount)
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
	if s.params.AIMDDisabled {
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
	metricName := metrics.ForceReplicationRPSIncreasedCount.Name()
	if hadPending {
		metricName = metrics.ForceReplicationRPSDecreasedCount.Name()
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
	// Inject-only mode has no way to observe arrival on the target, so
	// totalVerified is just the injected count and a no-progress signal
	// from it would fire spuriously.
	if !s.params.EnableVerification {
		return nil
	}
	if s.totalVerified > s.lastVerified {
		s.lastVerified = s.totalVerified
		s.lastProgressAt = workflow.Now(ctx)
		return nil
	}
	noProgress := time.Duration(s.params.NoProgressTimeoutSeconds) * time.Second
	if workflow.Now(ctx).Sub(s.lastProgressAt) > noProgress {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("adaptive force-rep no progress for %v: %d verified, %d fast pending, %d slow pending, %d shards quarantined",
				noProgress, s.totalVerified, len(s.fastPending), len(s.slowPending),
				len(s.quarantinedShard)),
			"AdaptiveForceReplicationNoProgress", nil)
	}
	return nil
}

func (s *adaptiveWorkflowState) drainSemaphores(ctx workflow.Context) {
	for range s.params.ConcurrentActivityCount {
		s.fastSem.Receive(ctx, nil)
	}
	for range s.params.SlowLaneConcurrency {
		s.slowSem.Receive(ctx, nil)
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

// estimateCANPayloadBytes returns a conservative upper bound on the
// marshalled size of the input-growing CAN fields (pending lists only).
// Shard-keyed state is bounded by shard count and excluded.
//
// Over-estimates because it doesn't dedupe BusinessIDs shared across runs
// of the same WF — intentional, since over-CAN'ing is preferable to
// missing the SDK's 512KB blob warn.
func (s *adaptiveWorkflowState) estimateCANPayloadBytes() int {
	// Per-pending-entry overhead in the wire form: tuple brackets,
	// quotes, comma, archetypeID digits, plus the BID grouping key
	// (over-counted for repeated BIDs, intentionally).
	const pendingEntryOverhead = 20

	bytes := 0
	for _, e := range s.fastPending {
		bytes += len(e.BusinessID) + len(e.RunID) + pendingEntryOverhead
	}
	for _, e := range s.slowPending {
		bytes += len(e.BusinessID) + len(e.RunID) + pendingEntryOverhead
	}
	return bytes
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

// Non-nil so callers can write to the result without panicking on nil.
func cloneInt32IntMap(m map[int32]int) map[int32]int {
	out := maps.Clone(m)
	if out == nil {
		out = make(map[int32]int)
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
