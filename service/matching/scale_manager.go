//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination scale_manager_mock.go

package matching

import (
	"context"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/number"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/proto"
)

// scaleManager keeps some state and manages the interaction with partitionScaler.
// scaleManager runs on the root partition only.
//
// All scaler/state work is funneled through a single background goroutine. That
// goroutine owns scaleState/scaleDB/lastDecision and is the only caller of
// partitionScaler, so the scaler implementation can rely on serial calls and
// scaleState needs no lock. AddedTasks talks to the worker only via atomics and
// the wakeup channel, so it never blocks.
type scaleManager struct {
	partition              tqid.Partition
	logger                 log.Logger
	metricsHandler         metrics.Handler
	userDataManager        userDataManager
	matchingClient         matchingservice.MatchingServiceClient
	partitionScaler        PartitionScaler
	batchSize              int64 // fixed at creation time
	settings               dynamicconfig.TypedPropertyFn[dynamicconfig.PartitionScaleManagerSettings]
	getWritePartitions     dynamicconfig.IntPropertyFn
	shouldEmitGaugeMetrics dynamicconfig.BoolPropertyFn
	timeSource             clock.TimeSource
	background             *goro.Handle

	// owned by the worker goroutine after Start starts it
	scaleState       *persistencespb.PartitionScaleState
	scaleDB          scaleDB
	nextDecision     time.Time
	nextShadowLog    time.Time
	prevShadowTarget int32

	// store separately from scaleState to avoid data race
	currentWrite atomic.Int32

	// batch counts estimated tasks across all partitions in between calls to the scaler
	batch  atomic.Int64
	wakeup chan struct{}
}

// scaleDB is used to write scale state to persistence. It's a sub-interface of
// physicalTaskQueueManager (for the default queue).
type scaleDB interface {
	UpdateScaleState(*persistencespb.PartitionScaleState, bool) error
}

func newScaleManager(
	baseCtx context.Context,
	partition tqid.Partition,
	logger log.Logger,
	metricsHandler metrics.Handler,
	userDataManager userDataManager,
	matchingClient matchingservice.MatchingServiceClient,
	partitionScaler PartitionScaler,
	timeSource clock.TimeSource,
	settings dynamicconfig.TypedPropertyFn[dynamicconfig.PartitionScaleManagerSettings],
	getWritePartitions dynamicconfig.IntPropertyFn,
	emitGaugeMetrics dynamicconfig.BoolPropertyFn,
) *scaleManager {
	return &scaleManager{
		partition:              partition,
		logger:                 log.With(logger, tag.ComponentPartitionScaler),
		metricsHandler:         metricsHandler,
		userDataManager:        userDataManager,
		matchingClient:         matchingClient,
		partitionScaler:        partitionScaler,
		batchSize:              int64(settings().BatchSize),
		settings:               settings,
		getWritePartitions:     getWritePartitions,
		shouldEmitGaugeMetrics: emitGaugeMetrics,
		timeSource:             timeSource,
		background:             goro.NewHandle(baseCtx),
		wakeup:                 make(chan struct{}, 1),
	}
}

func (sm *scaleManager) Stop() {
	if sm == nil {
		return
	}
	sm.background.Cancel()
	sm.partitionScaler.Stop()
	// this is unfortunate but at least allows max() across pods to get the right value
	sm.emitGaugeMetricsIfEnabled(-1, -1, -1)
}

// Start is called when the root partitions's default queue has loaded its metadata.
// Must be called at most once.
func (sm *scaleManager) Start(scaleState *persistencespb.PartitionScaleState, scaleDB scaleDB) {
	if sm == nil {
		return
	}
	// backgroundWork can assume sm.scaleDB is set since we set it before starting it.
	sm.scaleDB = scaleDB
	sm.setState(scaleState, sm.settings())
	sm.background.Go(sm.backgroundWork)
}

// AddedTasks records one root sample representing estimated queue-wide task additions.
// This is called in the task add path, so it shouldn't block.
func (sm *scaleManager) AddedTasks(estimatedTasksAllPartitions int) {
	if sm == nil {
		return
	}

	// Wake once ~batchSize tasks per write partition have accumulated. Before the first
	// scaler decision we don't know the write count, so use the per-sample estimate, which
	// scales with partitions the same way.
	threshold := int64(estimatedTasksAllPartitions) * sm.batchSize
	if currentWrite := sm.currentWrite.Load(); currentWrite > 0 {
		threshold = int64(currentWrite) * sm.batchSize
	}
	if sm.batch.Add(int64(estimatedTasksAllPartitions)) < threshold {
		return // not enough for a batch yet
	}

	// non-blocking signal
	select {
	case sm.wakeup <- struct{}{}:
	default:
	}
}

func (sm *scaleManager) backgroundWork(ctx context.Context) error {
	timerCh := func() <-chan time.Time {
		ch, _ := sm.timeSource.NewTimer(backoff.Jitter(sm.settings().BackgroundInterval, 0.05))
		return ch
	}
	ch := timerCh()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-sm.wakeup:
			sm.callScaler()

		case <-ch:
			ch = timerCh()
			// call scaler even if batch == 0, to allow scale down when no tasks are coming in
			sm.callScaler()
			// check child partitions periodically
			sm.updateBacklogAndDrainState(ctx)
		}
	}
}

// callScaler runs the scaler on the accumulated batch and persists the resulting state if it
// changed.
// Called from backgroundWork only.
func (sm *scaleManager) callScaler() {
	// don't bother calling during cooldown period
	if !sm.nextDecision.IsZero() && sm.timeSource.Now().Before(sm.nextDecision) {
		return
	}

	settings := sm.settings()
	shadowMode := !settings.Enabled

	// grab current batch (may be zero)
	tasks := int(sm.batch.Swap(0))

	// Only enabled mode applies what the scaler says, so in shadow mode managed scaling is
	// off: apply a disabled decision (the zero value) before anything else. It's
	// idempotent, breaking cleanly back to the dynamic config baseline the first time and
	// changing nothing after that, and doing it up front is what lets the shadow scaler
	// below observe that baseline rather than leftover managed state.
	if shadowMode {
		sm.applyDecision(PartitionScalerDecision{}, settings, shadowMode)
	}

	decision := sm.partitionScaler.OnTasks(PartitionScalerInput{
		NumTasks:      tasks,
		CurrentTarget: int(sm.scaleState.GetTarget()),
		BacklogCounts: sm.scaleState.GetBacklogCounts(),
		PrivateState:  sm.scaleState.GetPrivateScalerState(),
	})

	if shadowMode {
		sm.observeShadowDecision(decision, settings)
	} else {
		sm.applyDecision(decision, settings, shadowMode)
	}
}

// applyDecision persists decision and pushes it to ephemeral data, if it changes anything.
// Called from callScaler only.
func (sm *scaleManager) applyDecision(
	decision PartitionScalerDecision,
	settings dynamicconfig.PartitionScaleManagerSettings,
	shadowMode bool,
) {
	backlogCapC8 := number.EncodeCompact8(int64(decision.BacklogCap))
	disabledStateNeedsCleanup := decision.NewTarget == 0 && hasManagedStateBesidesTarget(sm.scaleState)
	if decision.NoChange ||
		decision.NewTarget == int(sm.scaleState.GetTarget()) &&
			backlogCapC8 == number.Compact8(sm.scaleState.GetBacklogCap()) &&
			!disabledStateNeedsCleanup {
		return
	}

	target := int32(decision.NewTarget)

	newState := common.CloneProto(sm.scaleState)
	if newState == nil {
		newState = &persistencespb.PartitionScaleState{}
	}
	prevTarget := newState.Target
	newState.Target = target
	newState.MaxTarget = max(newState.MaxTarget, target)
	newState.TargetVersion = sm.timeSource.Now().UnixNano()
	newState.BacklogCap = int32(backlogCapC8)
	newState.PrivateScalerState = decision.PrivateState
	var prevRead, prevWrite int32
	if target == 0 {
		// Disabling managed scaling is a clean break to dynamic config; any backlog
		// outside its read range remains unpolled until it times out.
		prevInfo := scaleStateToInfo(sm.scaleState, settings)
		prevRead, prevWrite = prevInfo.Read, prevInfo.Write
		newState.BacklogState = nil
		newState.BacklogCounts = nil
		newState.BacklogCap = 0
		newState.PrivateScalerState = nil
	}

	mayHaveBacklog := target
	if prevTarget == 0 && target > 0 {
		// Turning on managed partition scaling: consider all partitions from dynamic
		// config as having backlog also.
		mayHaveBacklog = max(mayHaveBacklog, int32(sm.getWritePartitions()))
	}
	for i := range mayHaveBacklog {
		newState.BacklogState = bitSet(newState.BacklogState).set(i)
	}

	// we must successfully write to the db before making new state active
	if err := sm.scaleDB.UpdateScaleState(newState, true); err != nil {
		sm.logger.Error("failed to update state", tag.Error(err), tag.Operation("scale"))
		return
	}

	sm.setState(newState, settings) // emits partition_scale_{read,write,target}

	cooldown := time.Duration(float32(time.Second) / settings.MaxRate)
	sm.nextDecision = sm.timeSource.Now().Add(cooldown)

	if target == 0 {
		sm.logger.Info("disabled managed scaling",
			tag.Int32("prev-read", prevRead),
			tag.Int32("prev-write", prevWrite),
			tag.Bool(metrics.ScalerShadowModeTagName, shadowMode))
	} else {
		sm.logger.Info("new target",
			tag.Int32("target", target),
			tag.Int32("prev-target", prevTarget),
			tag.Int32("max-target", newState.MaxTarget),
			tag.Bool(metrics.ScalerShadowModeTagName, false))
	}
	if !shadowMode {
		// in shadow mode, observeShadowDecision emits the per-call event instead
		metrics.PartitionScaleEvents.With(sm.metricsHandler.WithTags(metrics.ScalerShadowModeTag(false))).Record(1)
	}
}

// observeShadowDecision logs and emits metrics for a decision the scaler would have made,
// without applying any of it. It's rate limited to one log per ShadowModeLogInterval, and
// only logs when the hypothetical target changes, to keep the volume down.
// Called from callScaler only, in shadow mode.
func (sm *scaleManager) observeShadowDecision(
	decision PartitionScalerDecision,
	settings dynamicconfig.PartitionScaleManagerSettings,
) {
	target := int32(decision.NewTarget)
	if settings.ShadowModeLogInterval <= 0 || // no logging
		decision.NoChange || // scaler has nothing to say yet
		target <= 0 || // only log if scaler is enabled
		sm.prevShadowTarget == target || // only log new changes
		sm.timeSource.Now().Before(sm.nextShadowLog) { // too early
		// emit scale event metric as a heartbeat even if no shadow log
		metrics.PartitionScaleEvents.With(sm.metricsHandler.WithTags(metrics.ScalerShadowModeTag(true))).Record(1)
		return
	}

	// Untagged: read == write == 0 marks this as a shadow target rather than an applied one.
	// Emit only when the target decision changed (like in real mode).
	sm.emitGaugeMetricsIfEnabled(0, 0, float64(target))
	sm.nextShadowLog = sm.timeSource.Now().Add(settings.ShadowModeLogInterval)
	sm.prevShadowTarget = target
	// A logged shadow decision starts the cooldown, just as an applied one does, so that
	// MaxRate limits the simulation at the same rate it would limit the real thing.
	sm.nextDecision = sm.timeSource.Now().Add(time.Duration(float32(time.Second) / settings.MaxRate))

	// same message as an applied decision, distinguished by the shadow mode tag
	sm.logger.Info("new target",
		tag.Int32("target", target),
		tag.Bool(metrics.ScalerShadowModeTagName, true))
	metrics.PartitionScaleEvents.With(sm.metricsHandler.WithTags(metrics.ScalerShadowModeTag(true))).Record(1)
}

func (sm *scaleManager) emitGaugeMetricsIfEnabled(read, write, target float64) {
	if sm.shouldEmitGaugeMetrics() {
		metrics.PartitionScaleRead.With(sm.metricsHandler).Record(read)
		metrics.PartitionScaleWrite.With(sm.metricsHandler).Record(write)
		metrics.PartitionScaleTarget.With(sm.metricsHandler).Record(target)
	}
}

// setState updates the current scale state and syncs it to ephemeral data.
// This should only be called _after_ the state is persisted to the db.
// Called from backgroundWork or LoadedMetadata only.
func (sm *scaleManager) setState(newState *persistencespb.PartitionScaleState, settings dynamicconfig.PartitionScaleManagerSettings) {
	prevInfo := scaleStateToInfo(sm.scaleState, settings)

	sm.scaleState = newState

	newInfo := scaleStateToInfo(sm.scaleState, settings)
	sm.currentWrite.Store(newInfo.GetWrite())

	// only push ephemeral data if _info_ changed, not on any state change
	if !proto.Equal(prevInfo, newInfo) {
		sm.userDataManager.SetPartitionScale(newInfo)
	}

	sm.emitGaugeMetricsIfEnabled(float64(newInfo.Read), float64(newInfo.Write), float64(sm.scaleState.GetTarget()))
}

func (sm *scaleManager) versionsForDescribe() ([]string, error) {
	// Get all the versions that this task queue has ever been a part of,
	// they could have backlog even if not loaded, so we need to check them.
	// Exclude drained versions because they definitely have no backlog.
	//
	// Also exclude deleted versions, even if they are not drained; if a
	// non-drained version is deleted, the user killed all pollers and then
	// force-deleted the version, explicitly abandoning it. The backlog can't
	// be consumed by any existing pollers, so it should not prevent the partition
	// from draining.
	// If the user re-creates the version and the partition count does not scale
	// back up to reach this backlog, the tasks will time out. That is reasonable
	// given the force-delete, and better than the alternative of blocking partition
	// scale down indefinitely.
	userData, _, err := sm.userDataManager.GetUserData()
	if err != nil {
		return nil, err
	}
	perType := userData.GetData().GetPerType()[int32(sm.partition.TaskType())]
	versionsSet := make(map[string]struct{})
	//nolint:staticcheck // SA1019: old deployment data remains supported during migration
	for _, versionData := range perType.GetDeploymentData().GetVersions() {
		version := versionData.GetVersion()
		if version.GetDeploymentName() == "" || version.GetBuildId() == "" || // legacy version data may have version=nil for unversioned
			versionData.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED {
			continue
		}
		versionsSet[worker_versioning.WorkerDeploymentVersionToStringV32(version)] = struct{}{}
	}

	for deploymentName, deploymentData := range perType.GetDeploymentData().GetDeploymentsData() {
		for buildID, versionData := range deploymentData.GetVersions() {
			if versionData.GetDeleted() || versionData.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED {
				continue
			}
			versionsSet[worker_versioning.BuildIDToStringV32(deploymentName, buildID)] = struct{}{}
		}
	}
	versions := make([]string, 0, len(versionsSet))
	for version := range versionsSet {
		versions = append(versions, version)
	}
	return versions, nil
}

func (sm *scaleManager) describeRequest(id int32, versions []string) *matchingservice.DescribeTaskQueuePartitionRequest {
	return &matchingservice.DescribeTaskQueuePartitionRequest{
		NamespaceId: sm.partition.NamespaceId(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     sm.partition.TaskQueue().Name(),
			TaskQueueType: sm.partition.TaskType(),
			PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: id},
		},
		Versions: &taskqueuepb.TaskQueueVersionSelection{
			BuildIds: versions,
			// this is the default queue, equivalent to putting "" in the BuildIds list
			Unversioned: true,
			// versions list should already contain all the loaded per-version queues, we
			// include AllActive too because it doesn't hurt and would cover recently-added
			// versions in case the user data is stale for some reason
			AllActive: true,
		},
		ReportInternalTaskQueueStatus: true,
		OnlyIfLoaded:                  true,
	}
}

func (sm *scaleManager) updateBacklogAndDrainState(ctx context.Context) {
	if !sm.settings().Enabled {
		// if we're not enabled, we don't have to do any of this
		return
	}

	scaleState := sm.scaleState
	read := scaleStateToReadCount(scaleState)
	if read == 0 {
		return
	}
	versions, err := sm.versionsForDescribe()
	if err != nil {
		return
	}

	prevBacklog := scaleState.GetBacklogCounts()
	newBacklog := make([]byte, read)
	// Preserve the last known backlog for partitions whose Describe call fails.
	copy(newBacklog, prevBacklog)
	backlogChanged := false

	// check if we should evaluate drain state
	settings := sm.settings()
	target := scaleState.GetTarget()
	checkDrain := target > 0 &&
		sm.timeSource.Since(time.Unix(0, scaleState.GetTargetVersion())) >= settings.DrainBufferTime
	info := scaleStateToInfo(scaleState, settings)
	var toClear []int32

	for id := range read {
		callCtx, cancel := context.WithTimeout(ctx, ioTimeout)
		res, err := sm.matchingClient.DescribeTaskQueuePartition(callCtx, sm.describeRequest(id, versions))
		cancel()
		if err != nil {
			// CONSIDER(carlydf): Emit a metric when an unloaded partition in the draining range blocks scale-down.
			continue
		}

		// update backlog count
		total := totalBacklogFromDescribeResponse(res)
		var prev number.Compact8
		if id < int32(len(prevBacklog)) {
			prev = prevBacklog[id]
		}
		newBacklog[id] = number.UpdateCompact8(total, prev)
		backlogChanged = backlogChanged || newBacklog[id] != prev

		// check drain state for partitions in the draining range
		if checkDrain &&
			id >= target &&
			bitSet(scaleState.BacklogState).get(id) &&
			partitionIsFullyDrained(res, info) {
			toClear = append(toClear, id)
		}
	}

	if !backlogChanged && len(toClear) == 0 {
		return
	}

	newState := common.CloneProto(scaleState)
	if newState == nil {
		newState = &persistencespb.PartitionScaleState{}
	}
	newState.BacklogCounts = newBacklog
	for _, i := range toClear {
		newState.BacklogState = bitSet(newState.BacklogState).clear(i)
	}

	// sync to DB only when drain bits changed (must be persisted before taking effect).
	// for backlog-count-only updates, update in-memory state only (will be persisted
	// periodically).
	needSync := len(toClear) > 0
	if err := sm.scaleDB.UpdateScaleState(newState, needSync); err != nil {
		sm.logger.Error("failed to update state", tag.Error(err), tag.Operation("drain"))
		return
	}

	if len(toClear) > 0 {
		sm.logger.Info("drain",
			tag.Any("drained-partitions", toClear),
			tag.Int32("target", info.Write),
			tag.Int32("prev-read", info.Read),
			tag.Int32("read", bitSet(newState.BacklogState).len()))
	}

	sm.setState(newState, settings)
}

func partitionIsFullyDrained(
	res *matchingservice.DescribeTaskQueuePartitionResponse,
	info *taskqueuespb.PartitionScaleInfo,
) bool {
	// Require that the partition agrees with the current scale state, i.e. it knows that
	// it's draining, i.e. it knows it can't accept any new tasks. We include the version
	// as well as just the read+write counts to avoid an ABA problem.
	resInfo := res.GetScaleInfo()
	if resInfo == nil ||
		resInfo.Version != info.Version ||
		resInfo.Read != info.Read ||
		resInfo.Write != info.Write {
		return false
	}

	for _, v := range res.GetVersionsInfoInternal() {
		for _, q := range v.GetPhysicalTaskQueueInfo().GetInternalTaskQueueStatus() {
			if !q.GetBacklogDrained() {
				return false
			}
		}
	}
	return true
}

func totalBacklogFromDescribeResponse(res *matchingservice.DescribeTaskQueuePartitionResponse) (total int64) {
	for _, v := range res.GetVersionsInfoInternal() {
		for _, q := range v.GetPhysicalTaskQueueInfo().GetInternalTaskQueueStatus() {
			total += q.GetApproximateBacklogCount()
		}
	}
	return
}

// hasManagedStateBesidesTarget reports whether scaleState holds managed scaling state
// other than Target, i.e. whether zeroing Target alone would leave something behind that
// still drives read partitions or the scaler.
func hasManagedStateBesidesTarget(scaleState *persistencespb.PartitionScaleState) bool {
	return len(scaleState.GetBacklogState()) > 0 ||
		len(scaleState.GetBacklogCounts()) > 0 ||
		scaleState.GetBacklogCap() != 0 ||
		scaleState.GetPrivateScalerState() != nil
}

func scaleStateToReadCount(scaleState *persistencespb.PartitionScaleState) int32 {
	return max(scaleState.GetTarget(), bitSet(scaleState.GetBacklogState()).len())
}

func scaleStateToInfo(
	scaleState *persistencespb.PartitionScaleState,
	settings dynamicconfig.PartitionScaleManagerSettings,
) *taskqueuespb.PartitionScaleInfo {
	// note if scaleState == nil, read and write will both be 0
	read := scaleStateToReadCount(scaleState)
	allowedShrink := max(
		1,
		min(
			int32(float32(read)*settings.ShrinkRatio),
			settings.ShrinkDelta,
		),
	)
	write := max(
		scaleState.GetTarget(),
		read-allowedShrink,
	)
	return &taskqueuespb.PartitionScaleInfo{
		Read:          read,
		Write:         write,
		BacklogCounts: scaleState.GetBacklogCounts(),
		BacklogCap:    scaleState.GetBacklogCap(),
		Version:       scaleState.GetTargetVersion(),
	}
}
