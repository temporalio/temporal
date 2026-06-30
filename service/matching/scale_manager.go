//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination scale_manager_mock.go

package matching

import (
	"context"
	"sync/atomic"
	"time"

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
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/proto"
)

// scaleManager keeps some state and manages the interaction with partitionScaler.
// scaleManager runs on the root partition only.
//
// All scaler/state work is funneled through a single background goroutine. That
// goroutine owns scaleState/scaleDB/lastDecision and is the only caller of
// partitionScaler, so the scaler implementation can rely on serial calls and
// scaleState needs no lock. AddedTasks talks to the worker only via the atomic
// batch counter and the wakeup channel, so it never blocks.
type scaleManager struct {
	partition          tqid.Partition
	logger             log.Logger
	metricsHandler     metrics.Handler
	userDataManager    userDataManager
	matchingClient     matchingservice.MatchingServiceClient
	partitionScaler    PartitionScaler
	batchSize          int64 // fixed at creation time
	settings           dynamicconfig.TypedPropertyFn[dynamicconfig.PartitionScaleManagerSettings]
	getWritePartitions dynamicconfig.IntPropertyFn
	emitGaugeMetrics   dynamicconfig.BoolPropertyFn
	timeSource         clock.TimeSource
	background         *goro.Handle

	// owned by the worker goroutine after Start starts it
	scaleState       *persistencespb.PartitionScaleState
	scaleDB          scaleDB
	nextDecision     time.Time
	nextShadowLog    time.Time
	prevShadowTarget int32

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
		partition:          partition,
		logger:             log.With(logger, tag.ComponentPartitionScaler),
		metricsHandler:     metricsHandler,
		userDataManager:    userDataManager,
		matchingClient:     matchingClient,
		partitionScaler:    partitionScaler,
		batchSize:          int64(settings().BatchSize),
		settings:           settings,
		getWritePartitions: getWritePartitions,
		emitGaugeMetrics:   emitGaugeMetrics,
		timeSource:         timeSource,
		background:         goro.NewHandle(baseCtx),
		wakeup:             make(chan struct{}, 1),
	}
}

func (sm *scaleManager) Stop() {
	if sm == nil {
		return
	}
	sm.background.Cancel()
	sm.partitionScaler.Stop()
	if sm.emitGaugeMetrics() {
		// this is unfortunate but at least allows max() across pods to get the right value
		metrics.PartitionScaleRead.With(sm.metricsHandler).Record(float64(-1))
		metrics.PartitionScaleWrite.With(sm.metricsHandler).Record(float64(-1))
	}
}

// Start is called when the root partitions's default queue has loaded its metadata.
// Must be called at most once.
func (sm *scaleManager) Start(scaleState *persistencespb.PartitionScaleState, scaleDB scaleDB) {
	if sm == nil {
		return
	}
	// backgroundWork can assume sm.scaleDB is set since we set it before starting it.
	sm.scaleDB = scaleDB
	sm.setState(scaleState)
	sm.background.Go(sm.backgroundWork)
}

// AddedTasks is called on a batch of tasks added.
// This is called in the task add path, so it shouldn't block.
func (sm *scaleManager) AddedTasks(numTasks int) {
	if sm == nil {
		return
	}

	// scale target batch size by numTasks (since numTasks is scaled by partitions)
	batchSize := int64(numTasks) * sm.batchSize
	if sm.batch.Add(int64(numTasks)) < batchSize {
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
			sm.updateDrainState(ctx)
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
	shadowMode := settings.ShadowModeLogInterval > 0

	// Entering shadow mode on top of a previously-applied managed target releases
	// control back to the dynamic-config baseline: zero the managed target once so
	// the write side follows dynamic config again (and tracks future config
	// changes), and cold-start the shadow simulation from the baseline. BacklogState
	// is preserved, so read partitions are not dropped here (reclaiming them is left
	// to the drain path). This is the only state shadow mode writes; it still never
	// applies the scaler's hypothetical decisions.
	if shadowMode && sm.scaleState.GetTarget() != 0 {
		sm.releaseManagedState()
	}

	// grab current batch (may be zero)
	tasks := int(sm.batch.Swap(0))

	decision := sm.partitionScaler.OnTasks(PartitionScalerInput{
		NumTasks:      tasks,
		CurrentTarget: int(sm.scaleState.GetTarget()),
		PrivateState:  sm.scaleState.GetPrivateScalerState(),
	})
	if decision.NoChange ||
		decision.NewTarget == int(sm.scaleState.GetTarget()) {
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
	newState.PrivateScalerState = decision.PrivateState

	mayHaveBacklog := target
	if prevTarget == 0 {
		// Turning on managed partition scaling: consider all partitions from dynamic
		// config as having backlog also.
		mayHaveBacklog = max(mayHaveBacklog, int32(sm.getWritePartitions()))
	}
	for i := range mayHaveBacklog {
		newState.BacklogState = bitSet(newState.BacklogState).set(i)
	}

	if shadowMode {
		if sm.timeSource.Now().Before(sm.nextShadowLog) || // too early
			sm.prevShadowTarget == target || // only log new changes
			target <= 0 { // only log if scaler is enabled
			// emit scale event metric as a heartbeat even if no shadow log
			metrics.PartitionScaleEvents.With(sm.metricsHandler.
				WithTags(metrics.ScalerShadowModeTag(shadowMode))).Record(1)
			return
		}
		sm.nextShadowLog = sm.timeSource.Now().Add(settings.ShadowModeLogInterval)
		sm.prevShadowTarget = target
	} else {
		// we must successfully write to the db before making new state active
		if err := sm.scaleDB.UpdateScaleState(newState, true); err != nil {
			sm.logger.Error("failed to update state", tag.Error(err), tag.Operation("scale"))
			return
		}

		sm.setState(newState)
	}

	cooldown := time.Duration(float32(time.Second) / settings.MaxRate)
	sm.nextDecision = sm.timeSource.Now().Add(cooldown)

	sm.logger.Info("new target",
		tag.Int32("target", target),
		tag.Int32("prev-target", prevTarget),
		tag.Int32("max-target", newState.MaxTarget),
		tag.Bool(metrics.ScalerShadowModeTagName, shadowMode))
	metrics.PartitionScaleEvents.With(sm.metricsHandler.
		WithTags(metrics.ScalerShadowModeTag(shadowMode))).Record(1)
}

// releaseManagedState relinquishes a previously-applied managed scale target back
// to the dynamic-config baseline by zeroing Target and PrivateScalerState. With
// Target == 0 the write side falls back to dynamic config (PartitionScaleInfo.Write
// is 0), and the scaler simulates from a cold start on subsequent calls.
// BacklogState is preserved, so read partitions are unchanged until drained.
// Called from callScaler only, in shadow mode, once, when a managed target exists.
func (sm *scaleManager) releaseManagedState() {
	newState := common.CloneProto(sm.scaleState)
	if newState == nil {
		return
	}
	prevTarget := newState.Target
	newState.Target = 0
	newState.PrivateScalerState = nil
	newState.TargetVersion = sm.timeSource.Now().UnixNano()

	// we must successfully write to the db before making new state active
	if err := sm.scaleDB.UpdateScaleState(newState, true); err != nil {
		sm.logger.Error("failed to update state", tag.Error(err), tag.Operation("release"))
		return
	}
	sm.setState(newState)

	sm.logger.Info("released managed scale state to baseline",
		tag.Int32("prev-target", prevTarget),
		tag.Bool(metrics.ScalerShadowModeTagName, true))
}

// setState updates the current scale state and syncs it to ephemeral data.
// This should only be called _after_ the state is persisted to the db.
// Called from backgroundWork or LoadedMetadata only.
func (sm *scaleManager) setState(newState *persistencespb.PartitionScaleState) {
	prevInfo := scaleStateToInfo(sm.scaleState)

	sm.scaleState = newState

	newInfo := scaleStateToInfo(sm.scaleState)

	// only push ephemeral data if _info_ changed, not on any state change
	if !proto.Equal(prevInfo, newInfo) {
		sm.userDataManager.SetPartitionScale(newInfo)
	}

	if sm.emitGaugeMetrics() {
		metrics.PartitionScaleRead.With(sm.metricsHandler).Record(float64(newInfo.Read))
		metrics.PartitionScaleWrite.With(sm.metricsHandler).Record(float64(newInfo.Write))
	}
}

func (sm *scaleManager) describeRequest(id int32) *matchingservice.DescribeTaskQueuePartitionRequest {
	return &matchingservice.DescribeTaskQueuePartitionRequest{
		NamespaceId: sm.partition.NamespaceId(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     sm.partition.TaskQueue().Name(),
			TaskQueueType: sm.partition.TaskType(),
			PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: id},
		},
		Versions: &taskqueuepb.TaskQueueVersionSelection{
			Unversioned: true,
			// TODO(dp)(carlydf): deal with inactive/deleted versions (use user data version info)
			AllActive: true,
		},
		ReportInternalTaskQueueStatus: true,
	}
}

func (sm *scaleManager) updateDrainState(ctx context.Context) {
	scaleState := sm.scaleState
	read := scaleStateToReadCount(scaleState)
	if read == 0 {
		return
	}

	// check if we should evaluate drain state
	settings := sm.settings()
	target := scaleState.GetTarget()
	checkDrain := target > 0 &&
		sm.timeSource.Since(time.Unix(0, scaleState.GetTargetVersion())) >= settings.DrainBufferTime
	info := scaleStateToInfo(scaleState)
	var toClear []int32

	for id := range read {
		// note: right now, this call is useless if checkDrain is false, or for partitions < write.
		// later we'll need these calls to update backlog and other stats, so we just do it always
		// now to simplify the future diff.
		callCtx, cancel := context.WithTimeout(ctx, ioTimeout)
		res, err := sm.matchingClient.DescribeTaskQueuePartition(callCtx, sm.describeRequest(id))
		cancel()
		if err != nil {
			continue
		}

		// check drain state for partitions in the draining range
		if checkDrain &&
			id >= target &&
			bitSet(scaleState.BacklogState).get(id) &&
			partitionIsFullyDrained(res, info) {
			toClear = append(toClear, id)
		}
	}

	if len(toClear) == 0 {
		return
	}

	// Reachable only in the brief window after shadow mode is enabled but before
	// releaseManagedState has zeroed a leftover target>0 (callScaler is still in
	// cooldown). Shadow mode must not persist drain completion or mutate read
	// partitions, so bail before applying toClear.
	if settings.ShadowModeLogInterval > 0 {
		return
	}

	newState := common.CloneProto(scaleState)
	if newState == nil {
		newState = &persistencespb.PartitionScaleState{}
	}
	for _, i := range toClear {
		newState.BacklogState = bitSet(newState.BacklogState).clear(i)
	}

	// sync to db (must be persisted before taking effect)
	if err := sm.scaleDB.UpdateScaleState(newState, true); err != nil {
		sm.logger.Error("failed to update state", tag.Error(err), tag.Operation("drain"))
		return
	}

	sm.logger.Info("drain",
		tag.Any("drained-partitions", toClear),
		tag.Int32("target", info.Write),
		tag.Int32("prev-read", info.Read),
		tag.Int32("read", bitSet(newState.BacklogState).len()))

	sm.setState(newState)
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

func scaleStateToReadCount(scaleState *persistencespb.PartitionScaleState) int32 {
	return max(scaleState.GetTarget(), bitSet(scaleState.GetBacklogState()).len())
}

func scaleStateToInfo(scaleState *persistencespb.PartitionScaleState) *taskqueuespb.PartitionScaleInfo {
	// note if scaleState == nil, read and write will both be 0
	return &taskqueuespb.PartitionScaleInfo{
		Read:    scaleStateToReadCount(scaleState),
		Write:   scaleState.GetTarget(),
		Version: scaleState.GetTargetVersion(),
	}
}
