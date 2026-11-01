package replication

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	// laneMinRetryAfter keeps a limiter that refuses with no delay from spinning the sender.
	laneMinRetryAfter = 10 * time.Millisecond
	// laneMaxRetryAfter makes a refused lane re-check within a second whether it is still
	// paced, so that a handover or retirement takes effect promptly.
	laneMaxRetryAfter = time.Second
)

// laneSendTarget directs sendTasks to send on a lane, or to filter the default lane.
type laneSendTarget struct {
	// laneInfo is set on every message. Nil sends on the default lane.
	laneInfo *replicationspb.ReplicationLaneInfo
	// accept reports whether a task is sent. Nil accepts every task.
	accept func(tasks.Task) bool
	// laneOwner, on the default lane, returns the key of the lane that owns a task, which the
	// default lane then skips, or "".
	laneOwner func(tasks.Task) string
	// rateLimit waits for permission to send one task. Nil does not limit.
	rateLimit func(ctx context.Context) error
}

func (t *laneSendTarget) getLaneInfo() *replicationspb.ReplicationLaneInfo {
	if t == nil {
		return nil
	}
	return t.laneInfo
}

func (t *laneSendTarget) accepts(task tasks.Task) bool {
	return t == nil || t.accept == nil || t.accept(task)
}

// skippingOwner returns the lane that owns a task the default lane skips, or "".
func (t *laneSendTarget) skippingOwner(task tasks.Task) string {
	if t == nil || t.laneOwner == nil {
		return ""
	}
	return t.laneOwner(task)
}

// startLanePolicy creates the stream's lane policy. Without one, the stream retires its lanes.
func (s *laneStreamSender) startLanePolicy() {
	policy, err := s.lanePolicyFactory.NewStreamPolicy(LaneStream{
		RemoteCluster: s.clientClusterName,
		ShardID:       s.serverShardKey.ShardID,
	})
	if err != nil {
		s.logger.Error("Unable to create the replication lane policy. The stream runs without lanes.", tag.Error(err))
		return
	}
	s.lanePolicyMu.Lock()
	defer s.lanePolicyMu.Unlock()
	s.lanePolicy = policy
}

func (s *laneStreamSender) closeLanePolicy() {
	s.lanePolicyMu.Lock()
	defer s.lanePolicyMu.Unlock()
	if s.lanePolicy != nil && !s.lanePolicyClosed {
		s.lanePolicy.Close()
		s.lanePolicyClosed = true
	}
}

// withLanePolicy calls f with the stream's lane policy, unless the stream has none or has
// closed it.
func (s *laneStreamSender) withLanePolicy(f func(LaneStreamPolicy)) {
	s.lanePolicyMu.Lock()
	defer s.lanePolicyMu.Unlock()
	if s.lanePolicy != nil && !s.lanePolicyClosed {
		f(s.lanePolicy)
	}
}

// initLaneRegistry creates the lane registry, restoring the lanes persisted for this
// reader. Persisted lanes that cannot be restored are re-sent on the default lane from
// the persisted HIGH scope.
func (s *laneStreamSender) initLaneRegistry() {
	readerState := s.persistedReaderState()
	var persistedDefaultCursor int64
	var persistedLanes []*persistencespb.QueueReaderLane
	if readerState != nil && len(readerState.GetScopes()) > laneReaderHighScopeIndex {
		persistedDefaultCursor = laneDefaultCursor(readerState)
		persistedLanes = readerState.GetLanes()
		if len(persistedLanes) > 0 {
			s.hasPersistedLanes = true
			s.laneRecoveryFloor = readerState.GetScopes()[laneReaderHighScopeIndex].GetRange().GetInclusiveMin().GetTaskId()
		}
	}
	registry, err := newSenderLaneRegistry(persistedDefaultCursor, persistedLanes)
	if err != nil {
		s.logger.Error("Unable to restore persisted replication lanes. Re-sending their tasks on the default lane.", tag.Error(err))
		s.laneRestoreIncomplete = true
		registry, _ = newSenderLaneRegistry(persistedDefaultCursor, nil)
	}
	s.laneRegistry = registry
}

func (s *laneStreamSender) persistedReaderState() *persistencespb.QueueReaderState {
	queueState, ok := s.shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		return nil
	}
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	return queueState.ReaderStates[readerID]
}

// laneCatchupBeginWatermark returns where the default HIGH lane starts its catch-up scan
// when the stream has a lane registry. It must only be called after the receiver's first
// ack has been applied.
func (s *laneStreamSender) laneCatchupBeginWatermark(end int64) int64 {
	readerState := s.persistedReaderState()
	if readerState == nil || len(readerState.GetScopes()) <= laneReaderHighScopeIndex {
		return s.catchupBeginWatermark(enumsspb.TASK_PRIORITY_HIGH, end)
	}
	begin := laneDefaultCursor(readerState)
	if s.hasPersistedLanes && (!s.lanesConfirmed.Load() || s.laneRestoreIncomplete) {
		// The persisted lanes are not continued, so the default lane re-sends their tasks.
		begin = min(begin, s.laneRecoveryFloor)
	}
	return begin
}

// sendLaneCatchUpPrime sends an empty HIGH batch so the receiver acks, and waits until
// that first ack has been applied. The batch does not move the receiver's HIGH watermark
// past any persisted lane's tasks.
func (s *laneStreamSender) sendLaneCatchUpPrime(end int64) error {
	prime := s.catchupBeginWatermark(enumsspb.TASK_PRIORITY_HIGH, end)
	if readerState := s.persistedReaderState(); readerState != nil && len(readerState.GetScopes()) > laneReaderHighScopeIndex {
		prime = min(prime, laneDefaultCursor(readerState))
	}
	if s.hasPersistedLanes {
		prime = min(prime, s.laneRecoveryFloor)
	}
	if err := s.sendTasks(enumsspb.TASK_PRIORITY_HIGH, prime, prime, nil); err != nil {
		return err
	}
	select {
	case <-s.initialLaneState:
		return nil
	case <-s.shutdownChan.Channel():
		return context.Canceled
	}
}

// recvLaneSyncReplicationState applies a receiver ack when the stream has a lane registry.
func (s *laneStreamSender) recvLaneSyncReplicationState(attr *replicationspb.SyncReplicationState) error {
	if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
		return NewStreamError("streamSender encountered unsupported SyncReplicationState", nil)
	}
	supported := attr.GetLaneProtocolVersion() >= replicationLaneProtocolVersion
	first := false
	s.initialLaneStateOnce.Do(func() {
		s.lanesConfirmed.Store(supported)
		first = true
	})
	if s.lanesConfirmed.Load() != supported {
		return NewStreamError("StreamSender detected replication lane support change", nil)
	}
	s.flowController.RefreshReceiverFlowControlInfo(attr)
	if supported {
		s.laneRegistry.ObserveAcks(attr.GetLaneStates())
		if err := s.reconcileLanes(attr, first); err != nil {
			return err
		}
		if err := s.sendReadyLaneRetirements(); err != nil {
			return err
		}
	} else {
		s.laneRegistry.ClearLanes()
	}
	if first {
		close(s.initialLaneState)
	}

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	if err := s.shardContext.UpdateReplicationQueueReaderState(readerID, s.laneRegistry.BuildReaderState(attr)); err != nil {
		return err
	}
	taskID, ts := s.laneFailoverWatermark(attr)
	return s.shardContext.UpdateRemoteReaderInfo(readerID, taskID, ts)
}

// reconcileLanes creates the lanes the policy asks for and requests retirement of the others.
// A lane that is no longer requested sends without a rate limit until it retires. On the
// stream's first ack, it also drops the restored lanes beyond the receiver's lane limit.
func (s *laneStreamSender) reconcileLanes(attr *replicationspb.SyncReplicationState, first bool) error {
	signals := s.laneSignals(attr)
	desired, err := s.evaluateLanes(signals)
	if err != nil {
		return err
	}
	if first {
		s.dropRestoredLanesOverLimit(desired, signals.ReceiverMaxLanes)
	}
	desired = s.admitDesiredLanes(desired, signals.ReceiverMaxLanes)
	requested := make(map[string]struct{}, len(desired))
	for _, lane := range desired {
		requested[lane.Key] = struct{}{}
		if _, ok := s.laneRegistry.SnapshotByKey(lane.Key); ok {
			s.laneRegistry.CancelRetirement(lane.Key)
			s.laneRegistry.SetPolicyState(lane.Key, lane.PolicyState)
			continue
		}
		if _, _, err := s.laneRegistry.Create(lane.Key, lane.Filter, signals.DefaultAcked, lane.PolicyState); err != nil {
			return err
		}
	}
	s.setRequestedLanes(requested)
	for _, lane := range s.laneRegistry.Snapshots() {
		if _, ok := requested[lane.logicalKey]; !ok {
			s.laneRegistry.RequestRetirement(lane.logicalKey)
		}
	}
	return nil
}

// evaluateLanes returns the policy's answer for signals. When the policy fails, it reacts as
// the error asks, by default keeping the last answer for ReplicationLaneEvaluateErrorGrace. It
// returns an error only to restart the stream.
func (s *laneStreamSender) evaluateLanes(signals LaneSignals) ([]DesiredLane, error) {
	if s.lanePolicyDisabled {
		return nil, nil
	}
	var desired []DesiredLane
	var err error
	s.withLanePolicy(func(policy LaneStreamPolicy) {
		desired, err = policy.Evaluate(signals)
	})
	if err == nil {
		s.lastGoodLanes, s.hasLastGoodLanes, s.lanePolicyFailingSince = desired, true, time.Time{}
		return desired, nil
	}
	if s.lanePolicyFailingSince.IsZero() {
		s.lanePolicyFailingSince = signals.Now
	}
	action, grace := LaneErrorKeepLastGood, s.config.ReplicationLaneEvaluateErrorGrace()
	if evaluateErr, ok := errors.AsType[*LaneEvaluateError](err); ok {
		action, grace = evaluateErr.Action, cmp.Or(max(evaluateErr.Grace, 0), grace)
	}
	logger := s.shardContext.GetThrottledLogger()
	switch action {
	case LaneErrorKeepLastGood:
		if s.hasLastGoodLanes && signals.Now.Sub(s.lanePolicyFailingSince) < grace {
			logger.Warn("Replication lane policy failed. Keeping its last answer.", tag.Error(err))
			return s.lastGoodLanes, nil
		}
	case LaneErrorNoLanes:
		logger.Error("Replication lane policy failed. Retiring all lanes until the stream restarts.", tag.Error(err))
		s.lanePolicyDisabled = true
		return nil, nil
	case LaneErrorRestartStream:
		stream := LaneStream{RemoteCluster: s.clientClusterName, ShardID: s.serverShardKey.ShardID}
		if s.laneRestarts.Allow(stream, signals.Now) {
			return nil, NewStreamError("Replication lane policy asked to restart the stream", err)
		}
		logger.Error("Replication lane policy restarted the stream too often. Retiring all lanes until the stream restarts.", tag.Error(err))
		s.lanePolicyDisabled = true
		return nil, nil
	default:
	}
	logger.Error("Replication lane policy failed. Retiring all lanes.", tag.Error(err))
	return nil, nil
}

// laneRestartTracker counts, for each stream, the restarts its lane policy asked for. It lives
// as long as the history host, so the count survives the restarts.
type laneRestartTracker struct {
	mu       sync.Mutex
	restarts map[LaneStream][]time.Time
}

func newLaneRestartTracker() *laneRestartTracker {
	return &laneRestartTracker{restarts: make(map[LaneStream][]time.Time)}
}

// Allow records a restart of stream at now, unless the stream has restarted laneRestartLimit
// times in the last laneRestartWindow.
func (t *laneRestartTracker) Allow(stream LaneStream, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	recent := slices.DeleteFunc(t.restarts[stream], func(at time.Time) bool { return now.Sub(at) >= laneRestartWindow })
	if len(recent) >= laneRestartLimit {
		t.restarts[stream] = recent
		return false
	}
	t.restarts[stream] = append(recent, now)
	return true
}

// laneSignals returns the policy's inputs for a receiver ack.
func (s *laneStreamSender) laneSignals(attr *replicationspb.SyncReplicationState) LaneSignals {
	now := s.shardContext.GetTimeSource().Now()
	snapshots := s.laneRegistry.Snapshots()
	slices.SortFunc(snapshots, func(a, b senderLaneSnapshot) int { return strings.Compare(a.logicalKey, b.logicalKey) })
	lanes := make([]LaneStatus, 0, len(snapshots))
	s.laneStateMu.Lock()
	for _, lane := range snapshots {
		pacedAt, paced := s.lanePacedAt[lane.logicalKey]
		lanes = append(lanes, LaneStatus{
			Key:         lane.logicalKey,
			Filter:      lane.filter,
			SentCursor:  lane.cursor,
			Acked:       lane.acked,
			AckedTime:   lane.ackedTime,
			Retiring:    lane.retiring,
			Paced:       paced && now.Sub(pacedAt) < lanePacedWindow,
			PolicyState: lane.policyState,
		})
	}
	s.laneStateMu.Unlock()
	return LaneSignals{
		Now:                   now,
		ThrottledNamespaceIDs: attr.GetThrottleHighNamespaceIds(),
		Feedback:              attr.GetLaneFeedback(),
		ReceiverMaxLanes:      int(attr.GetMaxLanes()),
		SourceHighWatermark:   s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID,
		DefaultAcked:          attr.GetHighPriorityState().GetInclusiveLowWatermark(),
		Lanes:                 lanes,
	}
}

// dropRestoredLanesOverLimit keeps at most limit of the lanes restored from the reader state:
// first those the policy asks for, in its order, then the others in key order. None of them has
// sent on this stream yet, and the default lane re-sends the tasks of the ones it drops.
func (s *laneStreamSender) dropRestoredLanesOverLimit(desired []DesiredLane, limit int) {
	snapshots := s.laneRegistry.Snapshots()
	if len(snapshots) <= limit {
		return
	}
	restored := make(map[string]struct{}, len(snapshots))
	for _, lane := range snapshots {
		restored[lane.logicalKey] = struct{}{}
	}
	var keep []string
	for _, lane := range desired {
		if _, ok := restored[lane.Key]; ok {
			keep = append(keep, lane.Key)
			delete(restored, lane.Key)
		}
	}
	keep = append(keep, slices.Sorted(maps.Keys(restored))...)
	for _, key := range keep[max(limit, 0):] {
		s.laneRegistry.Forget(key)
	}
	s.laneRestoreIncomplete = true
	s.logger.Warn("The receiver accepts fewer replication lanes than were persisted. Re-sending the others on the default lane.",
		tag.Counter(len(snapshots)), tag.NewInt("max-lanes", limit))
}

// admitDesiredLanes returns the desired lanes the stream can have, in the policy's order: valid
// ones whose filters are disjoint from every other lane's, up to ReplicationStreamSenderMaxLanes,
// and without more lanes open than the receiver accepts. An existing lane keeps the filter it was
// created with. A lane that overlaps a retiring lane is created once that lane has retired.
func (s *laneStreamSender) admitDesiredLanes(desired []DesiredLane, receiverMaxLanes int) []DesiredLane {
	snapshots := s.laneRegistry.Snapshots()
	// The receiver counts every lane until it receives the lane's retirement marker.
	open := len(snapshots)
	filters := make(map[string]LaneFilter)
	owners := make(map[namespace.ID]string)
	for _, lane := range snapshots {
		filters[lane.logicalKey] = lane.filter
		for _, namespaceID := range lane.filter.NamespaceIDs {
			owners[namespaceID] = lane.logicalKey
		}
	}
	maxLanes := s.config.ReplicationStreamSenderMaxLanes()
	var admitted []DesiredLane
	admittedKeys := make(map[string]struct{}, len(desired))
	for i, lane := range desired {
		if len(admitted) >= maxLanes {
			s.shardContext.GetThrottledLogger().Warn("More replication lanes requested than ReplicationStreamSenderMaxLanes allows. Ignoring the rest.",
				tag.Counter(len(desired)), tag.NewInt("max-lanes", maxLanes))
			break
		}
		if err := validateDesiredLane(lane); err != nil {
			s.shardContext.GetThrottledLogger().Warn("Ignoring an invalid replication lane.", tag.NewInt("lane-index", i), tag.Error(err))
			continue
		}
		if _, repeated := admittedKeys[lane.Key]; repeated {
			s.shardContext.GetThrottledLogger().Warn("Ignoring a repeated replication lane.", tag.NewStringTag("lane", lane.Key))
			continue
		}
		filter, exists := filters[lane.Key]
		if !exists {
			filter = lane.Filter
		}
		if overlapping := laneFilterOwner(filter, lane.Key, owners); overlapping != "" {
			s.shardContext.GetThrottledLogger().Warn("Replication lane overlaps another lane. Creating it after that lane retires.",
				tag.NewStringTag("lane", lane.Key), tag.NewStringTag("other-lane", overlapping))
			continue
		}
		if !exists {
			if open >= receiverMaxLanes {
				s.shardContext.GetThrottledLogger().Warn("The receiver accepts no more replication lanes. Creating this one once a lane retires.",
					tag.NewStringTag("lane", lane.Key), tag.NewInt("max-lanes", receiverMaxLanes))
				continue
			}
			open++
		}
		admittedKeys[lane.Key] = struct{}{}
		for _, namespaceID := range filter.NamespaceIDs {
			owners[namespaceID] = lane.Key
		}
		admitted = append(admitted, lane)
	}
	return admitted
}

// laneFilterOwner returns another lane that owns a namespace in filter, or "".
func laneFilterOwner(filter LaneFilter, key string, owners map[namespace.ID]string) string {
	for _, namespaceID := range filter.NamespaceIDs {
		if owner, ok := owners[namespaceID]; ok && owner != key {
			return owner
		}
	}
	return ""
}

func validateDesiredLane(lane DesiredLane) error {
	if lane.Key == "" {
		return errors.New("lane key is empty")
	}
	if n := len(lane.Filter.NamespaceIDs); n == 0 || n > laneFilterMaxNamespaces {
		return fmt.Errorf("lane %q: filter has %d namespaces, want 1 to %d", lane.Key, n, laneFilterMaxNamespaces)
	}
	seen := make(map[namespace.ID]struct{}, len(lane.Filter.NamespaceIDs))
	for _, namespaceID := range lane.Filter.NamespaceIDs {
		if namespaceID == "" {
			return fmt.Errorf("lane %q: filter has an empty namespace ID", lane.Key)
		}
		if _, ok := seen[namespaceID]; ok {
			return fmt.Errorf("lane %q: filter names namespace %q twice", lane.Key, namespaceID)
		}
		seen[namespaceID] = struct{}{}
	}
	if n := len(lane.PolicyState); n > lanePolicyStateMaxBytes {
		return fmt.Errorf("lane %q: policy state is %d bytes, want at most %d", lane.Key, n, lanePolicyStateMaxBytes)
	}
	return nil
}

// setRequestedLanes records the policy's latest answer, and gets a limiter from the policy for
// each requested lane that has none yet.
func (s *laneStreamSender) setRequestedLanes(requested map[string]struct{}) {
	s.laneStateMu.Lock()
	s.requestedLanes = requested
	var missing []string
	for key := range requested {
		if _, ok := s.laneLimiters[key]; !ok {
			missing = append(missing, key)
		}
	}
	s.laneStateMu.Unlock()
	for _, key := range missing {
		var limiter LaneLimiter
		s.withLanePolicy(func(policy LaneStreamPolicy) {
			limiter = policy.Limiter(key)
		})
		if limiter == nil {
			continue
		}
		s.laneStateMu.Lock()
		s.laneLimiters[key] = limiter
		s.laneStateMu.Unlock()
	}
}

// lanePaced reports whether a lane's sends go through its limiter. Lanes are not paced once the
// policy no longer requests them, so that they can catch up to the default lane and retire.
func (s *laneStreamSender) lanePaced(logicalKey string) bool {
	s.laneStateMu.Lock()
	defer s.laneStateMu.Unlock()
	_, requested := s.requestedLanes[logicalKey]
	return requested
}

func (s *laneStreamSender) laneLimiter(logicalKey string) LaneLimiter {
	s.laneStateMu.Lock()
	defer s.laneStateMu.Unlock()
	return s.laneLimiters[logicalKey]
}

func (s *laneStreamSender) markLanePaced(logicalKey string, at time.Time) {
	s.laneStateMu.Lock()
	defer s.laneStateMu.Unlock()
	s.lanePacedAt[logicalKey] = at
}

// laneRetired drops a retired lane's state and tells the policy.
func (s *laneStreamSender) laneRetired(logicalKey string) {
	s.laneStateMu.Lock()
	delete(s.laneLimiters, logicalKey)
	delete(s.lanePacedAt, logicalKey)
	s.laneStateMu.Unlock()
	s.withLanePolicy(func(policy LaneStreamPolicy) {
		policy.LaneRetired(logicalKey)
	})
}

func (s *laneStreamSender) sendReadyLaneRetirements() error {
	for _, lane := range s.laneRegistry.ReadyRetirements() {
		err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ExclusiveHighWatermark:     s.laneRegistry.DefaultReservedCursor(),
					ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
					LaneInfo:                   &replicationspb.ReplicationLaneInfo{LaneId: lane.id, Retire: true},
				},
			},
		})
		if err != nil {
			return err
		}
		if _, ok := s.laneRegistry.CompleteRetirement(lane.id); !ok {
			return serviceerror.NewInternalf("replication lane %q changed while its retirement marker was sent", lane.id)
		}
		s.laneRetired(lane.logicalKey)
	}
	return nil
}

// laneFailoverWatermark returns the position up to which the receiver has applied every
// task, across the default HIGH lane and every lane.
func (s *laneStreamSender) laneFailoverWatermark(attr *replicationspb.SyncReplicationState) (int64, time.Time) {
	watermark := attr.GetHighPriorityState().GetInclusiveLowWatermark()
	watermarkTime := attr.GetHighPriorityState().GetInclusiveLowWatermarkTime().AsTime()
	for _, laneState := range attr.GetLaneStates() {
		if laneState.GetInclusiveLowWatermark() < watermark {
			watermark = laneState.GetInclusiveLowWatermark()
			watermarkTime = laneState.GetInclusiveLowWatermarkTime().AsTime()
		}
	}
	if floor, floorTime, ok := s.laneRegistry.ResumeFloor(); ok && floor < watermark {
		return floor - 1, floorTime
	}
	return watermark - 1, watermarkTime
}

// sendDefaultTasks sends a default lane range. On the HIGH priority default lane of a
// stream with lanes, it skips the tasks that lanes own, and waits while a lane handoff or
// retirement is in progress.
func (s *laneStreamSender) sendDefaultTasks(priority enumsspb.TaskPriority, begin, end int64) error {
	if s.laneRegistry == nil || priority != enumsspb.TASK_PRIORITY_HIGH {
		return s.sendTasks(priority, begin, end, nil)
	}
	var laneOwner func(tasks.Task) string
	for {
		changed := s.laneRegistry.Changed()
		var ok bool
		laneOwner, ok = s.laneRegistry.AcquireDefault(end)
		if ok {
			break
		}
		select {
		case <-changed:
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
	err := s.sendTasks(priority, begin, end, &laneSendTarget{laneOwner: laneOwner})
	s.laneRegistry.ReleaseDefault(end, err == nil && !s.shutdownChan.IsShutdown())
	return err
}

// startLaneLoops starts a send loop for every lane, now and as lanes are created.
func (s *laneStreamSender) startLaneLoops() {
	s.laneLoopsOnce.Do(func() {
		go WrapEventLoop(s.server.Context(), s.laneManagerLoop, s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
	})
}

func (s *laneStreamSender) laneManagerLoop() error {
	running := make(map[string]struct{})
	exited := make(chan string)
	for {
		changed := s.laneRegistry.Changed()
		for _, lane := range s.laneRegistry.Snapshots() {
			if _, ok := running[lane.id]; ok {
				continue
			}
			running[lane.id] = struct{}{}
			laneID := lane.id
			go func() {
				s.runLaneSendLoop(laneID)
				select {
				case exited <- laneID:
				case <-s.shutdownChan.Channel():
				}
			}()
		}
		select {
		case <-changed:
		case laneID := <-exited:
			delete(running, laneID)
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}

// runLaneSendLoop runs laneSendLoop and stops the stream if it fails. Unlike the other
// event loops, a lane's loop ends without stopping the stream when the lane retires.
func (s *laneStreamSender) runLaneSendLoop(laneID string) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
			s.Stop()
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)

	if err := s.laneSendLoop(laneID); err != nil {
		// A loop whose sender has stopped, or whose stream has ended, has nothing to restart.
		if !s.shutdownChan.IsShutdown() && s.server.Context().Err() == nil {
			s.logger.Warn("Replication lane send loop failed. Restarting the stream.", tag.Error(err))
		}
		s.Stop()
	}
}

// laneSendLoop sends one lane's tasks until the lane is removed or the stream stops.
func (s *laneStreamSender) laneSendLoop(laneID string) error {
	newTaskNotificationChan, subscriberID := s.historyEngine.SubscribeReplicationNotification(s.clientClusterName)
	defer s.historyEngine.UnsubscribeReplicationNotification(subscriberID)
	syncStatusTimer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer syncStatusTimer.Stop()

	for {
		changed := s.laneRegistry.Changed()
		lane, acquired := s.laneRegistry.Acquire(laneID)
		if acquired {
			err := s.sendLaneRange(lane)
			s.laneRegistry.Release(laneID)
			if err != nil {
				return err
			}
		} else if _, exists := s.laneRegistry.SnapshotByID(laneID); !exists {
			return nil
		}
		select {
		case <-newTaskNotificationChan:
		case <-syncStatusTimer.C:
			syncStatusTimer.Reset(s.config.ReplicationStreamSendEmptyTaskDuration())
		case <-changed:
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}

func (s *laneStreamSender) sendLaneRange(lane senderLaneSnapshot) error {
	end := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
	if lane.cursor >= end {
		return nil
	}
	err := s.sendTasks(enumsspb.TASK_PRIORITY_HIGH, lane.cursor, end, &laneSendTarget{
		laneInfo: &replicationspb.ReplicationLaneInfo{LaneId: lane.id},
		accept:   lane.scope.Contains,
		rateLimit: func(ctx context.Context) error {
			return s.paceLane(ctx, lane.logicalKey)
		},
	})
	if err != nil {
		return err
	}
	if !s.shutdownChan.IsShutdown() {
		s.laneRegistry.AdvanceLaneCursor(lane.id, end)
	}
	return nil
}

// laneRetryDelay is how long a lane waits after its limiter refuses a send.
func laneRetryDelay(retryAfter time.Duration) time.Duration {
	return min(max(retryAfter, laneMinRetryAfter), laneMaxRetryAfter)
}

// paceLane returns once the lane may send its next task, or with the context's error once the
// stream ends, so that the send is not retried. The limiter never blocks: when it refuses a
// send, the lane waits here for laneRetryDelay and asks again.
func (s *laneStreamSender) paceLane(ctx context.Context, logicalKey string) error {
	for s.lanePaced(logicalKey) {
		limiter := s.laneLimiter(logicalKey)
		if limiter == nil {
			return nil
		}
		now := s.shardContext.GetTimeSource().Now()
		ok, retryAfter := limiter.Reserve(now)
		if ok {
			return nil
		}
		s.markLanePaced(logicalKey, now)
		timer := time.NewTimer(laneRetryDelay(retryAfter))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}
