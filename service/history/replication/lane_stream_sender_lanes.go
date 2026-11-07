package replication

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
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

	// Reasons on replication_lane_policy_failures.
	lanePolicyFailureError metrics.ReasonString = "error"
	lanePolicyFailurePanic metrics.ReasonString = "panic"
)

// laneSendTarget directs sendTasks to send on a lane, or to filter the default lane.
type laneSendTarget struct {
	// laneInfo is set on every message, and logicalKey is the lane's key. Nil sends on the
	// default lane.
	laneInfo   *replicationspb.ReplicationLaneInfo
	logicalKey string
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

// startLanePolicy creates the stream's lane policy. The stream does so on its first ack rather
// than when it starts, so that the liveness monitor also covers a factory that hangs. Without
// a policy, the stream retires its lanes.
func (s *laneStreamSender) startLanePolicy() {
	if s.lanePolicyFactory == nil {
		return
	}
	var policy LaneStreamPolicy
	var err error
	if !s.callLanePolicy("NewStreamPolicy", func() {
		policy, err = s.lanePolicyFactory.NewStreamPolicy(LaneStream{
			RemoteCluster: s.clientClusterName,
			ShardID:       s.serverShardKey.ShardID,
		})
	}) {
		return
	}
	if err != nil {
		s.recordLanePolicyFailure(lanePolicyFailureError)
		s.logger.Error("Unable to create the replication lane policy. The stream runs without lanes.", tag.Error(err))
		return
	}
	s.lanePolicyMu.Lock()
	s.lanePolicy = policy
	s.lanePolicyMu.Unlock()
	s.closeLanePolicyIfStopped()
}

// closeLanePolicy closes the stream's lane policy when the stream stops. It never waits for
// the policy: if a call is running, the policy is closed when that call returns.
func (s *laneStreamSender) closeLanePolicy() {
	s.lanePolicyStopped.Store(true)
	s.closeLanePolicyIfStopped()
}

// closeLanePolicyIfStopped closes the lane policy if the stream has stopped and no policy call
// is running. Whichever of Stop and a running call comes last closes the policy.
func (s *laneStreamSender) closeLanePolicyIfStopped() {
	if !s.lanePolicyStopped.Load() || !s.lanePolicyMu.TryLock() {
		return
	}
	if s.lanePolicy == nil || s.lanePolicyClosed {
		s.lanePolicyMu.Unlock()
		return
	}
	policy := s.lanePolicy
	s.lanePolicyClosed = true
	s.lanePolicyMu.Unlock()
	// Cleanup may hang just like any other plugin call. Mark it closed before scheduling
	// it, so Stop remains non-blocking and no later policy method can enter.
	go s.callLanePolicy("Close", policy.Close)
}

// withLanePolicy calls f with the stream's lane policy, unless the stream has none, has
// stopped, or has disabled the policy after a panic.
func (s *laneStreamSender) withLanePolicy(method string, f func(LaneStreamPolicy)) {
	s.lanePolicyMu.Lock()
	if s.lanePolicy != nil && !s.lanePolicyClosed && !s.lanePolicyStopped.Load() && !s.lanePolicyDisabled.Load() {
		policy := s.lanePolicy
		s.callLanePolicy(method, func() { f(policy) })
	}
	s.lanePolicyMu.Unlock()
	s.closeLanePolicyIfStopped()
}

// callLanePolicy calls f, which calls the lane policy or one of its limiters, and reports
// whether it returned without panicking. After a panic, the stream calls nothing more on the
// policy except Close, so its lanes catch up without their limits and retire.
func (s *laneStreamSender) callLanePolicy(method string, f func()) bool {
	var panicErr error
	func() {
		defer log.CapturePanic(s.logger, &panicErr)
		f()
	}()
	if panicErr == nil {
		return true
	}
	s.lanePolicyDisabled.Store(true)
	s.recordLanePolicyFailure(lanePolicyFailurePanic)
	s.logger.Error("Replication lane policy panicked. The stream runs without lanes until it restarts.",
		tag.NewStringTag("method", method), tag.Error(panicErr))
	return false
}

func (s *laneStreamSender) recordLanePolicyFailure(reason metrics.ReasonString) {
	metrics.ReplicationLanePolicyFailures.With(s.metrics).Record(
		1,
		metrics.ToClusterTag(s.clientClusterName),
		metrics.ReasonTag(reason),
	)
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
		registry.RequireDefaultRecovery(s.laneRecoveryFloor)
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
		if first {
			s.startLanePolicy()
		}
		s.laneRegistry.ObserveAcks(attr.GetLaneStates())
		s.recordLaneLag()
		if err := s.reconcileLanes(attr, first); err != nil {
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
	s.laneReconcileMu.Lock()
	defer s.laneReconcileMu.Unlock()
	if _, handovers, err := s.shardContext.GetReplicationStatus([]string{s.clientClusterName}); err == nil {
		s.handoverInProgress.Store(len(handovers) > 0)
	}
	signals := s.laneSignals(attr)
	desired := s.evaluateLanes(signals)
	if first {
		s.dropRestoredLanesOverLimit(desired, signals.ReceiverMaxLanes)
	}
	desired = s.admitDesiredLanes(desired, signals.ReceiverMaxLanes)
	requested := make(map[string]struct{}, len(desired))
	for _, lane := range desired {
		if existing, ok := s.laneRegistry.SnapshotByKey(lane.Key); ok && existing.retiring {
			// Default reservations may already own tasks beyond the finite handoff.
			// Finish that handoff before allowing the key to create a new lane.
			continue
		}
		requested[lane.Key] = struct{}{}
	}
	// Obtain limiters before publishing new lanes to the send-loop manager.
	s.setRequestedLanes(requested)
	if s.lanePolicyDisabled.Load() {
		// Send loops can already read the published map. Replace it instead of mutating it.
		requested = nil
		s.setRequestedLanes(requested)
	}
	for _, lane := range desired {
		if _, ok := requested[lane.Key]; !ok {
			continue
		}
		if _, ok := s.laneRegistry.SnapshotByKey(lane.Key); ok {
			s.laneRegistry.SetPolicyState(lane.Key, lane.PolicyState)
			continue
		}
		_, created, err := s.laneRegistry.Create(lane.Key, lane.Filter, signals.DefaultAcked, lane.PolicyState)
		if err != nil {
			return err
		}
		if created {
			metrics.ReplicationLaneCreated.With(s.metrics).Record(
				1,
				metrics.ToClusterTag(s.clientClusterName),
				metrics.ReplicationLaneTag(lane.Key),
			)
		}
	}
	for _, lane := range s.laneRegistry.Snapshots() {
		if _, ok := requested[lane.logicalKey]; !ok {
			s.laneRegistry.RequestRetirement(lane.logicalKey)
		}
	}
	return nil
}

// evaluateLanes returns the policy's answer for signals. When the stream has no answer,
// because it has no policy or the policy failed, it returns no lanes: every lane catches up
// without its limit and retires, and the stream asks again on the next ack.
func (s *laneStreamSender) evaluateLanes(signals LaneSignals) []DesiredLane {
	var desired []DesiredLane
	var err error
	s.withLanePolicy("Evaluate", func(policy LaneStreamPolicy) {
		desired, err = policy.Evaluate(signals)
	})
	if err != nil {
		s.recordLanePolicyFailure(lanePolicyFailureError)
		s.shardContext.GetThrottledLogger().Warn("Replication lane policy failed. Retiring its lanes until it answers.", tag.Error(err))
		return nil
	}
	return desired
}

// laneSignals returns the policy's inputs for a receiver ack.
func (s *laneStreamSender) laneSignals(attr *replicationspb.SyncReplicationState) LaneSignals {
	now := s.shardContext.GetTimeSource().Now()
	snapshots := s.laneRegistry.Snapshots()
	slices.SortFunc(snapshots, func(a, b senderLaneSnapshot) int { return strings.Compare(a.logicalKey, b.logicalKey) })
	lanes := make([]LaneStatus, 0, len(snapshots))
	s.laneStateMu.Lock()
	for _, lane := range snapshots {
		lanes = append(lanes, LaneStatus{
			Key:         lane.logicalKey,
			Filter:      LaneFilter{NamespaceIDs: slices.Clone(lane.filter.NamespaceIDs)},
			SentCursor:  lane.cursor,
			Acked:       lane.acked,
			AckedTime:   lane.ackedTime,
			Retiring:    lane.retiring,
			LastRefused: s.laneRefusedAt[lane.logicalKey],
			Paused:      lane.paused,
			PolicyState: slices.Clone(lane.policyState),
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
		HandoverInProgress:    s.handoverInProgress.Load(),
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
	s.laneRegistry.RequireDefaultRecovery(s.laneRecoveryFloor)
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
		} else if !sameLaneFilter(filter, lane.Filter) {
			s.shardContext.GetThrottledLogger().Warn("Ignoring a filter change for an existing replication lane. Retire the key before changing its filter.",
				tag.NewStringTag("lane", lane.Key))
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

func sameLaneFilter(a, b LaneFilter) bool {
	if slices.Equal(a.NamespaceIDs, b.NamespaceIDs) {
		return true
	}
	aIDs, bIDs := slices.Clone(a.NamespaceIDs), slices.Clone(b.NamespaceIDs)
	slices.Sort(aIDs)
	slices.Sort(bIDs)
	return slices.Equal(aIDs, bIDs)
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
		s.withLanePolicy("Limiter", func(policy LaneStreamPolicy) {
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

// laneShouldBeRateLimited reports whether a lane's sends go through its limiter. Lanes are
// not rate limited while a namespace handover is in progress or once the policy no longer
// requests them, so handovers can finish and withdrawn lanes can drain their finite prefix.
func (s *laneStreamSender) laneShouldBeRateLimited(logicalKey string) bool {
	if s.handoverInProgress.Load() {
		return false
	}
	s.laneStateMu.Lock()
	defer s.laneStateMu.Unlock()
	_, requested := s.requestedLanes[logicalKey]
	return requested
}

// laneLimiter returns a lane's limiter, or nil once the policy is disabled after a panic.
func (s *laneStreamSender) laneLimiter(logicalKey string) LaneLimiter {
	if s.lanePolicyDisabled.Load() || s.lanePolicyStopped.Load() {
		return nil
	}
	s.laneStateMu.Lock()
	defer s.laneStateMu.Unlock()
	return s.laneLimiters[logicalKey]
}

func (s *laneStreamSender) markLaneRefused(logicalKey string, at time.Time) {
	s.laneStateMu.Lock()
	defer s.laneStateMu.Unlock()
	s.laneRefusedAt[logicalKey] = at
}

// laneRetired drops a retired lane's state and tells the policy.
func (s *laneStreamSender) laneRetired(logicalKey string) {
	s.laneStateMu.Lock()
	delete(s.laneLimiters, logicalKey)
	delete(s.laneRefusedAt, logicalKey)
	s.laneStateMu.Unlock()
	s.withLanePolicy("LaneRetired", func(policy LaneStreamPolicy) {
		policy.LaneRetired(logicalKey)
	})
}

func (s *laneStreamSender) sendReadyLaneRetirements() error {
	for _, lane := range s.laneRegistry.ReadyRetirements() {
		err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ExclusiveHighWatermark:     lane.scope.Range.ExclusiveMax.TaskID,
					ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
					LaneInfo:                   &replicationspb.ReplicationLaneInfo{LaneId: lane.id, Retire: true},
				},
			},
		})
		if err != nil {
			return err
		}
		// Serialize completion and its callback with key reuse, but keep transport I/O
		// outside this lock. A blocked Send must never prevent ACK processing.
		s.laneReconcileMu.Lock()
		_, ok := s.laneRegistry.CompleteRetirement(lane.id)
		if ok {
			s.laneRetired(lane.logicalKey)
		}
		s.laneReconcileMu.Unlock()
		if !ok {
			return serviceerror.NewInternalf("replication lane %q changed while its retirement marker was sent", lane.id)
		}
		metrics.ReplicationLaneRetired.With(s.metrics).Record(
			1,
			metrics.ToClusterTag(s.clientClusterName),
			metrics.ReplicationLaneTag(lane.logicalKey),
		)
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
		if err := s.sendReadyLaneRetirements(); err != nil {
			return err
		}
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
	end := min(s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID, lane.scope.Range.ExclusiveMax.TaskID)
	if lane.cursor > end || (lane.cursor == end && lane.acked >= end) {
		return nil
	}
	err := s.sendTasks(enumsspb.TASK_PRIORITY_HIGH, lane.cursor, end, &laneSendTarget{
		laneInfo:   &replicationspb.ReplicationLaneInfo{LaneId: lane.id},
		logicalKey: lane.logicalKey,
		accept:     lane.scope.Contains,
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
// stream ends, so that the send is not retried. A lane the receiver has paused waits until it
// resumes, even during a handover, because the pause protects the receiver. The limiter never
// blocks: when it refuses a send, the lane waits here for laneRetryDelay and asks again.
func (s *laneStreamSender) paceLane(ctx context.Context, logicalKey string) error {
	for {
		changed := s.laneRegistry.Changed()
		if !s.laneRegistry.Paused(logicalKey) {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for s.laneShouldBeRateLimited(logicalKey) {
		limiter := s.laneLimiter(logicalKey)
		if limiter == nil {
			return nil
		}
		now := s.shardContext.GetTimeSource().Now()
		var ok bool
		var retryAfter time.Duration
		if !s.callLanePolicy("Reserve", func() { ok, retryAfter = limiter.Reserve(now) }) {
			return nil
		}
		if ok {
			return nil
		}
		s.markLaneRefused(logicalKey, now)
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

// recordLaneLag records how far each lane is behind, and reports lanes behind by more than
// ReplicationLaneMaxLag. The report carries the lane's live state, which is otherwise only in
// memory, so that a lagging lane can be told apart from a stuck one.
func (s *laneStreamSender) recordLaneLag() {
	maxLag := s.config.ReplicationLaneMaxLag()
	now := time.Now()
	lanes := s.laneRegistry.Snapshots()
	if len(lanes) == 0 {
		return
	}
	sourceHighWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
	for _, lane := range lanes {
		if lane.ackedTime.IsZero() {
			continue
		}
		var lag time.Duration
		// An idle lane's ACK timestamp ages even when it has acknowledged its entire range.
		if lane.acked < min(sourceHighWatermark, lane.scope.Range.ExclusiveMax.TaskID) {
			lag = now.Sub(lane.ackedTime)
		}
		laneTag := metrics.ReplicationLaneTag(lane.logicalKey)
		metrics.ReplicationLaneLag.With(s.metrics).Record(lag, metrics.ToClusterTag(s.clientClusterName), laneTag)
		if maxLag > 0 && lag > maxLag {
			metrics.ReplicationLaneMaxLagExceeded.With(s.metrics).Record(1, metrics.ToClusterTag(s.clientClusterName), laneTag)
			s.laneStateMu.Lock()
			_, requested := s.requestedLanes[lane.logicalKey]
			pacedAt, paced := s.lanePacedAt[lane.logicalKey]
			s.laneStateMu.Unlock()
			s.shardContext.GetThrottledLogger().Warn("Replication lane is behind by more than ReplicationLaneMaxLag.",
				tag.NewStringTag("lane", lane.logicalKey),
				tag.NewDurationTag("lag", lag),
				tag.NewDurationTag("max-lag", maxLag),
				tag.NewInt64("lane-sent-cursor", lane.cursor),
				tag.NewInt64("lane-acked", lane.acked),
				tag.NewInt64("source-high-watermark", sourceHighWatermark),
				tag.NewBoolTag("lane-requested", requested),
				tag.NewBoolTag("lane-paced", paced && now.Sub(pacedAt) < lanePacedWindow),
				tag.NewBoolTag("lane-paused", lane.paused),
				tag.NewBoolTag("lane-retiring", lane.retiring),
				tag.NewBoolTag("handover-in-progress", s.handoverInProgress.Load()))
		}
	}
}
