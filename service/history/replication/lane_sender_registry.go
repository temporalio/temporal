package replication

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

// Indexes of the tiered QueueReaderState scopes.
const (
	laneReaderOverallScopeIndex = 0
	laneReaderHighScopeIndex    = 1
)

type senderLaneSnapshot struct {
	id         string
	logicalKey string
	filter     LaneFilter
	scope      queues.Scope
	cursor     int64
	acked      int64
	ackedTime  time.Time
	// retiring records retirement intent. A finite scope end is the fixed handoff boundary.
	retiring bool
	// policyState is saved with the lane for the policy that created it.
	policyState []byte
}

type senderLane struct {
	senderLaneSnapshot
	leases int
	// A pending lane is durable but cannot send until the default lane handoff completes.
	pending bool
}

// retirementReady is called with the registry mutex held.
func (lane *senderLane) retirementReady() bool {
	return lane.retiring && !lane.pending && lane.leases == 0 &&
		lane.scope.Range.ExclusiveMax.TaskID != math.MaxInt64 && lane.acked >= lane.scope.Range.ExclusiveMax.TaskID
}

// senderLaneRegistry tracks the lanes of one lane stream sender and coordinates them with
// the default HIGH priority lane. Each lane owns the tasks in its scope from its scope's
// inclusive min. The default lane skips those tasks while the lane exists.
//
// The registry owns the durable logical-key/scope association and the stream-local lane
// ID. Restoring a durable lane always creates a new lane ID.
type senderLaneRegistry struct {
	mu                     sync.Mutex
	defaultReservedCursor  int64 // furthest default range reserved by a sender
	defaultCompletedCursor int64 // furthest default range sent successfully
	defaultLeases          int
	// A failed default pass must re-cover pending lane scopes before handoff.
	defaultRecoveryPending bool
	byKey                  map[string]*senderLane
	byID                   map[string]*senderLane
	generateLaneID         func() string
	changed                chan struct{}
}

func newSenderLaneRegistry(persistedDefaultCursor int64, persisted []*persistencespb.QueueReaderLane) (*senderLaneRegistry, error) {
	r := &senderLaneRegistry{
		defaultReservedCursor:  persistedDefaultCursor,
		defaultCompletedCursor: persistedDefaultCursor,
		byKey:                  make(map[string]*senderLane, len(persisted)),
		byID:                   make(map[string]*senderLane, len(persisted)),
		generateLaneID:         uuid.NewString,
		changed:                make(chan struct{}),
	}
	for i, persistedLane := range persisted {
		if persistedLane.GetLogicalKey() == "" || persistedLane.GetScope() == nil {
			return nil, fmt.Errorf("persisted replication lane %d is missing its logical key or scope", i)
		}
		if _, duplicate := r.byKey[persistedLane.GetLogicalKey()]; duplicate {
			return nil, fmt.Errorf("duplicate persisted replication lane logical key %q", persistedLane.GetLogicalKey())
		}
		persistenceScope := persistedLane.GetScope()
		if persistenceScope.GetPredicate().GetPredicateType() != enumsspb.PREDICATE_TYPE_NAMESPACE_ID {
			return nil, fmt.Errorf("persisted replication lane %q: scope is not a namespace filter", persistedLane.GetLogicalKey())
		}
		rangeState := persistenceScope.GetRange()
		if rangeState.GetInclusiveMin() == nil || rangeState.GetExclusiveMax() == nil {
			return nil, fmt.Errorf("persisted replication lane %q is missing its range", persistedLane.GetLogicalKey())
		}
		if queues.FromPersistenceTaskKey(rangeState.GetInclusiveMin()).CompareTo(queues.FromPersistenceTaskKey(rangeState.GetExclusiveMax())) > 0 {
			return nil, fmt.Errorf("persisted replication lane %q has an inverted range", persistedLane.GetLogicalKey())
		}
		scope := queues.FromPersistenceScope(persistedLane.GetScope())
		filter, err := laneFilterFromScope(scope)
		if err != nil {
			return nil, fmt.Errorf("persisted replication lane %q: %w", persistedLane.GetLogicalKey(), err)
		}
		lane := r.newLane(persistedLane.GetLogicalKey(), filter, scope, persistedLane.GetPolicyState())
		r.byKey[lane.logicalKey] = lane
		r.byID[lane.id] = lane
	}
	return r, nil
}

// Changed returns a channel that is closed at the next change to the set of lanes or to
// whether a lane can send.
func (r *senderLaneRegistry) Changed() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.changed
}

func (r *senderLaneRegistry) notifyChanged() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *senderLaneRegistry) newLane(logicalKey string, filter LaneFilter, scope queues.Scope, policyState []byte) *senderLane {
	laneID := r.generateLaneID()
	for {
		if _, exists := r.byID[laneID]; !exists {
			break
		}
		laneID = r.generateLaneID()
	}
	return &senderLane{senderLaneSnapshot: senderLaneSnapshot{
		id:          laneID,
		logicalKey:  logicalKey,
		filter:      filter,
		scope:       scope,
		cursor:      scope.Range.InclusiveMin.TaskID,
		retiring:    scope.Range.ExclusiveMax.TaskID != math.MaxInt64,
		policyState: slices.Clone(policyState),
	}}
}

// Create adds a lane for logicalKey if none exists, owning the tasks that filter selects from
// floor. The lane cannot send until every default range in flight has completed, so that the
// default lane and the new lane never both own a task.
func (r *senderLaneRegistry) Create(logicalKey string, filter LaneFilter, floor int64, policyState []byte) (senderLaneSnapshot, bool, error) {
	if logicalKey == "" {
		return senderLaneSnapshot{}, false, errors.New("replication lane logical key is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if lane, ok := r.byKey[logicalKey]; ok {
		return lane.senderLaneSnapshot, false, nil
	}
	lane := r.newLane(logicalKey, filter, laneScope(filter, floor), policyState)
	if r.defaultLeases == 0 && !r.defaultRecoveryPending {
		lane.cursor = max(lane.cursor, r.defaultCompletedCursor)
	} else {
		lane.pending = true
	}
	r.byKey[logicalKey] = lane
	r.byID[lane.id] = lane
	r.notifyChanged()
	return lane.senderLaneSnapshot, true, nil
}

func (r *senderLaneRegistry) SnapshotByID(laneID string) (senderLaneSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[laneID]
	if !ok {
		return senderLaneSnapshot{}, false
	}
	return lane.senderLaneSnapshot, true
}

func (r *senderLaneRegistry) SnapshotByKey(logicalKey string) (senderLaneSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byKey[logicalKey]
	if !ok {
		return senderLaneSnapshot{}, false
	}
	return lane.senderLaneSnapshot, true
}

func (r *senderLaneRegistry) Snapshots() []senderLaneSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]senderLaneSnapshot, 0, len(r.byKey))
	for _, lane := range r.byKey {
		out = append(out, lane.senderLaneSnapshot)
	}
	return out
}

// ObserveAcks records the lane watermarks from a receiver ack. A lane's acked position
// never moves backwards.
func (r *senderLaneRegistry) ObserveAcks(states map[string]*replicationspb.ReplicationState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	for laneID, state := range states {
		lane, ok := r.byID[laneID]
		if !ok {
			continue
		}
		if acked := min(state.GetInclusiveLowWatermark(), lane.scope.Range.ExclusiveMax.TaskID); acked > lane.acked {
			lane.acked = acked
			lane.ackedTime = state.GetInclusiveLowWatermarkTime().AsTime()
			changed = true
		}
	}
	if changed {
		r.notifyChanged()
	}
}

// RequestRetirement withdraws a lane. It keeps sending until its cursor covers every
// default reservation, then fixes its exclusive end at that sent cursor. New default
// reservations own the suffix, after the lane has sent the entire prefix. The lane stays
// registered until the receiver acknowledges that prefix.
func (r *senderLaneRegistry) RequestRetirement(logicalKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byKey[logicalKey]
	if !ok || lane.retiring {
		return false
	}
	lane.retiring = true
	r.boundRetiringLane(lane)
	r.notifyChanged()
	return true
}

// boundRetiringLane fixes a retirement boundary under mu once the lane has sent through
// every default reservation and released its send lease. If default reservations keep ahead
// of the lane, retirement remains pending. The finite end is persisted and never moves again.
func (r *senderLaneRegistry) boundRetiringLane(lane *senderLane) bool {
	if !lane.retiring || lane.pending || lane.leases != 0 || lane.scope.Range.ExclusiveMax.TaskID != math.MaxInt64 || lane.cursor < r.defaultReservedCursor {
		return false
	}
	lane.scope.Range.ExclusiveMax = tasks.NewImmediateKey(lane.cursor)
	return true
}

// SetPolicyState replaces the state saved with a lane.
func (r *senderLaneRegistry) SetPolicyState(logicalKey string, policyState []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lane, ok := r.byKey[logicalKey]; ok {
		lane.policyState = slices.Clone(policyState)
	}
}

// ReadyRetirements returns lanes whose fixed prefix has been acknowledged and whose sender
// has released its lease. Default reservations can continue while the lane drains.
func (r *senderLaneRegistry) ReadyRetirements() []senderLaneSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []senderLaneSnapshot
	for _, lane := range r.byKey {
		if lane.retirementReady() {
			out = append(out, lane.senderLaneSnapshot)
		}
	}
	return out
}

// CompleteRetirement removes a retiring lane after its retirement marker was sent.
func (r *senderLaneRegistry) CompleteRetirement(laneID string) (senderLaneSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[laneID]
	if !ok || !lane.retirementReady() {
		return senderLaneSnapshot{}, false
	}
	delete(r.byID, laneID)
	delete(r.byKey, lane.logicalKey)
	r.notifyChanged()
	return lane.senderLaneSnapshot, true
}

// Acquire takes the lane's only send lease. The lane cannot retire or be removed while
// the lease is held.
func (r *senderLaneRegistry) Acquire(laneID string) (senderLaneSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[laneID]
	if !ok || lane.pending || lane.leases != 0 {
		return senderLaneSnapshot{}, false
	}
	if lane.retiring && lane.scope.Range.ExclusiveMax.TaskID != math.MaxInt64 && lane.acked >= lane.scope.Range.ExclusiveMax.TaskID {
		return senderLaneSnapshot{}, false
	}
	lane.leases++
	return lane.senderLaneSnapshot, true
}

func (r *senderLaneRegistry) Release(laneID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[laneID]
	if !ok || lane.leases == 0 {
		return
	}
	lane.leases--
	if lane.leases == 0 && lane.retiring {
		if r.boundRetiringLane(lane) || lane.retirementReady() {
			r.notifyChanged()
		}
	}
}

func (r *senderLaneRegistry) AdvanceLaneCursor(laneID string, to int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lane, ok := r.byID[laneID]; ok && to > lane.cursor {
		lane.cursor = to
	}
}

// AcquireDefault reserves a default lane range ending at endExclusive. It returns a function
// that names the lane owning a task, or "" for a task the default lane sends, or nil when no
// lane exists. It returns false while a new lane's handoff is in progress. Retiring lanes
// keep only their finite prefix, so new default reservations continue beyond that prefix.
func (r *senderLaneRegistry) AcquireDefault(endExclusive int64) (func(tasks.Task) string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	recovering := r.defaultLeases == 0 && r.defaultRecoveryPending
	for _, lane := range r.byKey {
		if lane.pending && !recovering {
			return nil, false
		}
	}
	if recovering {
		r.defaultRecoveryPending = false
	}
	r.defaultLeases++
	if endExclusive > r.defaultReservedCursor {
		r.defaultReservedCursor = endExclusive
	}
	type ownedScope struct {
		logicalKey string
		scope      queues.Scope
	}
	scopes := make([]ownedScope, 0, len(r.byKey))
	for _, lane := range r.byKey {
		if lane.pending {
			// Recovery must re-cover pending scopes on the default lane before handoff.
			continue
		}
		scopes = append(scopes, ownedScope{logicalKey: lane.logicalKey, scope: lane.scope})
	}
	if len(scopes) == 0 {
		return nil, true
	}
	return func(task tasks.Task) string {
		for i := range scopes {
			if scopes[i].scope.Contains(task) {
				return scopes[i].logicalKey
			}
		}
		return ""
	}, true
}

// ReleaseDefault ends a default range reserved by AcquireDefault. A range that did not
// complete forces a recovery pass before any pending lane can start.
func (r *senderLaneRegistry) ReleaseDefault(endExclusive int64, completed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if completed {
		r.defaultCompletedCursor = max(r.defaultCompletedCursor, endExclusive)
	} else {
		r.defaultRecoveryPending = true
	}
	if r.defaultLeases > 0 {
		r.defaultLeases--
	}
	if r.defaultLeases != 0 || r.defaultRecoveryPending {
		return
	}
	changed := false
	for _, lane := range r.byKey {
		if lane.pending {
			lane.cursor = max(lane.cursor, r.defaultCompletedCursor)
			lane.pending = false
			r.boundRetiringLane(lane)
			changed = true
		}
	}
	if changed {
		r.notifyChanged()
	}
}

func (r *senderLaneRegistry) DefaultReservedCursor() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.defaultReservedCursor
}

// RequireDefaultRecovery keeps new lanes pending until the default lane has replayed the
// conservative floor. Its old completion cursor cannot establish ownership for a new lane.
func (r *senderLaneRegistry) RequireDefaultRecovery(floor int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultCompletedCursor = min(r.defaultCompletedCursor, floor)
	r.defaultRecoveryPending = true
	r.notifyChanged()
}

// ResumeFloor returns the lowest resume point across lanes, with the ack time of the lane
// that sets it. The time is zero when that lane has never been acked: its resume point is
// its scope floor, which has no ack time.
func (r *senderLaneRegistry) ResumeFloor() (int64, time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.byKey) == 0 {
		return 0, time.Time{}, false
	}
	floor := int64(math.MaxInt64)
	var floorTime time.Time
	for _, lane := range r.byKey {
		if resume := max(lane.scope.Range.InclusiveMin.TaskID, lane.acked); resume < floor {
			floor = resume
			floorTime = lane.ackedTime
		}
	}
	return floor, floorTime, true
}

// Forget drops a lane without retiring it. The sender calls it only for a lane restored from
// the reader state that has not sent on this stream, and re-sends the lane's tasks on the
// default lane.
func (r *senderLaneRegistry) Forget(logicalKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byKey[logicalKey]
	if !ok {
		return
	}
	delete(r.byKey, logicalKey)
	delete(r.byID, lane.id)
	r.notifyChanged()
}

// ClearLanes drops all lane state. The sender calls it when the receiver does not support
// lanes. The default lane then re-covers the lane ranges from their resume floor.
func (r *senderLaneRegistry) ClearLanes() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.byKey)
	clear(r.byID)
	r.notifyChanged()
}

// BuildReaderState returns the reader state to persist for a receiver ack. While lanes
// exist, the overall and HIGH scopes start no later than any lane's resume point, so a
// sender that ignores lanes still re-sends every lane's tasks. The default lane's own
// position is kept in replication_lane_default_cursor.
func (r *senderLaneRegistry) BuildReaderState(attr *replicationspb.SyncReplicationState) *persistencespb.QueueReaderState {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := buildTieredReaderState(attr)
	if len(r.byKey) != 0 {
		state.ReplicationLaneDefaultCursor = shard.ConvertToPersistenceTaskKey(
			tasks.NewImmediateKey(attr.GetHighPriorityState().GetInclusiveLowWatermark()),
		)
	}
	keys := make([]string, 0, len(r.byKey))
	for logicalKey := range r.byKey {
		keys = append(keys, logicalKey)
	}
	slices.Sort(keys)
	for _, logicalKey := range keys {
		lane := r.byKey[logicalKey]
		resume := min(max(lane.scope.Range.InclusiveMin.TaskID, lane.acked), lane.scope.Range.ExclusiveMax.TaskID)
		scope := lane.scope
		scope.Range.InclusiveMin = tasks.NewImmediateKey(resume)
		state.Lanes = append(state.Lanes, &persistencespb.QueueReaderLane{
			LogicalKey:  logicalKey,
			Scope:       queues.ToPersistenceScope(scope),
			PolicyState: lane.policyState,
		})
		state.Scopes[laneReaderOverallScopeIndex].Range.InclusiveMin.TaskId = min(
			state.Scopes[laneReaderOverallScopeIndex].Range.InclusiveMin.TaskId,
			resume,
		)
		state.Scopes[laneReaderHighScopeIndex].Range.InclusiveMin.TaskId = min(
			state.Scopes[laneReaderHighScopeIndex].Range.InclusiveMin.TaskId,
			resume,
		)
	}
	return state
}

// laneScope is the scope of a lane that owns the tasks filter selects from floor.
func laneScope(filter LaneFilter, floor int64) queues.Scope {
	namespaceIDs := make([]string, len(filter.NamespaceIDs))
	for i, namespaceID := range filter.NamespaceIDs {
		namespaceIDs[i] = namespaceID.String()
	}
	return queues.NewScope(
		queues.NewRange(tasks.NewImmediateKey(floor), tasks.NewImmediateKey(math.MaxInt64)),
		tasks.NewNamespacePredicate(namespaceIDs),
	)
}

// laneFilterFromScope returns the filter of a lane restored with scope.
func laneFilterFromScope(scope queues.Scope) (LaneFilter, error) {
	predicate, ok := scope.Predicate.(*tasks.NamespacePredicate)
	if !ok || len(predicate.NamespaceIDs) == 0 {
		return LaneFilter{}, errors.New("lane scope is not a namespace filter")
	}
	namespaceIDs := make([]namespace.ID, 0, len(predicate.NamespaceIDs))
	for namespaceID := range predicate.NamespaceIDs {
		namespaceIDs = append(namespaceIDs, namespace.ID(namespaceID))
	}
	slices.Sort(namespaceIDs)
	return LaneFilter{NamespaceIDs: namespaceIDs}, nil
}

// laneDefaultCursor returns the persisted position of the default HIGH lane: the default
// lane cursor when lanes were persisted, otherwise the HIGH scope.
func laneDefaultCursor(readerState *persistencespb.QueueReaderState) int64 {
	if cursor := readerState.GetReplicationLaneDefaultCursor(); cursor != nil {
		return cursor.GetTaskId()
	}
	return readerState.Scopes[laneReaderHighScopeIndex].GetRange().GetInclusiveMin().GetTaskId()
}
