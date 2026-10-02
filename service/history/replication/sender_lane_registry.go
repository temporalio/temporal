package replication

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type replicationLaneClass int

type senderLaneSnapshot struct {
	id         string
	logicalKey string
	class      replicationLaneClass
	scope      queues.Scope
	cursor     int64
	acked      int64
	ackedTime  time.Time
	retiring   bool
}

type senderLane struct {
	senderLaneSnapshot
	leases int
	// A pending lane is durable but cannot send until the shared-lane handoff completes.
	pending bool
}

type senderLaneRegistry struct {
	mu                     sync.Mutex
	defaultReservedCursor  int64 // furthest shared range reserved by a sender
	defaultCompletedCursor int64 // furthest shared range sent successfully
	defaultLeases          int
	// A failed shared pass must re-cover pending lane scopes before handoff.
	defaultRecoveryPending bool
	byKey                  map[string]*senderLane
	byID                   map[string]*senderLane
	generateLaneID         func() string
}

// The registry owns the durable logical-key/scope association and the ephemeral
// wire ID. Restoring a durable lane always creates a new stream-local ID.

func newSenderLaneRegistry(persistedDefaultCursor int64, persisted []*persistencespb.QueueReaderLane, classCount int) (*senderLaneRegistry, error) {
	r := &senderLaneRegistry{
		defaultReservedCursor:  persistedDefaultCursor,
		defaultCompletedCursor: persistedDefaultCursor,
		byKey:                  make(map[string]*senderLane, len(persisted)),
		byID:                   make(map[string]*senderLane, len(persisted)),
		generateLaneID:         uuid.NewString,
	}
	for i, persistedLane := range persisted {
		if persistedLane.GetLogicalKey() == "" || persistedLane.GetScope() == nil {
			return nil, fmt.Errorf("persisted replication lane %d is missing its logical key or scope", i)
		}
		if _, duplicate := r.byKey[persistedLane.GetLogicalKey()]; duplicate {
			return nil, fmt.Errorf("duplicate persisted replication lane logical key %q", persistedLane.GetLogicalKey())
		}
		scope := queues.FromPersistenceScope(persistedLane.GetScope())
		lane := r.newLane(persistedLane.GetLogicalKey(), scope, restoreLaneClass(persistedLane.GetServiceClass(), classCount))
		r.byKey[lane.logicalKey] = lane
		r.byID[lane.id] = lane
	}
	return r, nil
}

// restoreLaneClass clamps a persisted service class into [1, classCount]: zero
// predates the field, and a class above the current count would never be served
// by a class event loop.
func restoreLaneClass(persisted int32, classCount int) replicationLaneClass {
	return replicationLaneClass(min(max(int(persisted), 1), max(1, classCount)))
}

func (r *senderLaneRegistry) newLane(logicalKey string, scope queues.Scope, class replicationLaneClass) *senderLane {
	cursor := scope.Range.InclusiveMin.TaskID
	laneID := r.generateLaneID()
	for {
		if _, exists := r.byID[laneID]; !exists {
			break
		}
		laneID = r.generateLaneID()
	}
	return &senderLane{senderLaneSnapshot: senderLaneSnapshot{
		id:         laneID,
		logicalKey: logicalKey,
		class:      class,
		scope:      scope,
		cursor:     cursor,
	}}
}

func (r *senderLaneRegistry) Create(logicalKey string, scope queues.Scope, class replicationLaneClass) (senderLaneSnapshot, bool, error) {
	if logicalKey == "" {
		return senderLaneSnapshot{}, false, errors.New("replication lane logical key is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if lane, ok := r.byKey[logicalKey]; ok {
		return lane.senderLaneSnapshot, false, nil
	}
	lane := r.newLane(logicalKey, scope, class)
	if r.defaultLeases == 0 && !r.defaultRecoveryPending {
		lane.cursor = max(lane.cursor, r.defaultCompletedCursor)
	} else {
		lane.pending = true
	}
	r.byKey[logicalKey] = lane
	r.byID[lane.id] = lane
	return lane.senderLaneSnapshot, true, nil
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

func (r *senderLaneRegistry) ClassSnapshots(class replicationLaneClass) []senderLaneSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []senderLaneSnapshot
	for _, lane := range r.byKey {
		if lane.class == class && !lane.pending && !lane.retiring {
			out = append(out, lane.senderLaneSnapshot)
		}
	}
	return out
}

func (r *senderLaneRegistry) SetClass(logicalKey string, class replicationLaneClass) (senderLaneSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byKey[logicalKey]
	if !ok || lane.class == class || lane.retiring {
		return senderLaneSnapshot{}, false
	}
	lane.class = class
	return lane.senderLaneSnapshot, true
}

func (r *senderLaneRegistry) ObserveAcks(states map[string]*replicationspb.ReplicationState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for laneID, state := range states {
		if lane, ok := r.byID[laneID]; ok && state.GetInclusiveLowWatermark() > lane.acked {
			lane.acked = state.GetInclusiveLowWatermark()
			lane.ackedTime = state.GetInclusiveLowWatermarkTime().AsTime()
		}
	}
}

func (r *senderLaneRegistry) RequestRetirement(logicalKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byKey[logicalKey]
	if !ok || lane.pending || lane.retiring || lane.leases != 0 {
		return false
	}
	if r.defaultReservedCursor == 0 {
		return false
	}
	retirementCursor := max(r.defaultReservedCursor, lane.cursor)
	if lane.acked < retirementCursor {
		return false
	}
	lane.retiring = true
	return true
}

func (r *senderLaneRegistry) CancelRetirement(logicalKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byKey[logicalKey]
	if !ok || !lane.retiring {
		return false
	}
	lane.retiring = false
	return true
}

func (r *senderLaneRegistry) ReadyRetirements() []senderLaneSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []senderLaneSnapshot
	for _, lane := range r.byKey {
		if lane.retiring && lane.leases == 0 && r.defaultLeases == 0 {
			out = append(out, lane.senderLaneSnapshot)
		}
	}
	return out
}

func (r *senderLaneRegistry) CompleteRetirement(laneID string) (senderLaneSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[laneID]
	if !ok || !lane.retiring || lane.leases != 0 {
		return senderLaneSnapshot{}, false
	}
	delete(r.byID, laneID)
	delete(r.byKey, lane.logicalKey)
	return lane.senderLaneSnapshot, true
}

func (r *senderLaneRegistry) Acquire(snapshot senderLaneSnapshot) (senderLaneSnapshot, replicationLaneClass, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[snapshot.id]
	if !ok || lane.pending || lane.retiring || lane.leases != 0 {
		return senderLaneSnapshot{}, 0, false
	}
	if lane.class != snapshot.class {
		return senderLaneSnapshot{}, lane.class, false
	}
	lane.leases++
	return lane.senderLaneSnapshot, lane.class, true
}

func (r *senderLaneRegistry) Release(laneID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.byID[laneID]
	if !ok || lane.leases == 0 {
		return
	}
	lane.leases--
}

func (r *senderLaneRegistry) AdvanceLaneCursor(laneID string, to int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lane, ok := r.byID[laneID]; ok && to > lane.cursor {
		lane.cursor = to
	}
}

func (r *senderLaneRegistry) AdvanceDefaultReservation(endExclusive int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if endExclusive > r.defaultReservedCursor {
		r.defaultReservedCursor = endExclusive
	}
}

func (r *senderLaneRegistry) AcquireDefault(endExclusive int64) (func(tasks.Task) bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	recovering := r.defaultLeases == 0 && r.defaultRecoveryPending
	for _, lane := range r.byKey {
		if lane.retiring || (lane.pending && !recovering) {
			// Do not let the shared cursor cross a lane handoff in either direction.
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
	scopes := make([]queues.Scope, 0, len(r.byKey))
	for _, lane := range r.byKey {
		if lane.pending {
			// Recovery must re-cover pending scopes on the shared lane before handoff.
			continue
		}
		scopes = append(scopes, lane.scope)
	}
	if len(scopes) == 0 {
		return nil, true
	}
	return func(task tasks.Task) bool {
		for i := range scopes {
			if scopes[i].Contains(task) {
				return false
			}
		}
		return true
	}, true
}

func (r *senderLaneRegistry) ReleaseDefault(endExclusive int64, completed bool) []replicationLaneClass {
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
		return nil
	}
	var runnableClasses []replicationLaneClass
	for _, lane := range r.byKey {
		if lane.pending {
			lane.cursor = max(lane.cursor, r.defaultCompletedCursor)
			lane.pending = false
			runnableClasses = append(runnableClasses, lane.class)
		}
	}
	return runnableClasses
}

func (r *senderLaneRegistry) DefaultReservedCursor() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.defaultReservedCursor
}

// ResumeFloor returns the lowest resume point across lanes, with the ack time of
// the binding lane. The time is zero when the binding lane has never been acked:
// its resume point is its scope floor, which has no associated ack.
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

// ClearLanes drops all lane state. The sender calls it once the receiver proves it
// does not understand lanes: the default lane re-covers the lane ranges from their
// resume floor, and the receiver's acks are truthful resume points, so the lanes
// are redundant from that point on.
func (r *senderLaneRegistry) ClearLanes() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.byKey)
	clear(r.byID)
}

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
		resume := max(lane.scope.Range.InclusiveMin.TaskID, lane.acked)
		scope := lane.scope
		scope.Range.InclusiveMin = tasks.NewImmediateKey(resume)
		state.Lanes = append(state.Lanes, &persistencespb.QueueReaderLane{
			LogicalKey:   logicalKey,
			Scope:        queues.ToPersistenceScope(scope),
			ServiceClass: int32(lane.class),
		})
		state.Scopes[readerOverallScopeIndex].Range.InclusiveMin.TaskId = min(
			state.Scopes[readerOverallScopeIndex].Range.InclusiveMin.TaskId,
			resume,
		)
		// Older servers cannot read Lanes, so their HIGH scan must re-cover every
		// outstanding lane range after a rollback.
		state.Scopes[readerHighPriorityScopeIndex].Range.InclusiveMin.TaskId = min(
			state.Scopes[readerHighPriorityScopeIndex].Range.InclusiveMin.TaskId,
			resume,
		)
	}
	return state
}

func namespaceLaneScope(namespaceID string, floor int64) queues.Scope {
	return queues.NewScope(
		queues.NewRange(tasks.NewImmediateKey(floor), tasks.NewImmediateKey(math.MaxInt64)),
		tasks.NewNamespacePredicate([]string{namespaceID}),
	)
}
