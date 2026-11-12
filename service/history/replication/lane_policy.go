//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination lane_policy_mock.go

package replication

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/anypb"
)

// namespaceLaneKeyPrefix starts the keys of the open-source policy's lanes, one per namespace.
const namespaceLaneKeyPrefix = "namespace:"

const (
	// laneFilterMaxNamespaces is the most namespaces one lane's filter may name. Every lane's
	// filter is saved with the shard's queue state.
	laneFilterMaxNamespaces = 100
	// lanePolicyStateMaxBytes is the most policy state one lane may save. It is saved with the
	// shard's queue state.
	lanePolicyStateMaxBytes = 256
)

type (
	// LaneStreamPolicyFactory creates the lane policy of each replication stream that the lane
	// stream sender runs.
	LaneStreamPolicyFactory interface {
		// NewStreamPolicy is called on a stream's first ack from a receiver that supports
		// lanes. If it fails or panics, the stream runs without lanes until it restarts.
		NewStreamPolicy(stream LaneStream) (LaneStreamPolicy, error)
	}

	// LaneStream identifies a replication stream: the one from the local shard ShardID to
	// RemoteCluster.
	LaneStream struct {
		RemoteCluster string
		ShardID       int32
		// RemoteShardID identifies the receiving shard, which may differ from ShardID.
		RemoteShardID int32
	}

	// LaneStreamPolicy decides the lanes of one stream. The stream never calls it from two
	// goroutines at once, and it must answer from memory, without blocking or I/O. Whatever it
	// answers, the stream enforces lane limits, valid and disjoint filters, and lifts lane rate
	// limits during a handover. If a method or one of the policy's limiters panics, the stream
	// calls nothing more on the policy except Close, and runs without lanes until it restarts.
	LaneStreamPolicy interface {
		// Evaluate returns the lanes the stream should have, in priority order: when there
		// are more than the stream allows, the first ones win. The stream calls it on each
		// receiver ack. A lane whose key is missing from the answer catches up without a rate
		// limit and then retires. A lane's filter is fixed when the lane is created: to change
		// it, retire the key and use a new one. Once requested, retirement finishes even if
		// the policy asks for the key again; a new lane can then use it.
		// Each lane scans the shard queue independently, so policies should keep lane counts
		// bounded. The optional read buffer shares recent reads, not old backlog scans.
		// An error counts as an answer with no lanes; the stream asks again on the next ack.
		// To keep its lanes through a short failure, a policy returns its last answer instead.
		Evaluate(signals LaneSignals) ([]DesiredLane, error)
		// Limiter returns the limiter that paces a lane. The stream calls it once per lane,
		// when the lane is first desired.
		Limiter(lane string) LaneLimiter
		// LaneRetired reports that a lane has drained and retired.
		LaneRetired(lane string)
		// Close reports that the stream has ended. The stream calls no method after Close. If
		// a call is running when the stream ends, the stream calls Close when it returns.
		Close()
	}

	// DesiredLane is a lane that a LaneStreamPolicy wants on its stream.
	DesiredLane struct {
		// Key identifies the lane across restarts. The policy chooses its format.
		Key string
		// SchedulingClass selects one of 16 bounded execution groups in the pooled
		// scheduler. It does not select a rate, and is not persisted. Zero is the
		// default group. Policy limiters retain admission authority across changes.
		SchedulingClass int
		// Filter selects the HIGH priority tasks the lane carries.
		Filter LaneFilter
		// PolicyState is saved with the lane and returned in LaneStatus, so that the policy
		// can resume the lane after the shard moves. At most 256 bytes.
		PolicyState []byte
	}

	// LaneFilter selects the tasks of a lane.
	LaneFilter struct {
		// NamespaceIDs are the namespaces whose tasks the lane carries: from 1 to 100.
		NamespaceIDs []namespace.ID
	}

	// LaneSignals are the inputs to LaneStreamPolicy.Evaluate.
	LaneSignals struct {
		Now time.Time
		// ThrottledNamespaceIDs are the namespaces the receiver reports as overwhelming this
		// stream's HIGH priority traffic.
		ThrottledNamespaceIDs []string
		// Feedback is the receiver's feedback for lane policies, or nil.
		Feedback *anypb.Any
		// ReceiverMaxLanes is the most lanes the receiver accepts on this stream.
		ReceiverMaxLanes int
		// SourceHighWatermark is the exclusive end of this shard's replication tasks.
		SourceHighWatermark int64
		// DefaultAcked is the receiver's acknowledged HIGH priority position. New lanes start
		// here.
		DefaultAcked int64
		// HandoverInProgress is whether a namespace handover is in progress on the shard.
		// Lanes then send without rate limits, whatever the policy answers.
		HandoverInProgress bool
		// Lanes are the stream's current lanes, including retiring ones.
		Lanes []LaneStatus
	}

	// LaneStatus describes a lane on the stream.
	LaneStatus struct {
		Key             string
		Filter          LaneFilter
		SchedulingClass int
		// SentCursor is the exclusive end of the tasks the lane has sent.
		SentCursor int64
		// Acked is the receiver's acknowledged position on the lane, and AckedTime the time
		// of the task at that position. AckedTime is zero until the first ack.
		Acked     int64
		AckedTime time.Time
		// Retiring is whether retirement has been requested, including while the lane drains.
		Retiring bool
		// LastRefused is when the lane's limiter last refused a send, or zero if it never did.
		// The policy chooses how long that observation remains relevant.
		LastRefused time.Time
		// Paused is whether the receiver has paused the lane.
		Paused bool
		// PolicyState is the state last saved with the lane.
		PolicyState []byte
	}

	// LaneLimiter paces a lane without blocking. The lane stream sender may call it from
	// several goroutines at once.
	LaneLimiter interface {
		// Reserve takes the budget to send one task, or reports how long the lane should wait
		// before asking again. It must answer from memory, without blocking.
		Reserve(now time.Time) (ok bool, retryAfter time.Duration)
	}

	// LaneLimiterReadiness lets the pooled scheduler avoid a flow-control wait
	// for a lane that cannot send yet. Ready must not spend or hold credit.
	// Like Reserve, it must answer from memory without blocking and support concurrent calls.
	// A limiter without this optional capability is checked after flow control.
	LaneLimiterReadiness interface {
		Ready(now time.Time) (ok bool, retryAfter time.Duration)
	}

	manualLaneStreamPolicyFactory struct {
		entries dynamicconfig.TypedPropertyFn[[]dynamicconfig.ReplicationLaneIsolationEntry]
	}

	// manualLaneStreamPolicy gives each namespace listed for its remote cluster in the
	// ReplicationLaneIsolation dynamic config its own lane, at the listed rate per shard.
	manualLaneStreamPolicy struct {
		remoteCluster string
		entries       dynamicconfig.TypedPropertyFn[[]dynamicconfig.ReplicationLaneIsolationEntry]
		// ratesMu guards rates, which the lanes' limiters read.
		ratesMu sync.Mutex
		rates   map[string]float64
	}

	// rateLaneLimiter is a token bucket at a rate that may change. It applies a new rate on the
	// next reservation and keeps its tokens, so a change never lets a burst through. A rate of
	// zero or less means no limit.
	rateLaneLimiter struct {
		rate func() float64
		mu   sync.Mutex
		// limiter is nil until the first limited reservation, so that the bucket starts full.
		limiter *rate.Limiter
	}
)

func namespaceLaneKey(namespaceID namespace.ID) string {
	return namespaceLaneKeyPrefix + namespaceID.String()
}

// namespaceLaneFilter selects one namespace's tasks.
func namespaceLaneFilter(namespaceID namespace.ID) LaneFilter {
	return LaneFilter{NamespaceIDs: []namespace.ID{namespaceID}}
}

// NewManualLaneStreamPolicyFactory returns a factory whose policies isolate the namespaces
// listed in the ReplicationLaneIsolation dynamic config.
func NewManualLaneStreamPolicyFactory(dc *dynamicconfig.Collection) LaneStreamPolicyFactory {
	return manualLaneStreamPolicyFactory{entries: dynamicconfig.ReplicationLaneIsolation.Get(dc)}
}

func (f manualLaneStreamPolicyFactory) NewStreamPolicy(stream LaneStream) (LaneStreamPolicy, error) {
	return &manualLaneStreamPolicy{remoteCluster: stream.RemoteCluster, entries: f.entries}, nil
}

// Evaluate returns a lane for each listed namespace, ordered by namespace ID. If the config is
// invalid, it returns an error and no lanes.
func (p *manualLaneStreamPolicy) Evaluate(LaneSignals) ([]DesiredLane, error) {
	entries := p.entries()
	if err := validateLaneIsolationEntries(entries); err != nil {
		p.setRates(nil)
		return nil, fmt.Errorf("invalid ReplicationLaneIsolation: %w", err)
	}
	var lanes []DesiredLane
	rates := make(map[string]float64)
	for _, entry := range entries {
		if entry.RemoteCluster != p.remoteCluster {
			continue
		}
		namespaceID := namespace.ID(entry.NamespaceID)
		key := namespaceLaneKey(namespaceID)
		lanes = append(lanes, DesiredLane{Key: key, Filter: namespaceLaneFilter(namespaceID)})
		rates[key] = entry.TasksPerSecond
	}
	slices.SortFunc(lanes, func(a, b DesiredLane) int { return cmp.Compare(a.Key, b.Key) })
	p.setRates(rates)
	return lanes, nil
}

func (p *manualLaneStreamPolicy) setRates(rates map[string]float64) {
	p.ratesMu.Lock()
	defer p.ratesMu.Unlock()
	p.rates = rates
}

func (p *manualLaneStreamPolicy) rate(lane string) float64 {
	p.ratesMu.Lock()
	defer p.ratesMu.Unlock()
	return p.rates[lane]
}

func (p *manualLaneStreamPolicy) Limiter(lane string) LaneLimiter {
	return newRateLaneLimiter(func() float64 { return p.rate(lane) })
}

func (*manualLaneStreamPolicy) LaneRetired(string) {}

func (*manualLaneStreamPolicy) Close() {}

func validateLaneIsolationEntries(entries []dynamicconfig.ReplicationLaneIsolationEntry) error {
	type entryKey struct {
		remoteCluster string
		namespaceID   string
	}
	seen := make(map[entryKey]struct{}, len(entries))
	for i, entry := range entries {
		if entry.RemoteCluster == "" {
			return fmt.Errorf("entry %d: RemoteCluster is empty", i)
		}
		if entry.NamespaceID == "" {
			return fmt.Errorf("entry %d: NamespaceID is empty", i)
		}
		if !(entry.TasksPerSecond > 0) || math.IsInf(entry.TasksPerSecond, 1) {
			return fmt.Errorf("entry %d: TasksPerSecond %v is not a positive finite number", i, entry.TasksPerSecond)
		}
		key := entryKey{remoteCluster: entry.RemoteCluster, namespaceID: entry.NamespaceID}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("entry %d: more than one entry for namespace %q and RemoteCluster %q", i, entry.NamespaceID, entry.RemoteCluster)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func newRateLaneLimiter(tasksPerSecond func() float64) *rateLaneLimiter {
	return &rateLaneLimiter{rate: tasksPerSecond}
}

func (l *rateLaneLimiter) Reserve(now time.Time) (bool, time.Duration) {
	return l.reserve(now, true)
}

func (l *rateLaneLimiter) Ready(now time.Time) (bool, time.Duration) {
	return l.reserve(now, false)
}

func (l *rateLaneLimiter) reserve(now time.Time, debit bool) (bool, time.Duration) {
	tasksPerSecond := l.rate()
	if tasksPerSecond <= 0 {
		return true, 0
	}
	limit := rate.Limit(tasksPerSecond)
	// The bucket holds one second of tasks, and at least one.
	burst := max(1, int(tasksPerSecond))
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limiter == nil {
		l.limiter = rate.NewLimiter(limit, burst)
	} else if l.limiter.Limit() != limit {
		l.limiter.SetLimitAt(now, limit)
		l.limiter.SetBurstAt(now, burst)
	}
	reservation := l.limiter.ReserveN(now, 1)
	if !reservation.OK() {
		return false, time.Second
	}
	delay := reservation.DelayFrom(now)
	if delay <= 0 {
		if !debit {
			reservation.CancelAt(now)
		}
		return true, 0
	}
	// Return the token rather than holding it: the lane asks again after the delay.
	reservation.CancelAt(now)
	return false, delay
}
