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
	// lanePacedWindow is how recently a lane's limiter must have refused a send for the lane
	// to count as paced.
	lanePacedWindow = 3 * time.Second
	// A stream restarts at most laneRestartLimit times in laneRestartWindow because its policy
	// asks to. After that, it retires its lanes instead, so a policy error never stops
	// replication.
	laneRestartLimit  = 3
	laneRestartWindow = 10 * time.Minute
)

// LaneErrorAction is how a lane stream sender reacts to an error from LaneStreamPolicy.Evaluate.
type LaneErrorAction int

const (
	// LaneErrorKeepLastGood keeps the policy's last answer for a grace period, then retires all
	// lanes. It is the reaction to an error that is not a LaneEvaluateError. When the policy has
	// not answered on this stream yet, it retires all lanes at once.
	LaneErrorKeepLastGood LaneErrorAction = iota
	// LaneErrorRetireAll retires all lanes, and keeps asking the policy on later acks.
	LaneErrorRetireAll
	// LaneErrorNoLanes retires all lanes, and stops asking the policy until the stream restarts.
	LaneErrorNoLanes
	// LaneErrorRestartStream restarts the stream. A stream that has restarted too often for its
	// policy reacts as to LaneErrorNoLanes instead.
	LaneErrorRestartStream
)

type (
	// LaneStreamPolicyFactory creates the lane policy of each replication stream that the lane
	// stream sender runs.
	LaneStreamPolicyFactory interface {
		// NewStreamPolicy is called when a stream starts. If it fails, the stream runs without
		// lanes.
		NewStreamPolicy(stream LaneStream) (LaneStreamPolicy, error)
	}

	// LaneStream identifies a replication stream: the one from the local shard ShardID to
	// RemoteCluster.
	LaneStream struct {
		RemoteCluster string
		ShardID       int32
	}

	// LaneStreamPolicy decides the lanes of one stream. The stream never calls it from two
	// goroutines at once, and it must answer from memory, without blocking or I/O. Whatever it
	// answers, the stream enforces lane limits and valid, disjoint filters.
	LaneStreamPolicy interface {
		// Evaluate returns the lanes the stream should have, in priority order: when there
		// are more than the stream allows, the first ones win. The stream calls it on each
		// receiver ack. A lane whose key is missing from the answer catches up without a rate
		// limit and then retires. A lane's filter is fixed when the lane is created: to change
		// it, retire the key and use a new one.
		Evaluate(signals LaneSignals) ([]DesiredLane, error)
		// Limiter returns the limiter that paces a lane. The stream calls it once per lane,
		// when the lane is first desired.
		Limiter(lane string) LaneLimiter
		// LaneRetired reports that a lane has drained and retired.
		LaneRetired(lane string)
		// Close reports that the stream has ended. The stream calls no method after Close.
		Close()
	}

	// DesiredLane is a lane that a LaneStreamPolicy wants on its stream.
	DesiredLane struct {
		// Key identifies the lane across restarts. The policy chooses its format.
		Key string
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
		// Lanes are the stream's current lanes, including retiring ones.
		Lanes []LaneStatus
	}

	// LaneStatus describes a lane on the stream.
	LaneStatus struct {
		Key    string
		Filter LaneFilter
		// SentCursor is the exclusive end of the tasks the lane has sent.
		SentCursor int64
		// Acked is the receiver's acknowledged position on the lane, and AckedTime the time
		// of the task at that position. AckedTime is zero until the first ack.
		Acked     int64
		AckedTime time.Time
		// Retiring is whether the lane is catching up to retire.
		Retiring bool
		// Paced is whether the lane's limiter refused a send in the last few seconds.
		Paced bool
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

	// LaneEvaluateError lets a LaneStreamPolicy choose how the stream reacts to its error.
	LaneEvaluateError struct {
		Err    error
		Action LaneErrorAction
		// Grace replaces ReplicationLaneEvaluateErrorGrace for LaneErrorKeepLastGood when it is
		// positive.
		Grace time.Duration
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
		// Waiting does not fix the config, so retire the lanes at once.
		return nil, &LaneEvaluateError{Err: err, Action: LaneErrorRetireAll}
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

func (e *LaneEvaluateError) Error() string {
	return e.Err.Error()
}

func (e *LaneEvaluateError) Unwrap() error {
	return e.Err
}

func newRateLaneLimiter(rate func() float64) *rateLaneLimiter {
	return &rateLaneLimiter{rate: rate}
}

func (l *rateLaneLimiter) Reserve(now time.Time) (bool, time.Duration) {
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
		return true, 0
	}
	// Return the token rather than holding it: the lane asks again after the delay.
	reservation.CancelAt(now)
	return false, delay
}
