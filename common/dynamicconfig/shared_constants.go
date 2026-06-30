package dynamicconfig

import (
	"time"

	"go.temporal.io/server/common/primitives"
)

const GlobalDefaultNumTaskQueuePartitions = 4

var defaultNumTaskQueuePartitions = []TypedConstrainedValue[int]{
	// The per-ns worker task queue in all namespaces should only have one partition, since
	// we'll only run one worker per task queue.
	{
		Constraints: Constraints{
			TaskQueueName: primitives.PerNSWorkerTaskQueue,
		},
		Value: 1,
	},

	// The system activity worker task queues in the system local namespace should only have
	// one partition, since we'll only run one worker per task queue.
	{
		Constraints: Constraints{
			TaskQueueName: primitives.AddSearchAttributesActivityTQ,
			Namespace:     primitives.SystemLocalNamespace,
		},
		Value: 1,
	},
	{
		Constraints: Constraints{
			TaskQueueName: primitives.DeleteNamespaceActivityTQ,
			Namespace:     primitives.SystemLocalNamespace,
		},
		Value: 1,
	},
	{
		Constraints: Constraints{
			TaskQueueName: primitives.MigrationActivityTQ,
			Namespace:     primitives.SystemLocalNamespace,
		},
		Value: 1,
	},

	// TODO: After we have a solution for ensuring no tasks are lost, add a constraint here for
	// all task queues in SystemLocalNamespace to have one partition.

	// Default for everything else:
	{
		Value: GlobalDefaultNumTaskQueuePartitions,
	},
}

var DefaultPerShardNamespaceRPSMax = GetIntPropertyFnFilteredByNamespace(0)

// params for controlling dynamic rate limiting options
type DynamicRateLimitingParams struct {
	// Enabled toggles whether dynamic rate limiting is enabled.
	Enabled bool
	// RefreshInterval is how often the rate limit and dynamic properties are refreshed. Should
	// be a string duratoin e.g. 10s even if the rate limiter is disabled, this property will
	// still determine how often the dynamic config is reevaluated.
	RefreshInterval time.Duration
	// LatencyThreshold is the maximum average latency in ms before the rate limiter should
	// backoff.
	LatencyThreshold float64
	// ErrorThreshold is the maximum ratio of errors:total_requests before the rate limiter
	// Should backoff. Should be between 0 and 1.
	ErrorThreshold float64
	// RateBackoffStepSize is the amount the rate limit multiplier is reduced when backing off.
	// Should be between 0 and 1
	RateBackoffStepSize float64
	// RateIncreaseStepSize is the amount the rate limit multiplier is increased when the
	// system is healthy and current rate < max rate. Should be between 0 and 1.
	RateIncreaseStepSize float64
	// RateMultiMin is the minimum the rate limit multiplier can be reduced to.
	RateMultiMin float64
	// RateMultiMax is the maximum the rate limit multiplier can be increased to.
	RateMultiMax float64
}

var DefaultDynamicRateLimitingParams = DynamicRateLimitingParams{
	Enabled:              false,
	RefreshInterval:      10 * time.Second,
	LatencyThreshold:     0.0, // will not do backoff based on latency
	ErrorThreshold:       0.0, // will not do backoff based on errors
	RateBackoffStepSize:  0.3,
	RateIncreaseStepSize: 0.1,
	RateMultiMin:         0.8,
	RateMultiMax:         1.0,
}

// ScheduleInvariantsScannerParams configures the schedule-invariants scanners. The three
// invariant checks are independently toggleable but otherwise share their timing and
// rate-limiting knobs, so they're grouped into a single struct-valued dynamic config.
type ScheduleInvariantsScannerParams struct {
	// OverdueNextActionTimeEnabled enables flagging schedules whose TemporalScheduleNextActionTime
	// lies further in the past than OverdueNextActionTimeTolerance.
	OverdueNextActionTimeEnabled bool
	// StuckOpenEnabled enables flagging schedules that appear stuck open long after their CloseTime.
	StuckOpenEnabled bool
	// UnknownStateEnabled enables flagging running, unpaused schedules with no
	// TemporalScheduleNextActionTime. Ship disabled until TemporalScheduleNextActionTime is known
	// to be backfilled on legacy schedules.
	UnknownStateEnabled bool
	// OverdueNextActionTimeTolerance is how far in the past TemporalScheduleNextActionTime must be
	// before the schedule is flagged.
	OverdueNextActionTimeTolerance time.Duration
	// OverdueNextActionTimeMaxChecksPerNamespace bounds how many overdue schedules the
	// overdue scanner will DescribeSchedule per namespace per scan pass. Schedules beyond
	// the cap are left unchecked for that pass, so a large backlog can't hammer the frontend.
	OverdueNextActionTimeMaxChecksPerNamespace int
	// VisibilityRPS rate-limits visibility calls from the schedule-invariants scanner.
	VisibilityRPS float64
	// ScanInterval is how often each schedule-invariants scanner activity kicks off a fresh scan pass.
	ScanInterval time.Duration
	// StuckOpenIdleTimeBufferMultiplier multiplies the configured schedule IdleTime to set how far
	// past a schedule's idle-close deadline it must be before the stuck-open scanner flags it.
	StuckOpenIdleTimeBufferMultiplier int
}

var DefaultScheduleInvariantsScannerParams = ScheduleInvariantsScannerParams{
	OverdueNextActionTimeEnabled:               false,
	StuckOpenEnabled:                           false,
	UnknownStateEnabled:                        false,
	OverdueNextActionTimeTolerance:             10 * time.Minute,
	OverdueNextActionTimeMaxChecksPerNamespace: 100,
	VisibilityRPS:                              1.0,
	ScanInterval:                               15 * time.Minute,
	StuckOpenIdleTimeBufferMultiplier:          2,
}

type CircuitBreakerSettings struct {
	// MaxRequests: Maximum number of requests allowed to pass through when
	// it is in half-open state (default 1).
	MaxRequests int
	// Interval: Cyclic period in closed state to clear the internal counts;
	// if interval is 0, then it never clears the internal counts (default 0).
	Interval time.Duration
	// Timeout: Period of open state before changing to half-open state (default 60s).`
	Timeout time.Duration
}

type CacheBackgroundEvictSettings struct {
	// Enabled controls whether background purging of expired entries is active. To enable,
	// this must be set to true at process start, but can be dynamically set to false to
	// stop scanning entries.
	Enabled bool
	// LoopInterval is the frequency that a background goroutine scans for expired entries.
	LoopInterval time.Duration
	// MaxEntryPerCall is the max number of entries that are scanned while the cache is locked.
	MaxEntryPerCall int
}

var DefaultHistoryCacheBackgroundEvictSettings = CacheBackgroundEvictSettings{
	Enabled:         false,
	LoopInterval:    1 * time.Minute,
	MaxEntryPerCall: 1024,
}

type PartitionScaleAllowedDrift struct {
	// Delta and Ratio controls how far off client counts can be before we reject an RPC.
	// If the client count is within the delta, it's allowed. Also, if the ratio of
	// client's count / current count is within [1/ratio, ratio], then it's allowed.
	// To always allow: set Delta to a very high number and Ratio to 1.0.
	// To never allow except on exact match: set Delta to 0 and Ratio to 1.0.
	// Allowing more means fewer retries, allowing less means more accurate load balancing.
	Delta int32
	Ratio float32
}

type PartitionScaleManagerSettings struct {
	// MaxRate limits scale change frequency.
	MaxRate float32
	// BatchSize is the size of a batch to send to the partition scaler. (Needs task queue
	// reload.)
	BatchSize int32
	// BackgroundInterval is the interval for background work:
	// - send signals to the scaler even if not a full batch of tasks has been received yet
	// - check drained partition state
	BackgroundInterval time.Duration
	// DrainBufferTime is how long to wait until after scaling down before we can consider
	// draining queues. It's needed because there's a tiny window where tasks may be written
	// after a scale down, since draining state is only checked at the start of an RPC. This
	// should be set to the maximum time of an AddTask call that may write to a backlog. Note
	// that query/nexus tasks will be processed without interruption even after scale down.
	DrainBufferTime time.Duration
	// ShadowModeLogInterval controls how often shadow decisions are logged. If <= 0, shadow mode
	// is disabled and enabled scaler decisions are applied normally. If > 0, the configured scaler
	// is evaluated and logged at that cadence but decisions are not applied. If the partition
	// scaler is disabled, shadow mode does not log.
	ShadowModeLogInterval time.Duration
}

type SimplePartitionScalerSettings struct {
	// If Enabled is false, scaler will remove dynamic scale state and fall back to dynamic
	// config. If Enabled is true but Ups and Downs are empty, dynamic scale state will be
	// preserved and used as-is without changes.
	Enabled bool

	// If non-zero, Fixed will be used as the scaling decision (overrides everything else).
	Fixed int32

	// Ups and Downs control scaling based on add rate: The TargetRate measured over the
	// Interval is used to calculate a target number of partitions. Ups may move the actual
	// partition target higher, Downs may move it lower. Ups take priority.
	//
	// Note the TargetRate for Downs should be lower than for Ups to leave a deadband in the
	// middle for hysteresis (avoid changing when the rate fluctuates above and below a
	// threshold).
	Downs []SimplePartitionScalerThreshold
	Ups   []SimplePartitionScalerThreshold

	// Overall bounds (0 means don't enforce).
	Min int32
	Max int32
}

type SimplePartitionScalerThreshold struct {
	Window     time.Duration // window to measure add rate over
	TargetRate int           // target tasks/second per partition
}
