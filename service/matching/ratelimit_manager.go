package matching

import (
	"math"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	rateLimitManager struct {
		mu sync.Mutex

		effectiveRPS    float64                 // Min of api/worker set RPS and system defaults.
		rateLimitSource enumspb.RateLimitSource // Source of the rate limit, can be set via API, worker or system default.
		userDataManager userDataManager         // User data manager to fetch user data for task queue configurations.
		workerRPS       *wrapperspb.DoubleValue // RPS set by worker at the time of polling, if available.
		apiConfigRPS    *wrapperspb.DoubleValue // RPS set via API, if available.
		config          *taskQueueConfig        // Dynamic configuration for task queues set by system.
		taskQueueType   enumspb.TaskQueueType   // Task queue type

		// dynamicRate is the dynamic rate & burst for rate limiter
		dynamicRateBurst quotas.MutableRateBurst
		// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
		dynamicRateLimiter *quotas.DynamicRateLimiterImpl
		// forceRefreshRateOnce is used to force refresh rate limit for first time
		forceRefreshRateOnce sync.Once
		// rateLimiter that limits the rate at which tasks can be dispatched to consumers
		rateLimiter quotas.RateLimiter
	}
)

// Create a new rate limit manager for the task queue partition.
func newRateLimitManager(userDataManager userDataManager,
	tqConfig *taskQueueConfig,
	tqType enumspb.TaskQueueType,
) *rateLimitManager {
	r := &rateLimitManager{
		userDataManager: userDataManager,
		workerRPS:       nil,
		apiConfigRPS:    nil,
		config:          tqConfig,
		rateLimitSource: enumspb.RATE_LIMIT_SOURCE_SYSTEM,
		taskQueueType:   tqType,
	}
	r.dynamicRateBurst = quotas.NewMutableRateBurst(
		defaultTaskDispatchRPS,
		int(defaultTaskDispatchRPS),
	)
	r.dynamicRateLimiter = quotas.NewDynamicRateLimiter(
		r.dynamicRateBurst,
		tqConfig.RateLimiterRefreshInterval,
	)
	r.rateLimiter = quotas.NewMultiRateLimiter([]quotas.RateLimiter{
		r.dynamicRateLimiter,
		quotas.NewDefaultOutgoingRateLimiter(
			tqConfig.AdminNamespaceTaskQueueToPartitionDispatchRate,
		),
		quotas.NewDefaultOutgoingRateLimiter(
			tqConfig.AdminNamespaceToPartitionDispatchRate,
		),
	})
	r.computeEffectiveRPSAndSource()
	return r
}

// Compute the effective RPS and source based on the task dispatch rate.
// This method sets the `effectiveRPS` and `rateLimitSource` based on the provided `taskDispatchRate`
// `taskDispatchRate` can be the rate from API, worker or system defaults.
func (r *rateLimitManager) computeEffectiveRPSAndSource() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.TrySetRPSFromUserData()
	systemRPS := min(
		r.config.AdminNamespaceTaskQueueToPartitionDispatchRate(),
		r.config.AdminNamespaceToPartitionDispatchRate(),
	)

	// Default to MaxFloat64 so systemRPS is chosen unless API or worker config is set.
	effectiveRPS := math.MaxFloat64
	rateLimitSource := enumspb.RATE_LIMIT_SOURCE_SYSTEM

	switch {
	case r.apiConfigRPS != nil:
		effectiveRPS = r.apiConfigRPS.GetValue()
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_API
	case r.workerRPS != nil:
		effectiveRPS = r.workerRPS.GetValue()
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_WORKER
	}

	if effectiveRPS < systemRPS {
		r.effectiveRPS = effectiveRPS
		r.rateLimitSource = rateLimitSource
	} else {
		r.effectiveRPS = systemRPS
		r.rateLimitSource = enumspb.RATE_LIMIT_SOURCE_SYSTEM
	}
}

// Lazy injection of poll metadata.
func (r *rateLimitManager) SetWorkerRPS(meta *pollMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if meta == nil || meta.taskQueueMetadata == nil {
		// If no metadata is provided then no worker RPS is set
		return
	}
	r.workerRPS = meta.taskQueueMetadata.GetMaxTasksPerSecond()
}

// Return the effective RPS and its source together.
// The source can be API, worker or system default.
// By default, it returns RATE_LIMIT_SOURCE_SYSTEM (default value).
func (r *rateLimitManager) GetEffectiveRPSAndSource() (float64, enumspb.RateLimitSource) {
	r.computeEffectiveRPSAndSource()
	return r.effectiveRPS, r.rateLimitSource
}

func (r *rateLimitManager) GetRateLimiter() quotas.RateLimiter {
	return r.rateLimiter
}

// SelectTaskQueueRateLimiter returns the user defined RPS (configured RPS) if exists/system default along with an update flag.
// Configured RPS may or may not be equal to the effective RPS.
// If the RPS set via API or workerOptions is lesser than the system defaults,
// only then configured RPS will be equal to effective RPS.
func (r *rateLimitManager) SelectTaskQueueRateLimiter(tqType enumspb.TaskQueueType) (*wrapperspb.DoubleValue, bool) {
	// Try setting RPS from user data (API-configured rate limit).
	r.computeEffectiveRPSAndSource()
	return wrapperspb.Double(r.effectiveRPS), false
}

// TrySetRPSFromUserData attempts to set effectiveRPS from user data.
// Returns true if RPS was set from user data, false otherwise.
func (r *rateLimitManager) TrySetRPSFromUserData() {
	userData, _, err := r.userDataManager.GetUserData()
	if userData == nil || err != nil {
		return
	}
	taskQueueData := userData.GetData()
	if taskQueueData == nil || taskQueueData.GetPerType() == nil {
		return
	}
	taskQueueTypeData := taskQueueData.GetPerType()[int32(r.taskQueueType)]
	if taskQueueTypeData == nil || taskQueueTypeData.GetConfig() == nil {
		return
	}
	// If rate limit is an empty message, it means rate limit could have been unset via API.
	// In this case, the apiConfigRPS will need to be unset.
	rateLimit := taskQueueTypeData.Config.GetQueueRateLimit()
	if rateLimit == nil || rateLimit.GetRateLimit() == nil {
		r.apiConfigRPS = nil
		return
	}
	r.apiConfigRPS = wrapperspb.Double(float64(rateLimit.GetRateLimit().GetRequestsPerSecond()))
}

// UpdateRatelimit updates the task dispatch rate
func (r *rateLimitManager) UpdateRatelimit() {
	oldRPS := r.effectiveRPS
	r.computeEffectiveRPSAndSource()
	if oldRPS == r.effectiveRPS {
		// No update required
		return
	}
	effectiveRPS := r.effectiveRPS
	nPartitions := float64(r.config.NumReadPartitions())
	if nPartitions > 0 {
		// divide the rate equally across all partitions
		effectiveRPS = effectiveRPS / nPartitions
	}
	burst := int(math.Ceil(effectiveRPS))

	minTaskThrottlingBurstSize := r.config.MinTaskThrottlingBurstSize()
	if burst < minTaskThrottlingBurstSize {
		burst = minTaskThrottlingBurstSize
	}

	r.dynamicRateBurst.SetRPS(effectiveRPS)
	r.dynamicRateBurst.SetBurst(burst)
	r.forceRefreshRateOnce.Do(func() {
		// Dynamic rate limiter only refresh its rate every 1m. Before that initial 1m interval, it uses default rate
		// which is 10K and is too large in most cases. We need to force refresh for the first time this rate is set
		// by poller. Only need to do that once. If the rate change later, it will be refresh in 1m.
		r.dynamicRateLimiter.Refresh()
	})
}
