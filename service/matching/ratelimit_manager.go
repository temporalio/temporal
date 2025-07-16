package matching

import (
	"math"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	rateLimitManagerImpl struct {
		effectiveRPS    float64                 // Min of api/worker set RPS and system defaults.
		rateLimitSource int32                   // Source of the rate limit, can be set via API, worker or system default.
		userDataManager userDataManager         // User data manager to fetch user data for task queue configurations.
		workerRPS       *wrapperspb.DoubleValue // RPS set by worker at the time of polling, if available.
		apiConfigRPS    *wrapperspb.DoubleValue // RPS set via API, if available.
		config          *taskQueueConfig        // Dynamic configuration for task queues set by system.

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

var _ rateLimitManager = (*rateLimitManagerImpl)(nil)

// Create a new rate limit manager for the task queue partition.
// `workerRPS` is expected to be nil and will be set lazily via SetWorkerRPS at the time of worker poll.
// `apiConfigRPS` will be set to nil initially and will get populated once a poll / describeTaskQueue request is called.
func newRateLimitManager(userDataManager userDataManager,
	workerRPS *wrapperspb.DoubleValue, apiConfigRPS *wrapperspb.DoubleValue, tqConfig *taskQueueConfig) *rateLimitManagerImpl {
	r := &rateLimitManagerImpl{
		userDataManager: userDataManager,
		workerRPS:       workerRPS,
		apiConfigRPS:    apiConfigRPS,
		config:          tqConfig,
		rateLimitSource: int32(enumspb.RATE_LIMIT_SOURCE_SYSTEM),
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
	r.computeEffectiveRPSAndSource(defaultTaskDispatchRPS)
	return r
}

// Compute the effective RPS and source based on the task dispatch rate.
// This method sets the `effectiveRPS` and `rateLimitSource` based on the provided `taskDispatchRate`
// `taskDispatchRate` can be the rate from API, worker or system defaults.
func (r *rateLimitManagerImpl) computeEffectiveRPSAndSource(taskDispatchRate float64) {
	systemRPS := min(
		r.config.AdminNamespaceTaskQueueToPartitionDispatchRate(),
		r.config.AdminNamespaceToPartitionDispatchRate(),
	)

	apiRPS := math.MaxFloat64
	if r.apiConfigRPS != nil {
		apiRPS = r.apiConfigRPS.GetValue()
	}

	workerRPS := math.MaxFloat64
	if r.workerRPS != nil {
		workerRPS = r.workerRPS.GetValue()
	}

	// Compute effective RPS from all sources
	r.effectiveRPS = min(taskDispatchRate, systemRPS)

	// Determine the source
	switch r.effectiveRPS {
	case apiRPS:
		r.rateLimitSource = int32(enumspb.RATE_LIMIT_SOURCE_API)
	case workerRPS:
		r.rateLimitSource = int32(enumspb.RATE_LIMIT_SOURCE_WORKER)
	default:
		r.rateLimitSource = int32(enumspb.RATE_LIMIT_SOURCE_SYSTEM)
	}
}

// Lazy injection of poll metadata.
func (r *rateLimitManagerImpl) SetWorkerRPS(meta *pollMetadata) {
	if meta == nil || meta.taskQueueMetadata == nil {
		// If no metadata is provided then no worker RPS is set
		return
	}
	r.workerRPS = meta.taskQueueMetadata.GetMaxTasksPerSecond()
}

// Return the source of the effective RPS.
// The source can be API, worker or system default.
// By default, it returns RATE_LIMIT_SOURCE_SYSTEM (default value).
func (r *rateLimitManagerImpl) GetSourceForEffectiveRPS() enumspb.RateLimitSource {
	return enumspb.RateLimitSource(r.rateLimitSource)
}

// Return the effective RPS for task queues.
// Whenever the effective RPS is retrieved, it needs to be computed based on the current configuration.
// It checks for API configured RPS, worker set RPS, and computes the effective rate limit.
func (r *rateLimitManagerImpl) GetEffectiveRPS(tqType enumspb.TaskQueueType) float64 {
	if r.TrySetRPSFromUserData(tqType) {
		// If RPS is set via API, it has the highest priority.
		r.computeEffectiveRPSAndSource(r.apiConfigRPS.GetValue())
	} else if r.workerRPS != nil {
		// If worker RPS is set, use it as the configured RPS.
		r.computeEffectiveRPSAndSource(r.workerRPS.GetValue())
	} else {
		// Use defaultTaskDispatchRPS if no specific RPS is set.
		r.computeEffectiveRPSAndSource(defaultTaskDispatchRPS)
	}
	return r.effectiveRPS
}

func (r *rateLimitManagerImpl) GetRateLimiter() quotas.RateLimiter {
	return r.rateLimiter
}

// SelectTaskQueueRateLimiter returns the user defined RPS (configured RPS) if exists/system default along with an update flag.
// Configured RPS may or may not be equal to the effective RPS.
// If the RPS set via API or workerOptions is lesser than the system defaults,
// only then configured RPS will be equal to effective RPS.
func (r *rateLimitManagerImpl) SelectTaskQueueRateLimiter(tqType enumspb.TaskQueueType) (*wrapperspb.DoubleValue, bool) {
	var configuredRPS *wrapperspb.DoubleValue
	var updateRequired bool
	// Try setting RPS from user data (API-configured rate limit).
	if r.TrySetRPSFromUserData(tqType) {
		configuredRPS = r.apiConfigRPS
		updateRequired = true
	} else if r.workerRPS != nil {
		// Fallback to worker-provided metadata if available.
		configuredRPS = r.workerRPS
		updateRequired = true
	} else {
		// If no RPS is set, use the default task dispatch RPS.
		configuredRPS = wrapperspb.Double(defaultTaskDispatchRPS)
		updateRequired = false
	}
	r.computeEffectiveRPSAndSource(configuredRPS.Value)
	return configuredRPS, updateRequired
}

// TrySetRPSFromUserData attempts to set effectiveRPS from user data.
// Returns true if RPS was set from user data, false otherwise.
func (r *rateLimitManagerImpl) TrySetRPSFromUserData(tqType enumspb.TaskQueueType) bool {
	userData, _, err := r.userDataManager.GetUserData()
	if userData == nil || err != nil {
		return false
	}
	taskQueueData := userData.GetData()
	if taskQueueData == nil || taskQueueData.GetPerType() == nil {
		return false
	}
	taskQueueTypeData := taskQueueData.GetPerType()[int32(tqType)]
	if taskQueueTypeData == nil || taskQueueTypeData.GetConfig() == nil {
		return false
	}
	// If rate limit is an empty message, it means rate limit could have been unset via API.
	// In this case, the apiConfigRPS will need to be unset.
	rateLimit := taskQueueTypeData.Config.GetQueueRateLimit()
	if rateLimit == nil || rateLimit.GetRateLimit() == nil {
		r.apiConfigRPS = nil
		return false
	}
	r.apiConfigRPS = wrapperspb.Double(float64(rateLimit.GetRateLimit().GetRequestsPerSecond()))
	return true
}

// UpdateRatelimit updates the task dispatch rate
func (r *rateLimitManagerImpl) UpdateRatelimit(rps float64) {
	nPartitions := float64(r.config.NumReadPartitions())
	if nPartitions > 0 {
		// divide the rate equally across all partitions
		rps = rps / nPartitions
	}
	burst := int(math.Ceil(rps))

	minTaskThrottlingBurstSize := r.config.MinTaskThrottlingBurstSize()
	if burst < minTaskThrottlingBurstSize {
		burst = minTaskThrottlingBurstSize
	}

	r.dynamicRateBurst.SetRPS(rps)
	r.dynamicRateBurst.SetBurst(burst)
	r.forceRefreshRateOnce.Do(func() {
		// Dynamic rate limiter only refresh its rate every 1m. Before that initial 1m interval, it uses default rate
		// which is 10K and is too large in most cases. We need to force refresh for the first time this rate is set
		// by poller. Only need to do that once. If the rate change later, it will be refresh in 1m.
		r.dynamicRateLimiter.Refresh()
	})
}
