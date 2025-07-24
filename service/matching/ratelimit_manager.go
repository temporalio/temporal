package matching

import (
	"math"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/quotas"
)

type (
	rateLimitManager struct {
		mu sync.Mutex

		effectiveRPS    *float64                // Min of api/worker set RPS and system defaults. Always reflects the overall task queue RPS.
		rateLimitSource enumspb.RateLimitSource // Source of the rate limit, can be set via API, worker or system default.
		userDataManager userDataManager         // User data manager to fetch user data for task queue configurations.
		workerRPS       *float64                // RPS set by worker at the time of polling, if available.
		apiConfigRPS    *float64                // RPS set via API, if available.
		systemRPS       *float64                // min of partition level dispatch rates times the number of read partitions.
		config          *taskQueueConfig        // Dynamic configuration for task queues set by system.
		taskQueueType   enumspb.TaskQueueType   // Task queue type

		// dynamicRate is the dynamic rate & burst for rate limiter
		dynamicRateBurst quotas.MutableRateBurst
		// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
		dynamicRateLimiter *quotas.DynamicRateLimiterImpl
		// forceRefreshRateOnce is used to force refresh rate limit for first time
		forceRefreshRateOnce sync.Once
	}
)

// Create a new rate limit manager for the task queue partition.
func newRateLimitManager(userDataManager userDataManager,
	config *taskQueueConfig,
	taskQueueType enumspb.TaskQueueType,
) *rateLimitManager {
	r := &rateLimitManager{
		userDataManager: userDataManager,
		config:          config,
		taskQueueType:   taskQueueType,
	}
	r.dynamicRateBurst = quotas.NewMutableRateBurst(
		defaultTaskDispatchRPS,
		int(defaultTaskDispatchRPS),
	)
	r.dynamicRateLimiter = quotas.NewDynamicRateLimiter(
		r.dynamicRateBurst,
		config.RateLimiterRefreshInterval,
	)
	// Overall system rate limit will be the min of the two configs that are partition wise times the number of partions.
	systemRPS := min(
		config.AdminNamespaceTaskQueueToPartitionDispatchRate(),
		config.AdminNamespaceToPartitionDispatchRate(),
	) * r.getNumberOfReadPartitions()
	r.systemRPS = &(systemRPS)
	r.computeEffectiveRPSAndSource()
	return r
}

func (r *rateLimitManager) getNumberOfReadPartitions() float64 {
	nPartitions := float64(r.config.NumReadPartitions())
	if nPartitions <= 0 {
		// Defaulting to 1 partition if misconfigured
		nPartitions = 1
	}
	return nPartitions
}

func (r *rateLimitManager) computeEffectiveRPSAndSource() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.computeEffectiveRPSAndSourceLocked()
}

// Computes the effectiveRPS and its source by evaluating all possible rate limit configurations.
// - If an API-level RPS is configured, effectiveRPS = min(system default RPS, API-configured RPS)
// - Else if a worker-level RPS is configured, effectiveRPS = min(system default RPS, worker-configured RPS)
// - Otherwise, fall back to the system default RPS from dynamic config.
func (r *rateLimitManager) computeEffectiveRPSAndSourceLocked() {

	var (
		effectiveRPS    *float64
		rateLimitSource enumspb.RateLimitSource
	)

	switch {
	case r.apiConfigRPS != nil:
		effectiveRPS = r.apiConfigRPS
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_API
	case r.workerRPS != nil:
		effectiveRPS = r.workerRPS
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_WORKER
	}

	if effectiveRPS != nil && *effectiveRPS < *r.systemRPS {
		r.effectiveRPS = effectiveRPS
		r.rateLimitSource = rateLimitSource
	} else {
		r.effectiveRPS = r.systemRPS
		r.rateLimitSource = enumspb.RATE_LIMIT_SOURCE_SYSTEM
	}
}

// Lazy injection of poll metadata.
// Internally call UpdateRateLimit to share the same mutex.
func (r *rateLimitManager) InjectWorkerRPS(meta *pollMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var rps *float64
	if meta != nil && meta.taskQueueMetadata != nil {
		if workerRPS := meta.taskQueueMetadata.GetMaxTasksPerSecond(); workerRPS != nil {
			value := workerRPS.GetValue()
			rps = &value
		}
	}
	r.workerRPS = rps
	// updateRatelimitLocked includes internal logic to determine if an update is needed,
	// so calling it unconditionally is safe and avoids redundant updates.
	r.updateRatelimitLocked()
}

// Return the effective RPS and its source together.
// The source can be API, worker or system default.
// It defaults to the system dynamic config.
func (r *rateLimitManager) GetEffectiveRPSAndSource() (*float64, enumspb.RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.computeEffectiveRPSAndSourceLocked()
	return r.effectiveRPS, r.rateLimitSource
}

func (r *rateLimitManager) GetRateLimiter() quotas.RateLimiter {
	return r.dynamicRateLimiter
}

// Updates the API-configured RPS based on the latest user data
// and applies the new rate limit if the effective RPS has changed.
func (r *rateLimitManager) UserDataChanged() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Immediately recompute and apply rate limit if necessary.
	r.updateRatelimitLocked()
}

// trySetRPSFromUserDataLocked sets the apiConfigRPS from user data.
func (r *rateLimitManager) trySetRPSFromUserDataLocked() {
	userData, _, err := r.userDataManager.GetUserData()
	if err != nil {
		return
	}
	config := userData.GetData().GetPerType()[int32(r.taskQueueType)].GetConfig()
	if config == nil {
		return
	}
	// If rate limit is an empty message, it means rate limit could have been unset via API.
	// In this case, the apiConfigRPS will need to be unset.
	rateLimit := config.GetQueueRateLimit()
	if rateLimit.GetRateLimit() == nil {
		r.apiConfigRPS = nil
		return
	}
	val := float64(rateLimit.GetRateLimit().GetRequestsPerSecond())
	r.apiConfigRPS = &val
}

func (r *rateLimitManager) getEffectiveRateLimitPartitionWiseLocked() float64 {
	return (*r.effectiveRPS) / r.getNumberOfReadPartitions()
}

// updateRatelimitLocked checks and updates the rate limit if changed.
func (r *rateLimitManager) updateRatelimitLocked() {
	// Always check for api configured rate limits before any update to the rate limits.
	r.trySetRPSFromUserDataLocked()
	oldRPS := r.effectiveRPS
	r.computeEffectiveRPSAndSourceLocked()
	newRPS := r.effectiveRPS
	if *oldRPS == *newRPS {
		// No update required
		return
	}
	effectiveRPSPartitionWise := r.getEffectiveRateLimitPartitionWiseLocked()
	burst := int(math.Ceil(effectiveRPSPartitionWise))
	burst = max(burst, r.config.MinTaskThrottlingBurstSize())
	r.dynamicRateBurst.SetRPS(effectiveRPSPartitionWise)
	r.dynamicRateBurst.SetBurst(burst)
	r.forceRefreshRateOnce.Do(func() {
		// Dynamic rate limiter only refresh its rate every 1m. Before that initial 1m interval, it uses default rate
		// which is 10K and is too large in most cases. We need to force refresh for the first time this rate is set
		// by poller. Only need to do that once. If the rate change later, it will be refresh in 1m.
		r.dynamicRateLimiter.Refresh()
	})
}
