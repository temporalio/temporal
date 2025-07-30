package matching

import (
	"math"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/quotas"
)

type (
	rateLimitManager struct {
		mu sync.Mutex

		effectiveRPS    float64                 // Min of api/worker set RPS and system defaults. Always reflects the overall task queue RPS.
		rateLimitSource enumspb.RateLimitSource // Source of the rate limit, can be set via API, worker or system default.
		userDataManager userDataManager         // User data manager to fetch user data for task queue configurations.
		workerRPS       *float64                // RPS set by worker at the time of polling, if available.
		apiConfigRPS    *float64                // RPS set via API, if available.
		fairnessRPS     *float64                // fairnessKeyRateLimitDefault set via API, if available
		systemRPS       float64                 // Min of partition level dispatch rates times the number of read partitions.
		config          *taskQueueConfig        // Dynamic configuration for task queues set by system.
		taskQueueType   enumspb.TaskQueueType   // Task queue type

		timeSource  clock.TimeSource
		adminNsRate float64
		adminTqRate float64

		// dynamicRate is the dynamic rate & burst for rate limiter
		dynamicRateBurst quotas.MutableRateBurst
		// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
		dynamicRateLimiter *quotas.DynamicRateLimiterImpl
		// forceRefreshRateOnce is used to force refresh rate limit for first time
		forceRefreshRateOnce sync.Once

		// Fairness tasks rate limiter.
		wholeQueueLimit simpleLimiterParams
		wholeQueueReady simpleLimiter

		// Rate limiter for individual fairness keys.
		// Note that we currently have only one limit for all keys, which is scaled by the key's
		// weight. If we do this, we can assume that all keys are either at or below their rate
		// limit at the same time, so that if the head of the queue is at its limit, then the rest
		// must also be, and so we don't have to "skip over" the head of the queue due to rate
		// limits. This isn't true in situations where weights have changed in between writing and
		// reading. We'll handle that situation better in the future.
		perKeyLimit      simpleLimiterParams
		perKeyReady      cache.Cache
		perKeyOverrides  fairnessWeightOverrides // TODO(fairness): get this from config
		cancel1, cancel2 func()
	}
)

const (
	// How much rate limit a task queue can use up in an instant. E.g., for a rate of
	// 100/second and burst duration of 2 seconds, the capacity of a bucket-type limiting
	// algorithm would be 200.
	defaultBurstDuration = time.Second
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
		perKeyReady:     cache.New(config.FairnessKeyRateLimitCacheSize(), nil),
		timeSource:      clock.NewRealTimeSource(),
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
	r.adminNsRate, r.cancel1 = config.AdminNamespaceToPartitionRateSub(r.setAdminNsRate)
	r.adminTqRate, r.cancel2 = config.AdminNamespaceTaskQueueToPartitionRateSub(r.setAdminTqRate)
	r.computeEffectiveRPSAndSource()
	return r
}

func (r *rateLimitManager) setAdminNsRate(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adminNsRate = rps
}

func (r *rateLimitManager) setAdminTqRate(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adminTqRate = rps
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
		effectiveRPS    = math.Inf(1)
		rateLimitSource enumspb.RateLimitSource
	)
	// Overall system rate limit will be the min of the two configs that are partition wise times the number of partions.
	systemRPS := min(
		r.adminNsRate,
		r.adminTqRate,
	) * r.getNumberOfReadPartitions()
	r.systemRPS = systemRPS
	switch {
	case r.apiConfigRPS != nil:
		effectiveRPS = *r.apiConfigRPS
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_API
	case r.workerRPS != nil:
		effectiveRPS = *r.workerRPS
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_WORKER
	}

	if effectiveRPS < r.systemRPS {
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
	r.updateSimpleRateLimitLocked(defaultBurstDuration)
}

// Return the effective RPS and its source together.
// The source can be API, worker or system default.
// It defaults to the system dynamic config.
func (r *rateLimitManager) GetEffectiveRPSAndSource() (float64, enumspb.RateLimitSource) {
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
	r.updateSimpleRateLimitLocked(defaultBurstDuration)
	// UpdateTaskQueueConfig api is the single source for fairness per-key rate limit defaults.
	// Fairness per-key rate limits are only updated upon updates to `fairnessKeyRateLimitDefault`.
	r.updatePerKeySimpleRateLimitLocked(defaultBurstDuration)
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
	queueRateLimit := config.GetQueueRateLimit()
	if queueRateLimit.GetRateLimit() == nil {
		r.apiConfigRPS = nil
	} else {
		val := float64(queueRateLimit.GetRateLimit().GetRequestsPerSecond())
		r.apiConfigRPS = &val
	}
	fairnessKeyRateLimitDefault := config.GetFairnessKeysRateLimitDefault()
	if fairnessKeyRateLimitDefault.GetRateLimit() == nil {
		r.fairnessRPS = nil
	} else {
		val := float64(fairnessKeyRateLimitDefault.GetRateLimit().GetRequestsPerSecond())
		r.fairnessRPS = &val
	}
}

// updateRatelimitLocked checks and updates the rate limit if changed.
func (r *rateLimitManager) updateRatelimitLocked() {
	// Always check for api configured rate limits before any update to the rate limits.
	r.trySetRPSFromUserDataLocked()
	oldRPS := r.effectiveRPS
	r.computeEffectiveRPSAndSourceLocked()
	newRPS := r.effectiveRPS
	if oldRPS == newRPS {
		// No update required
		return
	}
	effectiveRPSPartitionWise := r.effectiveRPS / r.getNumberOfReadPartitions()
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

// UpdateSimpleRateLimit updates the overall queue rate limits for the simpleRateLimiter implementation
func (r *rateLimitManager) updateSimpleRateLimitLocked(burstDuration time.Duration) {
	r.trySetRPSFromUserDataLocked()
	r.computeEffectiveRPSAndSourceLocked()
	newRPS := r.effectiveRPS
	// Always update the rate limit when called, even if RPS hasn't changed
	// This ensures that the burst duration is properly applied
	r.wholeQueueLimit = makeSimpleLimiterParams(newRPS, burstDuration)

	// Clip to handle the case where we have increased from a zero or very low limit and had
	// ready times in the far future.
	now := r.timeSource.Now().UnixNano()
	r.wholeQueueReady = r.wholeQueueReady.clip(r.wholeQueueLimit, now, maxTokens)
}

// UpdatePerKeySimpleRateLimit updates the per-key rate limit for the simpleRateLimit implementation
func (r *rateLimitManager) updatePerKeySimpleRateLimitLocked(burstDuration time.Duration) {
	r.trySetRPSFromUserDataLocked()
	if r.fairnessRPS == nil {
		return
	}
	r.computeEffectiveRPSAndSourceLocked()
	if r.fairnessRPS == nil {
		r.clearPerKeyRateLimitsLocked()
		return
	}
	rate := *r.fairnessRPS
	r.perKeyLimit = makeSimpleLimiterParams(rate, burstDuration)

	// Clip to handle the case where we have increased from a zero or very low limit and had
	// ready times in the far future.
	var updates map[string]simpleLimiter
	now := r.timeSource.Now().UnixNano()
	it := r.perKeyReady.Iterator()
	for it.HasNext() {
		e := it.Next()
		sl := e.Value().(simpleLimiter) //nolint:revive
		if clipped := sl.clip(r.perKeyLimit, now, maxTokens); clipped != sl {
			if updates == nil {
				updates = make(map[string]simpleLimiter)
			}
			updates[e.Key().(string)] = clipped
		}
	}
	it.Close()

	// This messes up the LRU order, but we can't avoid that here without adding new
	// functionality to Cache.
	for key, clipped := range updates {
		r.perKeyReady.Put(key, clipped)
	}
}

// clearPerKeyRateLimitsLocked removes all fairness per-key rate limits.
func (r *rateLimitManager) clearPerKeyRateLimitsLocked() {
	it := r.perKeyReady.Iterator()
	for it.HasNext() {
		e := it.Next()
		r.perKeyReady.Delete(e.Key())
	}
	it.Close()

	r.perKeyLimit = simpleLimiterParams{}
}

func (r *rateLimitManager) readyTimeForTask(task *internalTask) simpleLimiter {
	// TODO(pri): after we have task-specific ready time, we can re-enable this
	// if task.isForwarded() {
	// 	// don't count any rate limit for forwarded tasks, it was counted on the child
	// 	return 0
	// }
	ready := r.wholeQueueReady

	if r.perKeyLimit.limited() {
		key := task.getPriority().GetFairnessKey()
		if v := r.perKeyReady.Get(key); v != nil {
			ready = max(ready, v.(simpleLimiter))
		}
	}

	return ready
}

func (r *rateLimitManager) consumeTokens(now int64, task *internalTask, tokens int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumeTokensLocked(now, task, tokens)
}

func (r *rateLimitManager) consumeTokensLocked(now int64, task *internalTask, tokens int64) {
	if task.isForwarded() {
		// don't count any rate limit for forwarded tasks, it was counted on the child
		return
	}

	r.wholeQueueReady = r.wholeQueueReady.consume(r.wholeQueueLimit, now, tokens)

	if r.perKeyLimit.limited() {
		pri := task.getPriority()
		key := pri.GetFairnessKey()
		weight := getEffectiveWeight(r.perKeyOverrides, pri)
		p := r.perKeyLimit
		p.interval = time.Duration(float32(p.interval) / weight) // scale by weight
		var sl simpleLimiter
		if v := r.perKeyReady.Get(key); v != nil {
			sl = v.(simpleLimiter) // nolint:revive
		}
		r.perKeyReady.Put(key, sl.consume(p, now, tokens))
	}
}

func (r *rateLimitManager) Stop() {
	r.cancel1()
	r.cancel2()
}
