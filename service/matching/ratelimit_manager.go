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

		// Dependencies
		userDataManager userDataManager       // User data manager to fetch user data for task queue configurations.
		config          *taskQueueConfig      // Dynamic configuration for task queues set by system.
		taskQueueType   enumspb.TaskQueueType // Task queue type
		timeSource      clock.TimeSource

		// Sources of the effective RPS.
		workerRPS                   *float64 // RPS set by worker at the time of polling, if available.
		apiConfigRPS                *float64 // RPS set via API, if available.
		fairnessKeyRateLimitDefault *float64 // per-partition fairnessKeyRateLimitDefault set via API, if available
		adminNsRate                 float64
		adminTqRate                 float64
		numReadPartitions           int

		// Derived from the above sources.
		effectiveRPS    float64                 // Min of api/worker set RPS and system defaults. Always reflects the per-partition wise task queue RPS.
		systemRPS       float64                 // Min of partition level dispatch rates times the number of read partitions.
		rateLimitSource enumspb.RateLimitSource // Source of the rate limit, can be set via API, worker or system default.
		// Derived from the `defaultTaskDispatchRPS`.
		// dynamicRateBurst is the dynamic rate & burst for rate limiter
		dynamicRateBurst quotas.MutableRateBurst
		// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
		dynamicRateLimiter *quotas.DynamicRateLimiterImpl
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
		perKeyLimit     simpleLimiterParams
		perKeyReady     cache.Cache
		perKeyOverrides fairnessWeightOverrides // TODO(fairness): get this from config
		cancels         []func()
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

	r.mu.Lock()
	defer r.mu.Unlock()

	// Overall system rate limit will be the min of the two configs that are partition wise times the number of partitons.
	var cancel func()
	r.adminNsRate, cancel = config.AdminNamespaceToPartitionRateSub(r.setAdminNsRate)
	r.cancels = append(r.cancels, cancel)
	r.adminTqRate, cancel = config.AdminNamespaceTaskQueueToPartitionRateSub(r.setAdminTqRate)
	r.cancels = append(r.cancels, cancel)
	r.numReadPartitions, cancel = config.NumReadPartitionsSub(r.setNumReadPartitions)
	r.cancels = append(r.cancels, cancel)
	r.computeEffectiveRPSAndSourceLocked()

	return r
}

func (r *rateLimitManager) setAdminNsRate(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adminNsRate = rps
	r.computeAndApplyRateLimitLocked()
}

func (r *rateLimitManager) setAdminTqRate(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adminTqRate = rps
	r.computeAndApplyRateLimitLocked()
}

func (r *rateLimitManager) setNumReadPartitions(val int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Defaulting to 1 partition if misconfigured
	r.numReadPartitions = max(val, 1)
	r.computeAndApplyRateLimitLocked()
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
	)
	r.systemRPS = systemRPS
	switch {
	case r.apiConfigRPS != nil:
		effectiveRPS = *r.apiConfigRPS / float64(r.numReadPartitions)
		rateLimitSource = enumspb.RATE_LIMIT_SOURCE_API
	case r.workerRPS != nil:
		effectiveRPS = *r.workerRPS / float64(r.numReadPartitions)
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

func (r *rateLimitManager) computeAndApplyRateLimitLocked() {
	oldRPS := r.effectiveRPS
	r.computeEffectiveRPSAndSourceLocked()
	newRPS := r.effectiveRPS
	// If the effective RPS has changed, we need to update the rate limiters.
	if oldRPS != newRPS {
		r.updateRatelimitLocked()
		r.updateSimpleRateLimitWithBurstLocked(defaultBurstDuration)
	}
	// Internally, checks if the per-key rate limit has changed and updates it accordingly.
	r.updatePerKeySimpleRateLimitWithBurstLocked(defaultBurstDuration)
}

// Lazy injection of poll metadata.
// Called whenever a new poll request comes in.
// This allows the rate limit manager to adjust its rate limits based on any updates right before polling happens.
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
	r.computeAndApplyRateLimitLocked()
}

// Return the effective RPS and its source together.
// The source can be API, worker or system default.
// It defaults to the system dynamic config.
func (r *rateLimitManager) GetEffectiveRPSAndSource() (float64, enumspb.RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.effectiveRPS * float64(r.numReadPartitions), r.rateLimitSource
}

func (r *rateLimitManager) GetRateLimiter() quotas.RateLimiter {
	return r.dynamicRateLimiter
}

// Updates the API-configured RPS based on the latest user data
// and applies the new rate limit if the effective RPS has changed.
func (r *rateLimitManager) UserDataChanged() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Fetch the latest user data and update the API-configured RPS.
	r.trySetRPSFromUserDataLocked()
	r.computeAndApplyRateLimitLocked()
}

// trySetRPSFromUserDataLocked sets the apiConfigRPS from user data.
// Called exclusively in response to updates in user data.
func (r *rateLimitManager) trySetRPSFromUserDataLocked() {
	userData, _, err := r.userDataManager.GetUserData()
	if err != nil {
		return
	}
	config := userData.GetData().GetPerType()[int32(r.taskQueueType)].GetConfig()
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
		r.fairnessKeyRateLimitDefault = nil
	} else {
		// Maintain the fairnessKeyRateLimitDefault as per-partition rate.
		val := float64(fairnessKeyRateLimitDefault.GetRateLimit().GetRequestsPerSecond()) / float64(r.numReadPartitions)
		r.fairnessKeyRateLimitDefault = &val
	}
	fairnessWeightOverrides := config.GetFairnessWeightOverrides()
	r.perKeyOverrides = fairnessWeightOverrides
}

// updateRatelimitLocked checks and updates the overall queue rate limit if changed.
func (r *rateLimitManager) updateRatelimitLocked() {
	newRPS := r.effectiveRPS
	// If the effective RPS is zero, we set the burst to zero as well.
	// This prevents any initial tasks from executing immediately.
	// Allows pausing of the task queue by setting the RPS to zero.
	var burst int
	if newRPS != 0 {
		// If the effective RPS is non-zero, we can set a burst based on the effective RPS.
		burst = max(int(math.Ceil(newRPS)), r.config.MinTaskThrottlingBurstSize())
	}
	r.dynamicRateBurst.SetRPS(newRPS)
	r.dynamicRateBurst.SetBurst(burst)
	// updateRatelimitLocked is invoked whenever the effective RPS value changes.
	// At this point, the dynamicRateLimiter is always updated with the latest rate and burst values,
	// ensuring that the new rate limit takes effect immediately.
	r.dynamicRateLimiter.Refresh()
}

// UpdateSimpleRateLimit updates the overall queue rate limits for the simpleRateLimiter implementation
func (r *rateLimitManager) updateSimpleRateLimitWithBurstLocked(burstDuration time.Duration) {
	newRPS := r.effectiveRPS
	r.wholeQueueLimit = makeSimpleLimiterParams(newRPS, burstDuration)

	// Clip to handle the case where we have increased from a zero or very low limit and had
	// ready times in the far future.
	now := r.timeSource.Now().UnixNano()
	r.wholeQueueReady = r.wholeQueueReady.clip(r.wholeQueueLimit, now, maxTokens)
}

// UpdatePerKeySimpleRateLimit updates the per-key rate limit for the simpleRateLimit implementation
// UpdateTaskQueueConfig api is the single source for the per-key rate limit.
func (r *rateLimitManager) updatePerKeySimpleRateLimitWithBurstLocked(burstDuration time.Duration) {
	if r.fairnessKeyRateLimitDefault == nil {
		r.clearPerKeyRateLimitsLocked()
		return
	}
	rate := *r.fairnessKeyRateLimitDefault
	slp := makeSimpleLimiterParams(rate, burstDuration)
	if slp == r.perKeyLimit {
		// No change in per-key rate limit, no need to update.
		return
	}
	r.perKeyLimit = slp

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
	r.perKeyReady = cache.New(r.config.FairnessKeyRateLimitCacheSize(), nil)
	r.perKeyLimit = simpleLimiterParams{}
}

func (r *rateLimitManager) readyTimeForTask(task *internalTask) simpleLimiter {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// GetFairnessWeightOverrides returns the current fairness weight overrides.
func (r *rateLimitManager) GetFairnessWeightOverrides() fairnessWeightOverrides {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.perKeyOverrides
}

func (r *rateLimitManager) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cancel := range r.cancels {
		cancel()
	}
	r.cancels = nil
}
