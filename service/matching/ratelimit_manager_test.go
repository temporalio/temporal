package matching

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

func (r *rateLimitManager) UpdateSimpleRateLimit(burstDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateSimpleRateLimitLocked(burstDuration)
}

func (r *rateLimitManager) UpdatePerKeySimpleRateLimit(burstDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updatePerKeySimpleRateLimitLocked(burstDuration)
}

func (r *rateLimitManager) SetAdminRateForTesting(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adminNsRate = rps
	r.adminTqRate = rps
}

func (r *rateLimitManager) SetEffectiveRPSAndSourceForTesting(rps float64, source enumspb.RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.effectiveRPS = rps
	r.fairnessKeyRateLimitDefault = &rps
	r.rateLimitSource = source
}
