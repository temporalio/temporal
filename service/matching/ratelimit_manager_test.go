package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tqid"
)

type RateLimitManagerSuite struct {
	suite.Suite
	*require.Assertions
}

func TestRateLimitManagerSuite(t *testing.T) {
	suite.Run(t, new(RateLimitManagerSuite))
}

func (s *RateLimitManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *RateLimitManagerSuite) TestUpdatePerKeySimpleRateLimitLocked_WhenFairnessKeyRateLimitDefaultIsNil() {
	mockUserDataManager := &mockUserDataManager{}
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-namespace", "test-task-queue").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"test-namespace",
	)
	rateLimitManager := newRateLimitManager(mockUserDataManager, config, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	rateLimitManager.mu.Lock()
	// Simulate the condition where fairnessKeyRateLimitDefault is nil
	rateLimitManager.fairnessKeyRateLimitDefault = nil
	// Add per-key ready entries to verify they get cleared
	rateLimitManager.perKeyReady.Put("key1", simpleLimiter(1000))
	rateLimitManager.perKeyReady.Put("key2", simpleLimiter(2000))
	// Set per-key limit to verify it gets cleared
	rateLimitManager.perKeyLimit = simpleLimiterParams{
		interval: time.Second,
		burst:    10,
	}
	// Verify initial state
	s.Equal(2, rateLimitManager.perKeyReady.Size())
	s.True(rateLimitManager.perKeyLimit.limited())
	// Update the per-key simple rate limit with fairnessKeyRateLimitDefault as nil
	rateLimitManager.updatePerKeySimpleRateLimitWithBurstLocked(time.Second)
	// Verify that clearPerKeyRateLimitsLocked was called
	// The cache should be replaced with a new empty cache
	s.Equal(0, rateLimitManager.perKeyReady.Size(), "All per-key ready entries should be cleared")
	s.False(rateLimitManager.perKeyLimit.limited(), "Per-key limit should be cleared")
	rateLimitManager.mu.Unlock()
}

// Additions to rateLimitManager for use by other unit tests:

func (r *rateLimitManager) UpdateSimpleRateLimitWithBurstForTesting(burstDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateSimpleRateLimitWithBurstLocked(burstDuration)
}

func (r *rateLimitManager) UpdatePerKeySimpleRateLimitWithBurstForTesting(burstDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updatePerKeySimpleRateLimitWithBurstLocked(burstDuration)
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

func (r *rateLimitManager) SetFairnessKeyRateLimitDefaultForTesting(rps float64, source enumspb.RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fairnessKeyRateLimitDefault = &rps
}
