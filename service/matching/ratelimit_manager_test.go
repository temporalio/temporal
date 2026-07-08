package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
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

func (r *rateLimitManager) SetAPIConfigRPSForTesting(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apiConfigRPS = &rps
	r.computeEffectiveRPSAndSourceLocked()
}

func (r *rateLimitManager) SetWorkerRPSForTesting(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workerRPS = &rps
	r.computeEffectiveRPSAndSourceLocked()
}

// GetFairnessKeyRateLimitDefaultForTesting returns the stored per-partition fairness-key rate and
// whether it is set. It is the value stored after fraction and partition scaling.
func (r *rateLimitManager) GetFairnessKeyRateLimitDefaultForTesting() (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fairnessKeyRateLimitDefault == nil {
		return 0, false
	}
	return *r.fairnessKeyRateLimitDefault, true
}

func (r *rateLimitManager) GetNumReadPartitionsForTesting() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.numReadPartitions
}

// newConfigWithFraction returns a Config whose RateLimitFractionProvider always returns the given fraction.
func newConfigWithFraction(fraction float64) *Config {
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	cfg.RateLimitFractionProvider = TaskQueueRateLimitFractionProviderFunc(
		func(_ namespace.Name, _ string, _ enumspb.TaskQueueType) float64 {
			return fraction
		},
	)
	return cfg
}

// TestFractionScaling_ApiConfigRPS verifies that apiConfigRPS is scaled by the fraction
// before being divided by the number of read partitions.
func (s *RateLimitManagerSuite) TestFractionScaling_ApiConfigRPS() {
	cfg := newConfigWithFraction(0.5)
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-ns", "test-tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		cfg, "test-ns",
	)
	rlm := newRateLimitManager(&mockUserDataManager{}, config, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	defer rlm.Stop()

	rlm.SetAPIConfigRPSForTesting(100.0)

	rps, source := rlm.GetEffectiveRPSAndSource()
	// effectiveRPS = apiRPS * fraction / numPartitions; GetEffectiveRPSAndSource multiplies back by numPartitions → apiRPS * fraction
	s.InEpsilon(50.0, rps, 1e-9)
	s.Equal(enumspb.RATE_LIMIT_SOURCE_API, source)
}

// TestFractionScaling_WorkerRPS verifies that workerRPS is scaled by the fraction.
func (s *RateLimitManagerSuite) TestFractionScaling_WorkerRPS() {
	cfg := newConfigWithFraction(0.5)
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-ns", "test-tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		cfg, "test-ns",
	)
	rlm := newRateLimitManager(&mockUserDataManager{}, config, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	defer rlm.Stop()

	rlm.SetWorkerRPSForTesting(100.0)

	rps, source := rlm.GetEffectiveRPSAndSource()
	s.InEpsilon(50.0, rps, 1e-9)
	s.Equal(enumspb.RATE_LIMIT_SOURCE_WORKER, source)
}

// TestFractionScaling_FairnessKeyRateLimitDefault verifies that the per-key fairness rate default
// is also scaled by the fraction (Fix 3 coverage).
func (s *RateLimitManagerSuite) TestFractionScaling_FairnessKeyRateLimitDefault() {
	const fraction = 0.5
	const fairnessKeyRPS = float32(100)
	cfg := newConfigWithFraction(fraction)
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-ns", "test-tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		cfg, "test-ns",
	)
	udm := &mockUserDataManager{
		data: &persistencespb.VersionedTaskQueueUserData{
			Data: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
						Config: &taskqueuepb.TaskQueueConfig{
							FairnessKeysRateLimitDefault: &taskqueuepb.RateLimitConfig{
								RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: fairnessKeyRPS},
							},
						},
					},
				},
			},
		},
	}
	rlm := newRateLimitManager(udm, config, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	defer rlm.Stop()

	rlm.UserDataChanged()

	got, ok := rlm.GetFairnessKeyRateLimitDefaultForTesting()
	s.True(ok)
	// val = fairnessKeyRPS * fraction / numPartitions
	numPartitions := rlm.GetNumReadPartitionsForTesting()
	want := float64(fairnessKeyRPS) * fraction / float64(numPartitions)
	s.InEpsilon(want, got, 1e-9)
}

// TestFractionScaling_ZeroFraction verifies that a zero fraction (fully drained) reduces
// the effective RPS to zero even when an API rate limit is set.
func (s *RateLimitManagerSuite) TestFractionScaling_ZeroFraction() {
	cfg := newConfigWithFraction(0.0)
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-ns", "test-tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		cfg, "test-ns",
	)
	rlm := newRateLimitManager(&mockUserDataManager{}, config, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	defer rlm.Stop()

	rlm.SetAPIConfigRPSForTesting(100.0)

	rps, source := rlm.GetEffectiveRPSAndSource()
	s.InDelta(0.0, rps, 0.0)
	s.Equal(enumspb.RATE_LIMIT_SOURCE_API, source)
}
