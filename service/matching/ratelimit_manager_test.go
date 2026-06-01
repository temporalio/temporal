package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
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
	rateLimitManager := newRateLimitManager(mockUserDataManager, config, enumspb.TASK_QUEUE_TYPE_ACTIVITY, metrics.NoopMetricsHandler)
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

func (s *RateLimitManagerSuite) TestEffectiveRateLimitGauge() {
	mockUserDataManager := &mockUserDataManager{}
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-namespace", "test-task-queue").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"test-namespace",
	)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	interval := config.EffectiveRateLimitMetricsEmitInterval()

	// Construction emits the initial gauge sample: UNSPECIFIED->SYSTEM counts as a source change,
	// so the throttle is bypassed and the first sample fires.
	r := newRateLimitManager(mockUserDataManager, config, enumspb.TASK_QUEUE_TYPE_ACTIVITY, handler)

	recordings := capture.Snapshot()[metrics.TaskQueueEffectiveRateLimitGauge.Name()]
	s.Len(recordings, 1, "construction should emit one gauge sample")
	s.Equal(enumspb.RATE_LIMIT_SOURCE_SYSTEM.String(), recordings[0].Tags["rate_limit_source"])

	// Re-baseline throttle state onto a controlled clock so the rest of the test is deterministic.
	// Pretend construction's emit happened at ts.Now(); subsequent emits compare against this.
	ts := clock.NewEventTimeSource().Update(time.Unix(1_000_000, 0))
	r.mu.Lock()
	r.timeSource = ts
	r.lastMetricEmittedAt = ts.Now()
	r.lastMetricEmittedSource = r.rateLimitSource
	r.mu.Unlock()

	// (1) Same source, within the throttle interval -> no new emit.
	r.SetAdminRateForTesting(123.0)
	r.mu.Lock()
	r.computeAndApplyRateLimitLocked()
	r.mu.Unlock()
	recordings = capture.Snapshot()[metrics.TaskQueueEffectiveRateLimitGauge.Name()]
	s.Len(recordings, 1, "same-source recompute within interval must be throttled")

	// (2) Advance past the throttle interval -> next same-source recompute emits.
	ts.Advance(interval + time.Millisecond)
	r.mu.Lock()
	r.computeAndApplyRateLimitLocked()
	r.mu.Unlock()
	recordings = capture.Snapshot()[metrics.TaskQueueEffectiveRateLimitGauge.Name()]
	s.Len(recordings, 2, "advance past interval should release a throttled sample")
	s.Equal(enumspb.RATE_LIMIT_SOURCE_SYSTEM.String(), recordings[1].Tags["rate_limit_source"])
	s.InEpsilon(123.0, recordings[1].Value.(float64), 1e-9)

	// (3) Source flip WITHIN the throttle interval -> bypasses throttle and emits immediately.
	apiRPS := 50.0
	r.mu.Lock()
	r.apiConfigRPS = &apiRPS
	r.computeAndApplyRateLimitLocked()
	wantPerPartitionRPS := apiRPS / float64(r.numReadPartitions)
	r.mu.Unlock()
	recordings = capture.Snapshot()[metrics.TaskQueueEffectiveRateLimitGauge.Name()]
	s.Len(recordings, 3, "source change must bypass throttle even within the interval window")
	s.Equal(enumspb.RATE_LIMIT_SOURCE_API.String(), recordings[2].Tags["rate_limit_source"])
	s.InEpsilon(wantPerPartitionRPS, recordings[2].Value.(float64), 1e-9)
}

func (s *RateLimitManagerSuite) TestEffectiveRateLimitGauge_NilHandlerSafe() {
	mockUserDataManager := &mockUserDataManager{}
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-namespace", "test-task-queue").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"test-namespace",
	)
	// Nil handler must not panic; the gauge emission is silently skipped.
	r := newRateLimitManager(mockUserDataManager, config, enumspb.TASK_QUEUE_TYPE_ACTIVITY, nil)
	r.mu.Lock()
	r.computeAndApplyRateLimitLocked()
	r.mu.Unlock()
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
