package queues

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

// Benchmark parameters. These are fixed so that runs of different controller designs are
// directly comparable.
const (
	benchBacklog        = 50000
	benchBudgetPerSec   = 200.0
	benchMaxDuration    = 120 * time.Second
	benchSuccessCost    = time.Millisecond
	benchSpread         = 3 * time.Second
	benchWorkerCount    = 512
	benchNamespaceID    = "bench-namespace"
	benchSampleInterval = 100 * time.Millisecond
	benchSeed           = 20260824
)

type benchConfig struct {
	name              string
	controllerEnabled bool
	rejectionCost     time.Duration
}

type benchResult struct {
	Name              string        `json:"name"`
	ControllerEnabled bool          `json:"controllerEnabled"`
	RejectionCostMs   float64       `json:"rejectionCostMs"`
	Elapsed           float64       `json:"elapsedSeconds"`
	TotalAttempts     int64         `json:"totalAttempts"`
	Completions       int64         `json:"completions"`
	WastedAttempts    int64         `json:"wastedAttempts"`
	AttemptsPerDone   float64       `json:"attemptsPerCompletion"`
	WastedPerSec      float64       `json:"wastedAttemptsPerSecond"`
	ThroughputPerSec  float64       `json:"throughputPerSecond"`
	UtilisationPct    float64       `json:"utilisationPercent"`
	Drained           bool          `json:"drained"`
	DrainSeconds      float64       `json:"drainSeconds"`
	RemainingBacklog  int64         `json:"remainingBacklog"`
	P50LatencyMs      float64       `json:"p50LatencyMs"`
	P99LatencyMs      float64       `json:"p99LatencyMs"`
	MaxInFlight       int64         `json:"maxInFlight"`
	RateMin           float64       `json:"admittedRateMin"`
	RateMax           float64       `json:"admittedRateMax"`
	RateMean          float64       `json:"admittedRateMean"`
	SteadyRateMin     float64       `json:"steadyStateRateMin"`
	SteadyRateMax     float64       `json:"steadyStateRateMax"`
	SteadyRateMean    float64       `json:"steadyStateRateMean"`
	SawtoothAmplitude float64       `json:"sawtoothAmplitudeSteadyState"`
	SawtoothPeriodSec float64       `json:"sawtoothPeriodSeconds"`
	Decreases         int64         `json:"decreaseEvents"`
	Increases         int64         `json:"increaseEvents"`
	Series            []benchBucket `json:"series"`
}

// benchBucket is one second of the run. Cumulative counters are differenced into per second
// rates when the series is written out, so a dropped sample cannot silently invent a spike.
type benchBucket struct {
	T                float64 `json:"t"`
	Attempts         int64   `json:"attemptsPerSecond"`
	Completions      int64   `json:"completionsPerSecond"`
	Rejections       int64   `json:"rejectionsPerSecond"`
	RemainingBacklog int64   `json:"remainingBacklog"`
	RateMin          float64 `json:"admittedRateMin"`
	RateMean         float64 `json:"admittedRateMean"`
	RateMax          float64 `json:"admittedRateMax"`
}

type benchHarness struct {
	cfg         benchConfig
	limiter     *quotas.RateLimiterImpl
	state       *ThrottleState
	rescheduler *reschedulerImpl
	scheduler   Scheduler
	timeSource  clock.TimeSource
	throttleKey ThrottleKey

	attempts    atomic.Int64
	rejections  atomic.Int64
	completions atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	latencyMu sync.Mutex
	latencies []time.Duration

	backlog   int64
	startTime time.Time
	doneOnce  sync.Once
	doneCh    chan struct{}
}

type benchTask struct {
	tasks.Task

	harness  *benchHarness
	enqueued time.Time

	mu                     sync.Mutex
	state                  ctasks.State
	throttleAdmitted       bool
	attempt                int
	resourceExhaustedCount int
	scheduledTime          time.Time
	throttleKey            ThrottleKey
	hasThrottleKey         bool
}

var _ Executable = (*benchTask)(nil)
var _ ThrottleKeyProvider = (*benchTask)(nil)

// spend simulates the wall clock cost of an attempt. Task attempts here are IO bound (a
// workflow lock acquisition, a mutable state load, a limiter check), so sleeping models them
// far better than spinning, which would only measure the host's core count.
func spend(d time.Duration) {
	if d <= 0 {
		return
	}
	//nolint:forbidigo // the harness is simulating service time, which is the point of it.
	time.Sleep(d)
}

func (t *benchTask) Execute() error {
	h := t.harness
	h.attempts.Add(1)

	inFlight := h.inFlight.Add(1)
	for {
		observed := h.maxInFlight.Load()
		if inFlight <= observed || h.maxInFlight.CompareAndSwap(observed, inFlight) {
			break
		}
	}
	defer h.inFlight.Add(-1)

	if h.limiter.Allow() {
		spend(benchSuccessCost)
		return nil
	}

	spend(h.cfg.rejectionCost)
	return &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "namespace APS limit reached",
	}
}

func (t *benchTask) HandleErr(err error) error {
	if err == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.attempt++

	var resourceExhausted *serviceerror.ResourceExhausted
	if errors.As(err, &resourceExhausted) {
		t.resourceExhaustedCount++
		t.harness.rejections.Add(1)
		admitted := t.throttleAdmitted
		t.throttleAdmitted = false
		if IsControllerInput(err, resourceExhausted.Cause) {
			t.throttleKey = NewThrottleKey(
				resourceExhausted.Cause,
				resourceExhausted.Scope,
				t.GetNamespaceID(),
				0,
				t.GetCategory().Name(),
			)
			t.hasThrottleKey = true
			t.harness.state.ReportThrottled(t.throttleKey, admitted)
		}
	}
	return err
}

func (t *benchTask) Nack(err error) {
	if t.State() != ctasks.TaskStatePending {
		return
	}

	submitted := false
	if t.shouldResubmitOnNack() {
		t.SetScheduledTime(t.harness.timeSource.Now())
		submitted = t.harness.scheduler.TrySubmit(t)
	}
	if !submitted {
		t.harness.rescheduler.Add(t, t.harness.timeSource.Now().Add(t.backoffDuration(err)))
	}
}

// shouldResubmitOnNack mirrors executableImpl. With the controller enabled a namespace scoped
// throttle never resubmits synchronously, because that path bypasses the controller entirely.
func (t *benchTask) shouldResubmitOnNack() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.harness.cfg.controllerEnabled {
		return false
	}
	return t.resourceExhaustedCount <= resourceExhaustedResubmitMaxAttempts
}

func (t *benchTask) backoffDuration(err error) time.Duration {
	t.mu.Lock()
	attempt := t.attempt
	resourceExhaustedCount := t.resourceExhaustedCount
	t.mu.Unlock()

	duration := reschedulePolicy.ComputeNextDelay(0, attempt, err)
	return max(duration, taskResourceExhuastedReschedulePolicy.ComputeNextDelay(0, resourceExhaustedCount, err))
}

func (t *benchTask) Ack() {
	t.mu.Lock()
	if t.state != ctasks.TaskStatePending {
		t.mu.Unlock()
		return
	}
	t.state = ctasks.TaskStateAcked
	key, known := t.throttleKey, t.hasThrottleKey
	t.mu.Unlock()

	h := t.harness
	if known {
		h.state.ReportSuccess(key)
	}

	latency := time.Since(t.enqueued)
	h.latencyMu.Lock()
	h.latencies = append(h.latencies, latency)
	h.latencyMu.Unlock()

	if h.completions.Add(1) == h.backlog {
		h.doneOnce.Do(func() { close(h.doneCh) })
	}
}

func (t *benchTask) Abort() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == ctasks.TaskStatePending {
		t.state = ctasks.TaskStateAborted
	}
}

func (t *benchTask) Cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == ctasks.TaskStatePending {
		t.state = ctasks.TaskStateCancelled
	}
}

func (t *benchTask) Reschedule() {
	t.harness.rescheduler.Add(t, t.harness.timeSource.Now().Add(t.backoffDuration(nil)))
}

func (t *benchTask) State() ctasks.State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *benchTask) IsRetryableError(error) bool { return false }
func (t *benchTask) RetryPolicy() backoff.RetryPolicy {
	return backoff.DisabledRetryPolicy
}
func (t *benchTask) Attempt() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.attempt
}
func (t *benchTask) GetTask() tasks.Task           { return t.Task }
func (t *benchTask) GetPriority() ctasks.Priority  { return ctasks.PriorityHigh }
func (t *benchTask) GetScheduledTime() time.Time   { return t.scheduledTime }
func (t *benchTask) SetScheduledTime(ts time.Time) { t.scheduledTime = ts }
func (t *benchTask) SetThrottleAdmitted(key ThrottleKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.throttleAdmitted = key != ThrottleKey{}
}

func (t *benchTask) ThrottleKey() (ThrottleKey, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.throttleKey, t.hasThrottleKey
}

func newBenchThrottleState(enabled bool, timeSource clock.TimeSource) *ThrottleState {
	return NewThrottleState(
		ThrottleStateOptions{
			Enabled:       dynamicconfig.GetBoolPropertyFn(enabled),
			Beta:          dynamicconfig.GetFloatPropertyFn(0.85),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(0.10),
			Window:        dynamicconfig.GetDurationPropertyFn(time.Second),
			MinRate:       dynamicconfig.GetFloatPropertyFn(1),
			MaxRate:       dynamicconfig.GetFloatPropertyFn(10000),
			InitialRate:   dynamicconfig.GetFloatPropertyFn(1000),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(1024),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(5 * time.Minute),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
}

// benchOverride lets a smoke run shrink the harness. Unset, the benchmark runs exactly the
// parameters the results are reported against.
func benchOverride(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func runBenchConfig(t *testing.T, cfg benchConfig) benchResult {
	t.Helper()

	backlog := benchOverride("THROTTLE_BENCH_BACKLOG", benchBacklog)
	maxDuration := time.Duration(benchOverride("THROTTLE_BENCH_SECONDS", int(benchMaxDuration/time.Second))) * time.Second

	logger := log.NewTestLogger()
	timeSource := clock.NewRealTimeSource()

	fifo := ctasks.NewFIFOScheduler[Executable](
		&ctasks.FIFOSchedulerOptions{
			QueueSize: prioritySchedulerProcessorQueueSize,
			WorkerCount: func(_ func(int)) (int, func()) {
				return benchWorkerCount, func() {}
			},
		},
		logger,
	)
	iwrr := ctasks.NewInterleavedWeightedRoundRobinScheduler(
		ctasks.InterleavedWeightedRoundRobinSchedulerOptions[Executable, TaskChannelKey]{
			TaskChannelKeyFn: func(e Executable) TaskChannelKey {
				return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
			},
			ChannelWeightFn:              func(TaskChannelKey) int { return 1 },
			InactiveChannelDeletionDelay: dynamicconfig.GetDurationPropertyFn(time.Hour),
		},
		fifo,
		logger,
	)
	scheduler := &CommonSchedulerWrapper{
		Scheduler: iwrr,
		TaskKeyFn: func(e Executable) TaskChannelKey {
			return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
		},
	}

	state := newBenchThrottleState(cfg.controllerEnabled, timeSource)
	rescheduler := NewRescheduler(
		scheduler,
		timeSource,
		logger,
		metrics.NoopMetricsHandler,
		state,
		dynamicconfig.GetIntPropertyFn(1000),
	)

	h := &benchHarness{
		cfg:         cfg,
		limiter:     quotas.NewRateLimiter(benchBudgetPerSec, int(benchBudgetPerSec)),
		state:       state,
		rescheduler: rescheduler,
		scheduler:   scheduler,
		timeSource:  timeSource,
		latencies:   make([]time.Duration, 0, backlog),
		doneCh:      make(chan struct{}),
		throttleKey: NewThrottleKey(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
			enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			benchNamespaceID,
			0,
			tasks.CategoryTransfer.Name(),
		),
	}

	scheduler.Start()
	rescheduler.Start()
	defer func() {
		rescheduler.Stop()
		scheduler.Stop()
	}()

	// Seed the backlog as parked tasks: each has already attempted once and been throttled, and
	// each is spread over one initial backoff interval rather than all becoming due at t=0.
	rng := rand.New(rand.NewSource(benchSeed)) //nolint:gosec // deterministic harness input
	h.startTime = time.Now()
	h.backlog = int64(backlog)
	for i := 0; i < backlog; i++ {
		task := &benchTask{
			Task: tasks.NewFakeTask(
				definition.NewWorkflowKey(benchNamespaceID, fmt.Sprintf("wf-%d", i), "run"),
				tasks.CategoryTransfer,
				h.startTime,
			),
			harness:                h,
			enqueued:               h.startTime,
			state:                  ctasks.TaskStatePending,
			attempt:                1,
			resourceExhaustedCount: 1,
			throttleKey:            h.throttleKey,
			hasThrottleKey:         true,
		}
		rescheduler.Add(task, h.startTime.Add(time.Duration(rng.Int63n(int64(benchSpread)))))
	}
	if cfg.controllerEnabled {
		state.ReportThrottled(h.throttleKey, false)
	}

	type sample struct {
		t    float64
		rate float64
	}
	samples := make([]sample, 0, int(maxDuration/benchSampleInterval)+1)
	buckets := make([]benchBucket, 0, int(maxDuration/time.Second)+1)

	ticker := time.NewTicker(benchSampleInterval)
	defer ticker.Stop()

	deadline := time.NewTimer(maxDuration)
	defer deadline.Stop()

	subSamplesPerBucket := int(time.Second / benchSampleInterval)
	bucketMin, bucketMax, bucketSum := math.MaxFloat64, 0.0, 0.0
	var prevAttempts, prevCompletions, prevRejections int64
	subSamples := 0

	drained := false
loop:
	for {
		select {
		case <-h.doneCh:
			drained = true
			break loop
		case <-deadline.C:
			break loop
		case <-ticker.C:
			rate := state.AdmittedRate(h.throttleKey)
			samples = append(samples, sample{t: time.Since(h.startTime).Seconds(), rate: rate})
			bucketMin = min(bucketMin, rate)
			bucketMax = max(bucketMax, rate)
			bucketSum += rate
			subSamples++
			if subSamples < subSamplesPerBucket {
				continue
			}

			attemptsNow := h.attempts.Load()
			completionsNow := h.completions.Load()
			rejectionsNow := h.rejections.Load()
			buckets = append(buckets, benchBucket{
				T:                math.Round(time.Since(h.startTime).Seconds()),
				Attempts:         attemptsNow - prevAttempts,
				Completions:      completionsNow - prevCompletions,
				Rejections:       rejectionsNow - prevRejections,
				RemainingBacklog: h.backlog - completionsNow,
				RateMin:          bucketMin,
				RateMean:         bucketSum / float64(subSamples),
				RateMax:          bucketMax,
			})
			prevAttempts, prevCompletions, prevRejections = attemptsNow, completionsNow, rejectionsNow
			bucketMin, bucketMax, bucketSum, subSamples = math.MaxFloat64, 0.0, 0.0, 0
		}
	}
	elapsed := time.Since(h.startTime)

	decreases, increases := state.Counters(h.throttleKey)

	h.latencyMu.Lock()
	latencies := append([]time.Duration(nil), h.latencies...)
	h.latencyMu.Unlock()
	slices.Sort(latencies)

	percentile := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(math.Ceil(p*float64(len(latencies)))) - 1
		idx = min(max(idx, 0), len(latencies)-1)
		return float64(latencies[idx].Microseconds()) / 1000
	}

	attempts := h.attempts.Load()
	completions := h.completions.Load()
	wasted := h.rejections.Load()

	result := benchResult{
		Name:              cfg.name,
		ControllerEnabled: cfg.controllerEnabled,
		RejectionCostMs:   float64(cfg.rejectionCost.Microseconds()) / 1000,
		Elapsed:           elapsed.Seconds(),
		TotalAttempts:     attempts,
		Completions:       completions,
		WastedAttempts:    wasted,
		WastedPerSec:      float64(wasted) / elapsed.Seconds(),
		ThroughputPerSec:  float64(completions) / elapsed.Seconds(),
		Drained:           drained,
		RemainingBacklog:  h.backlog - completions,
		P50LatencyMs:      percentile(0.5),
		P99LatencyMs:      percentile(0.99),
		MaxInFlight:       h.maxInFlight.Load(),
		Decreases:         decreases,
		Increases:         increases,
		Series:            buckets,
	}
	if completions > 0 {
		result.AttemptsPerDone = float64(attempts) / float64(completions)
	}
	result.UtilisationPct = result.ThroughputPerSec / benchBudgetPerSec * 100
	if drained {
		result.DrainSeconds = elapsed.Seconds()
	}

	if cfg.controllerEnabled && len(samples) > 0 {
		minRate, maxRate, sum := math.MaxFloat64, 0.0, 0.0
		for _, s := range samples {
			minRate = min(minRate, s.rate)
			maxRate = max(maxRate, s.rate)
			sum += s.rate
		}
		result.RateMin = minRate
		result.RateMax = maxRate
		result.RateMean = sum / float64(len(samples))

		// The sawtooth is only meaningful once the initial descent from the starting rate is
		// over, so report its shape over the second half of the run.
		steady := samples[len(samples)/2:]
		steadyMin, steadyMax, steadySum := math.MaxFloat64, 0.0, 0.0
		for _, s := range steady {
			steadyMin = min(steadyMin, s.rate)
			steadyMax = max(steadyMax, s.rate)
			steadySum += s.rate
		}
		result.SteadyRateMin = steadyMin
		result.SteadyRateMax = steadyMax
		result.SteadyRateMean = steadySum / float64(len(steady))
		result.SawtoothAmplitude = steadyMax - steadyMin
		if decreases > 0 {
			result.SawtoothPeriodSec = elapsed.Seconds() / float64(decreases)
		}
	}

	return result
}

// writeBenchCSV writes one config's per second series so the charts derived from it can be
// reproduced without rerunning the harness.
func writeBenchCSV(dir string, result benchResult) error {
	name := strings.NewReplacer("/", "_", "-", "_").Replace(result.Name)
	file, err := os.Create(filepath.Join(dir, name+".csv")) //nolint:gosec // benchmark output path is operator supplied
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"t_seconds",
		"attempts_per_second",
		"completions_per_second",
		"throttle_rejections_per_second",
		"remaining_backlog",
		"admitted_rate_min",
		"admitted_rate_mean",
		"admitted_rate_max",
	}); err != nil {
		return err
	}
	for _, bucket := range result.Series {
		if err := writer.Write([]string{
			strconv.FormatFloat(bucket.T, 'f', 0, 64),
			strconv.FormatInt(bucket.Attempts, 10),
			strconv.FormatInt(bucket.Completions, 10),
			strconv.FormatInt(bucket.Rejections, 10),
			strconv.FormatInt(bucket.RemainingBacklog, 10),
			strconv.FormatFloat(bucket.RateMin, 'f', 2, 64),
			strconv.FormatFloat(bucket.RateMean, 'f', 2, 64),
			strconv.FormatFloat(bucket.RateMax, 'f', 2, 64),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

// TestThrottleControllerBenchmark measures the throttle controller against the current
// behaviour on a single throttled namespace. It takes several minutes, so it only runs when
// THROTTLE_BENCH is set.
func TestThrottleControllerBenchmark(t *testing.T) {
	if os.Getenv("THROTTLE_BENCH") == "" {
		t.Skip("set THROTTLE_BENCH=1 to run the throttle controller benchmark")
	}

	configs := []benchConfig{
		{name: "baseline/aps-100us", controllerEnabled: false, rejectionCost: 100 * time.Microsecond},
		{name: "aimd/aps-100us", controllerEnabled: true, rejectionCost: 100 * time.Microsecond},
		{name: "baseline/persistence-2ms", controllerEnabled: false, rejectionCost: 2 * time.Millisecond},
		{name: "aimd/persistence-2ms", controllerEnabled: true, rejectionCost: 2 * time.Millisecond},
	}

	results := make([]benchResult, 0, len(configs))
	for _, cfg := range configs {
		result := runBenchConfig(t, cfg)
		results = append(results, result)
		t.Logf("%s: attempts=%d completions=%d attempts/completion=%.2f wasted/s=%.0f throughput=%.1f/s (%.1f%%) p50=%.0fms p99=%.0fms remaining=%d steadyRate[min/mean/max]=%.0f/%.0f/%.0f decreases=%d increases=%d maxInFlight=%d",
			result.Name, result.TotalAttempts, result.Completions, result.AttemptsPerDone,
			result.WastedPerSec, result.ThroughputPerSec, result.UtilisationPct,
			result.P50LatencyMs, result.P99LatencyMs, result.RemainingBacklog,
			result.SteadyRateMin, result.SteadyRateMean, result.SteadyRateMax,
			result.Decreases, result.Increases, result.MaxInFlight)
	}

	if path := os.Getenv("THROTTLE_BENCH_OUT"); path != "" {
		encoded, err := json.MarshalIndent(results, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, encoded, 0o600))
	}

	if dir := os.Getenv("THROTTLE_BENCH_CSV_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o750))
		for _, result := range results {
			require.NoError(t, writeBenchCSV(dir, result))
		}
	}

	for _, result := range results {
		require.Positive(t, result.Completions, "%s made no progress at all", result.Name)
	}

	assertControllerBeatsBaseline(t, results)
}

// assertControllerBeatsBaseline turns the simulation from a report into a regression gate. The
// bounds are deliberately loose: the simulation is concurrent and wall clock driven, so pinning
// it tightly would make it flaky. They are still tight enough to catch the regressions that
// matter, such as the controller losing its attempt reduction or costing throughput.
func assertControllerBeatsBaseline(t *testing.T, results []benchResult) {
	t.Helper()

	byName := make(map[string]benchResult, len(results))
	for _, r := range results {
		byName[r.Name] = r
	}

	for _, scenario := range []string{"aps-100us", "persistence-2ms"} {
		baseline, ok := byName["baseline/"+scenario]
		require.True(t, ok, "missing baseline arm for %s", scenario)
		controller, ok := byName["aimd/"+scenario]
		require.True(t, ok, "missing controller arm for %s", scenario)

		// The controller exists to stop tasks rediscovering the same constraint. On the cluster
		// it held near 2 attempts per completion at every scale tested.
		require.Less(t, controller.AttemptsPerDone, 3.0,
			"%s: controller attempts/completion regressed to %.2f",
			scenario, controller.AttemptsPerDone)

		require.Less(t, controller.AttemptsPerDone, baseline.AttemptsPerDone,
			"%s: controller (%.2f) must waste fewer attempts than baseline (%.2f)",
			scenario, controller.AttemptsPerDone, baseline.AttemptsPerDone)

		require.Less(t, controller.WastedPerSec, baseline.WastedPerSec,
			"%s: controller wasted %.0f attempts/s against baseline %.0f",
			scenario, controller.WastedPerSec, baseline.WastedPerSec)

		// Suppressing attempts must not cost throughput. Some slack is allowed because the
		// controller paces releases, but a real regression shows up well outside it.
		require.Greater(t, controller.ThroughputPerSec, baseline.ThroughputPerSec*0.8,
			"%s: controller throughput %.1f/s fell below 80%% of baseline %.1f/s",
			scenario, controller.ThroughputPerSec, baseline.ThroughputPerSec)

		// A controller pinned at the floor drains nothing; one pinned at the ceiling is not
		// controlling. Both show up as a steady state rate outside a broad band.
		require.Positive(t, controller.Decreases,
			"%s: controller never decreased, so it never saw back pressure", scenario)
		require.Positive(t, controller.Increases,
			"%s: controller never increased, so it is stuck at the floor", scenario)
	}
}
