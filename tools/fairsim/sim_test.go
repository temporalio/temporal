package fairsim

import (
	"io"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/service/matching/counter"
)

func newTestSimulator() *simulator {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	rnd := rand.New(src)

	counterFactory := func() counter.Counter {
		return counter.NewHybridCounter(counter.DefaultCounterParams, src)
	}

	st := newState(rnd, counterFactory, 1, defaultStrideFactor)
	stats := newLatencyStats()
	return newSimulator(st, stats, 3, io.Discard, false)
}

// pOfP returns the crossP-th percentile of per-key keyP-th percentiles.
func pOfP[T int64 | float64](data map[string][]T, keyP, crossP float64) float64 {
	result := percentileOfPercentiles(data, keyP, []float64{crossP})
	if result == nil {
		return 0
	}
	return result[0]
}

func addZipfTasks(sim *simulator, numTasks, numKeys int, zipfS, zipfV float64) {
	zipf := rand.NewZipf(sim.state.rnd, zipfS, zipfV, uint64(numKeys-1))
	for range numTasks {
		sim.addTask(&task{
			fkey: makeKey(int(zipf.Uint64())),
		})
	}
}

func makeKey(i int) string {
	return "key" + strconv.Itoa(i)
}

func drainAndCalc(sim *simulator) {
	sim.drainTasks()
	sim.stats.calculateNormalized()
}

// TestFairness_MapCounter tests fairness with 90 keys (below MapLimit=100),
// ensuring the map-based counter provides fair scheduling under Zipf skew.
func TestFairness_MapCounter(t *testing.T) {
	t.Parallel()

	sim := newTestSimulator()
	addZipfTasks(sim, 100_000, 90, 2.0, 2.0)
	drainAndCalc(sim)

	// Light keys (p90) should be expedited (negative median displacement).
	// percentileOfPercentiles sorts by value, so p90 = 90th percentile of per-key
	// medians sorted ascending. Since most keys are light under Zipf, even the p90
	// should still be a light key with negative displacement.
	p90median := pOfP(sim.stats.byKey, 50, 90)
	assert.Less(t, p90median, 0.0,
		"p90 key median displacement should be negative (light keys expedited)")

	// Heaviest key is at p100 (max median, most delayed by fairness)
	p100median := pOfP(sim.stats.byKey, 50, 100)
	assert.Greater(t, p100median, 0.0,
		"heaviest key median displacement should be positive (delayed)")
}

// TestFairness_CMSCounter tests fairness with 500 keys (above MapLimit=100),
// ensuring the count-min sketch still provides fair scheduling.
func TestFairness_CMSCounter(t *testing.T) {
	t.Parallel()

	sim := newTestSimulator()
	addZipfTasks(sim, 100_000, 500, 2.0, 2.0)
	drainAndCalc(sim)

	p90median := pOfP(sim.stats.byKey, 50, 90)
	assert.Less(t, p90median, 0.0,
		"p90 key median displacement should be negative (light keys expedited)")

	p100median := pOfP(sim.stats.byKey, 50, 100)
	assert.Greater(t, p100median, 0.0,
		"heaviest key median displacement should be positive (delayed)")
}

// TestFairness_ExtremeSkew tests with 2 keys where one dominates (95% of tasks).
func TestFairness_ExtremeSkew(t *testing.T) {
	t.Parallel()

	sim := newTestSimulator()

	// 9500 tasks for the heavy key, 500 for the light key
	for range 9500 {
		sim.addTask(&task{fkey: "heavy"})
	}
	for range 500 {
		sim.addTask(&task{fkey: "light"})
	}
	drainAndCalc(sim)

	heavyMedian := sim.stats.percentile("heavy", 50)
	lightMedian := sim.stats.percentile("light", 50)

	assert.Greater(t, heavyMedian, 0.0,
		"heavy key should have positive median displacement (delayed)")
	assert.Less(t, lightMedian, 0.0,
		"light key should have negative median displacement (expedited)")
}

// TestFairness_Weights tests that higher-weight keys get better (lower) displacement.
func TestFairness_Weights(t *testing.T) {
	t.Parallel()

	sim := newTestSimulator()

	// Round-robin 3 keys with equal task counts but different weights.
	// Higher weight = lower pass increment = dispatched sooner.
	for range 1000 {
		sim.addTask(&task{fkey: "normal", fweight: 1.0})
		sim.addTask(&task{fkey: "high", fweight: 2.0})
		sim.addTask(&task{fkey: "low", fweight: 0.5})
	}
	drainAndCalc(sim)

	highMean := sim.stats.percentile("high", 50)
	normalMean := sim.stats.percentile("normal", 50)
	lowMean := sim.stats.percentile("low", 50)

	assert.Less(t, highMean, normalMean,
		"high-weight key should have lower displacement than normal-weight")
	assert.Greater(t, lowMean, normalMean,
		"low-weight key should have higher displacement than normal-weight")
}

// TestFairness_UniformDistribution tests that with equal traffic per key,
// fairness doesn't create significant disparity.
func TestFairness_UniformDistribution_50(t *testing.T) {
	sim := newTestSimulator()
	const numTasks = 100_000
	for i := range numTasks {
		sim.addTask(&task{fkey: makeKey(i % 50)})
	}
	drainAndCalc(sim)

	p90 := pOfP(sim.stats.byKey, 50, 90)
	p10 := pOfP(sim.stats.byKey, 50, 10)
	spread := p90 - p10
	// Map counter tracks exact counts, so uniform distribution should yield
	// very tight spread.
	assert.Less(t, spread, 1000.0,
		"uniform distribution with map counter should have tight spread")
}

func TestFairness_UniformDistribution_500(t *testing.T) {
	sim := newTestSimulator()
	const numTasks = 100_000
	for i := range numTasks {
		sim.addTask(&task{fkey: makeKey(i % 500)})
	}
	drainAndCalc(sim)

	p90 := pOfP(sim.stats.byKey, 50, 90)
	p10 := pOfP(sim.stats.byKey, 50, 10)
	spread := p90 - p10
	// CMS with 500 keys has hash collisions (initial width=100), so spread
	// is wider than the map counter. Assert it stays within half the total
	// task count — a basic sanity bound.
	assert.Less(t, spread, float64(numTasks)/2,
		"uniform distribution with CMS should have bounded spread")
}
