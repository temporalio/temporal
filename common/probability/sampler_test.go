package probability

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSamplerSeededSequence(t *testing.T) {
	t.Parallel()
	probabilities := map[string]float64{"third": 0.22, "first": 0.01, "second": 0.11}
	sampler := NewSampler(probabilities, 2208, func(name string, probability float64) string {
		require.InDelta(t, probabilities[name], probability, 1e-12)
		return "value:" + name
	})
	for _, expected := range []struct {
		value string
		ok    bool
	}{{}, {value: "value:third", ok: true}, {value: "value:third", ok: true}, {}} {
		value, ok := sampler.Sample()
		require.Equal(t, expected.value, value)
		require.Equal(t, expected.ok, ok)
	}
}

func TestSamplerProbabilities(t *testing.T) {
	t.Parallel()
	probabilities := map[string]float64{"first": 0.2, "second": 0.3, "zero": 0}
	value := func(name string, _ float64) string { return name }
	sampler := NewSampler(probabilities, 123, value)
	repeated := NewSampler(probabilities, 123, value)
	counts := make(map[string]int)
	for range 10000 {
		value, ok := sampler.Sample()
		repeatedValue, repeatedOK := repeated.Sample()
		require.Equal(t, repeatedValue, value)
		require.Equal(t, repeatedOK, ok)
		counts[value]++
	}
	for value, probability := range map[string]float64{"first": 0.2, "second": 0.3, "": 0.5} {
		require.InDelta(t, probability, float64(counts[value])/10000, 0.02)
	}
	require.Zero(t, counts["zero"])
}

func TestSamplerDisabledAndCertain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		probabilities map[string]float64
		ok            bool
	}{
		{name: "empty"},
		{name: "zero probability", probabilities: map[string]float64{"value": 0}},
		{name: "certain zero value", probabilities: map[string]float64{"value": 1}, ok: true},
	} {
		for _, seed := range []int64{0, 123} {
			t.Run(tc.name+"/"+map[bool]string{true: "random seed", false: "fixed seed"}[seed == 0], func(t *testing.T) {
				t.Parallel()
				sampler := NewSampler(tc.probabilities, seed, func(string, float64) int { return 0 })
				value, ok := sampler.Sample()
				require.Zero(t, value)
				require.Equal(t, tc.ok, ok)
			})
		}
	}
}

func TestValidateProbabilities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		probabilities map[string]float64
	}{
		{name: "negative", probabilities: map[string]float64{"a": -0.1}},
		{name: "greater than one", probabilities: map[string]float64{"a": 1.1}},
		{name: "sum greater than one", probabilities: map[string]float64{"a": 0.6, "b": 0.6}},
		{name: "nan", probabilities: map[string]float64{"a": math.NaN()}},
		{name: "infinity", probabilities: map[string]float64{"a": math.Inf(1)}},
		{name: "negative infinity", probabilities: map[string]float64{"a": math.Inf(-1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, ValidateProbabilities(tc.probabilities))
		})
	}
	require.NoError(t, ValidateProbabilities(nil))
	require.NoError(t, ValidateProbabilities(map[string]float64{"a": 0.2, "b": 0.3, "c": 0.5}))
}

func TestSamplerConcurrent(t *testing.T) {
	t.Parallel()
	sampler := NewSampler(map[string]float64{"value": 0.5}, 123, func(string, float64) int { return 1 })
	counts := make([]int, 20)
	var wg sync.WaitGroup
	for i := range counts {
		wg.Go(func() {
			for range 100 {
				if _, ok := sampler.Sample(); ok {
					counts[i]++
				}
			}
		})
	}
	wg.Wait()
	total := 0
	for _, count := range counts {
		total += count
	}
	require.InDelta(t, 0.5, float64(total)/2000, 0.05)
}
