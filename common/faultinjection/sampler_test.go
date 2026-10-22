package faultinjection

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSamplerSeededSequence(t *testing.T) {
	t.Parallel()
	probabilities := map[string]float64{"third": 0.22, "first": 0.01, "second": 0.11}
	sampler := newSampler(Config{Errors: probabilities, Seed: 2208})
	for _, expected := range []string{"", "third", "third", ""} {
		selected := sampler.sample()
		if expected == "" {
			require.Nil(t, selected)
		} else {
			require.NotNil(t, selected)
			require.Equal(t, expected, selected.name)
			require.InDelta(t, probabilities[expected], selected.rate, 1e-12)
		}
	}
}

func TestSamplerProbabilities(t *testing.T) {
	t.Parallel()
	probabilities := map[string]float64{"first": 0.2, "second": 0.3, "zero": 0}
	sampler := newSampler(Config{Errors: probabilities, Seed: 123})
	repeated := newSampler(Config{Errors: probabilities, Seed: 123})
	counts := make(map[string]int)
	for range 10000 {
		selected := sampler.sample()
		require.Equal(t, repeated.sample(), selected)
		name := ""
		if selected != nil {
			name = selected.name
			require.InDelta(t, probabilities[name], selected.rate, 1e-12)
		}
		counts[name]++
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
		{name: "certain fault", probabilities: map[string]float64{"value": 1}, ok: true},
	} {
		for _, seed := range []int64{0, 123} {
			t.Run(tc.name+"/"+map[bool]string{true: "random seed", false: "fixed seed"}[seed == 0], func(t *testing.T) {
				t.Parallel()
				sampler := newSampler(Config{Errors: tc.probabilities, Seed: seed})
				selected := sampler.sample()
				require.Equal(t, tc.ok, selected != nil)
				if selected != nil {
					require.Equal(t, "value", selected.name)
					require.InDelta(t, 1, selected.rate, 1e-12)
				}
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
			require.Error(t, Config{Errors: tc.probabilities}.Validate())
		})
	}
	require.NoError(t, Config{}.Validate())
	require.NoError(t, Config{Errors: map[string]float64{"a": 0.2, "b": 0.3, "c": 0.5}}.Validate())
}

func TestSamplerConcurrent(t *testing.T) {
	t.Parallel()
	sampler := newSampler(Config{Errors: map[string]float64{"value": 0.5}, Seed: 123})
	counts := make([]int, 20)
	var wg sync.WaitGroup
	for i := range counts {
		wg.Go(func() {
			for range 100 {
				if sampler.sample() != nil {
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
