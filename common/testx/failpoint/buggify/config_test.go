package buggify_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	registerBuggifyError()

	tests := []struct {
		name    string
		config  buggify.Config
		wantErr string
	}{
		{name: "zero", config: buggify.Config{}},
		{
			name: "valid",
			config: buggify.Config{Seed: 1, Keys: []buggify.Key{
				{Match: "a:b", ActivationProbability: 0, FireProbability: 0.5},
				{Match: "a:*", ActivationProbability: 0.25, FireProbability: 1, Error: buggifyErrName},
			}},
		},
		{name: "keys without a seed", config: buggify.Config{Keys: []buggify.Key{{Match: "a"}}}},
		{name: "a seed without keys", config: buggify.Config{Seed: 1}, wantErr: "seed needs keys"},
		{name: "empty match", config: buggify.Config{Keys: []buggify.Key{{}}}, wantErr: "match must not be empty"},
		{
			name:    "duplicate match",
			config:  buggify.Config{Keys: []buggify.Key{{Match: "a*"}, {Match: "a*"}}},
			wantErr: `duplicate buggify key match "a*"`,
		},
		{
			name:    "activation out of range",
			config:  buggify.Config{Keys: []buggify.Key{{Match: "a", ActivationProbability: 1.5}}},
			wantErr: `probability 1.5 for key match "a"`,
		},
		{
			name:    "fire negative",
			config:  buggify.Config{Keys: []buggify.Key{{Match: "a", FireProbability: -0.1}}},
			wantErr: "probability -0.1",
		},
		{
			name:    "fire NaN",
			config:  buggify.Config{Keys: []buggify.Key{{Match: "a", FireProbability: math.NaN()}}},
			wantErr: "probability NaN",
		},
		{
			name:    "unknown error",
			config:  buggify.Config{Keys: []buggify.Key{{Match: "a", Error: "Oops"}}},
			wantErr: `unknown error "Oops" for buggify key match "a"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.config.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, buggify.New(failpoint.NewRegistry()).Set(tc.config), tc.wantErr)
		})
	}
}

func TestConfig_Activated(t *testing.T) {
	t.Parallel()

	names := make([]string, 2000)
	for i := range names {
		names[i] = fmt.Sprintf("key.%d", i)
	}
	activated := func(config buggify.Config) map[string]bool {
		set := map[string]bool{}
		for _, name := range names {
			if config.Activated(name) {
				set[name] = true
			}
		}
		return set
	}
	config := buggify.Config{Seed: 42, Keys: allKeys(0.25, 1, "")}

	t.Run("is a function of the seed", func(t *testing.T) {
		t.Parallel()
		set := activated(config)
		require.Equal(t, set, activated(config))
		require.InDelta(t, 0.25, float64(len(set))/float64(len(names)), 0.03)
		require.NotEqual(t, set, activated(buggify.Config{Seed: 43, Keys: allKeys(0.25, 1, "")}))
	})

	t.Run("is off without a seed", func(t *testing.T) {
		t.Parallel()
		require.Empty(t, activated(buggify.Config{Keys: allKeys(1, 1, "")}))
	})

	t.Run("applies the first matching entry and nothing else", func(t *testing.T) {
		t.Parallel()
		config := buggify.Config{
			Seed: 7,
			Keys: []buggify.Key{
				{Match: "grpc:/svc.A/Skip*", ActivationProbability: 0},
				{Match: "grpc:/svc.B/*", ActivationProbability: 0},
				{Match: "grpc:*", ActivationProbability: 1},
				{Match: "*.queue.*", ActivationProbability: 1},
				{Match: "exact", ActivationProbability: 1},
			},
		}
		for name, want := range map[string]bool{
			"grpc:/svc.A/SkipThis": false, // the narrow entry comes before the broad one
			"grpc:/svc.A/Call":     true,
			"grpc:/svc.B/Call":     false,
			"a.queue.forward":      true, // a * anywhere in the name
			"queue.forward":        false,
			"exact":                true,
			"exactly":              false, // no * means the whole name
			"store.update":         false, // matches no entry
		} {
			require.Equal(t, want, config.Activated(name), name)
		}
	})

	t.Run("an entry that comes later is shadowed", func(t *testing.T) {
		t.Parallel()
		config := buggify.Config{Seed: 7, Keys: []buggify.Key{
			{Match: "*", ActivationProbability: 0},
			{Match: "a", ActivationProbability: 1},
		}}
		require.False(t, config.Activated("a"))
	})
}

func TestConfig_Draw(t *testing.T) {
	t.Parallel()
	config := buggify.Config{Seed: 1}
	seen := map[float64]bool{}
	for n := range uint64(100) {
		d := config.Draw("key", n)
		require.GreaterOrEqual(t, d, 0.0)
		require.Less(t, d, 1.0)
		require.Equal(t, math.Float64bits(d), math.Float64bits(config.Draw("key", n)))
		seen[d] = true
	}
	require.Len(t, seen, 100)
	require.NotEqual(t, config.Draw("key", 1), config.Draw("other", 1))
	require.NotEqual(t, config.Draw("key", 1), buggify.Config{Seed: 2}.Draw("key", 1))
}
