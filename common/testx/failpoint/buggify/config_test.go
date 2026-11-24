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
			config: buggify.Config{
				Seed: 1, Activation: 0.25, Fire: 1, Error: buggifyErrName,
				Prefixes: []buggify.Prefix{{Prefix: "a:", Activation: 0, Fire: 0.5}},
				Include:  []string{"a:*"},
				Exclude:  []string{"a:b"},
			},
		},
		{name: "activation out of range", config: buggify.Config{Activation: 1.5}, wantErr: "probability 1.5"},
		{name: "fire negative", config: buggify.Config{Fire: -0.1}, wantErr: "probability -0.1"},
		{name: "unknown error", config: buggify.Config{Error: "Oops"}, wantErr: `unknown error "Oops"`},
		{name: "empty prefix", config: buggify.Config{Prefixes: []buggify.Prefix{{}}}, wantErr: "prefix must not be empty"},
		{
			name:    "duplicate prefix",
			config:  buggify.Config{Prefixes: []buggify.Prefix{{Prefix: "a"}, {Prefix: "a"}}},
			wantErr: `duplicate buggify prefix "a"`,
		},
		{
			name:    "prefix probability",
			config:  buggify.Config{Prefixes: []buggify.Prefix{{Prefix: "a", Fire: 2}}},
			wantErr: `probability 2 for prefix "a"`,
		},
		{name: "empty pattern", config: buggify.Config{Exclude: []string{""}}, wantErr: "must not be empty"},
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
			require.ErrorContains(t, buggify.For(failpoint.NewRegistry()).Set(tc.config), tc.wantErr)
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
	config := buggify.Config{Seed: 42, Activation: 0.25}

	t.Run("is a function of the seed", func(t *testing.T) {
		t.Parallel()
		set := activated(config)
		require.Equal(t, set, activated(config))
		require.InDelta(t, 0.25, float64(len(set))/float64(len(names)), 0.03)
		require.NotEqual(t, set, activated(buggify.Config{Seed: 43, Activation: 0.25}))
	})

	t.Run("is off without a seed", func(t *testing.T) {
		t.Parallel()
		require.Empty(t, activated(buggify.Config{Activation: 1}))
	})

	t.Run("applies the longest prefix and the patterns", func(t *testing.T) {
		t.Parallel()
		config := buggify.Config{
			Seed:       7,
			Activation: 1,
			Prefixes: []buggify.Prefix{
				{Prefix: "grpc:", Activation: 0},
				{Prefix: "grpc:/svc.A/", Activation: 1},
			},
			Include: []string{"grpc:*", "queue.*"},
			Exclude: []string{"grpc:/svc.A/Skip*", "*.internal"},
		}
		for name, want := range map[string]bool{
			"grpc:/svc.B/Call":     false, // grpc: prefix
			"grpc:/svc.A/Call":     true,  // longer prefix
			"grpc:/svc.A/SkipThis": false, // excluded
			"queue.forward":        true,  // base probability
			"queue.internal":       false, // excluded by a pattern with a leading *
			"store.update":         false, // not included
		} {
			require.Equal(t, want, config.Activated(name), name)
		}
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
