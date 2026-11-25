package buggify_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint/buggify"
)

func lookupIn(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		want    buggify.Config
		wantErr string
	}{
		{name: "unset", env: nil, want: buggify.Config{}},
		{name: "empty seed", env: map[string]string{buggify.SeedEnv: ""}, want: buggify.Config{}},
		{name: "gomad seed alone", env: map[string]string{buggify.GomadSeedEnv: "7"}, want: buggify.Config{}},
		{
			name: "seed",
			env:  map[string]string{buggify.SeedEnv: "42"},
			want: buggify.Config{Seed: 42, Activation: 0.25, Fire: 0.25},
		},
		{
			name: "probabilities",
			env:  map[string]string{buggify.SeedEnv: "42", buggify.ActivationEnv: "0.5", buggify.FireEnv: "0"},
			want: buggify.Config{Seed: 42, Activation: 0.5, Fire: 0},
		},
		{name: "zero seed", env: map[string]string{buggify.SeedEnv: "0"}, wantErr: "want a positive integer"},
		{name: "malformed seed", env: map[string]string{buggify.SeedEnv: "x"}, wantErr: `invalid FAILPOINT_BUGGIFY_SEED "x"`},
		{name: "gomad without its seed", env: map[string]string{buggify.SeedEnv: "gomad"}, wantErr: "FAILPOINT_BUGGIFY_SEED=gomad requires GOMADSEED"},
		{
			name:    "probability out of range",
			env:     map[string]string{buggify.SeedEnv: "1", buggify.FireEnv: "1.5"},
			wantErr: `invalid FAILPOINT_BUGGIFY_FIRE "1.5"`,
		},
		{
			name:    "malformed probability",
			env:     map[string]string{buggify.SeedEnv: "1", buggify.ActivationEnv: "NaN"},
			wantErr: `invalid FAILPOINT_BUGGIFY_ACTIVATION "NaN"`,
		},
		{
			name:    "probability without a seed",
			env:     map[string]string{buggify.ActivationEnv: "0.5"},
			wantErr: "FAILPOINT_BUGGIFY_ACTIVATION requires FAILPOINT_BUGGIFY_SEED",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config, err := buggify.ConfigFromEnv(lookupIn(tc.env))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, config)
		})
	}

	t.Run("gomad", func(t *testing.T) {
		t.Parallel()
		env := map[string]string{buggify.SeedEnv: "gomad", buggify.GomadSeedEnv: "0"}
		config, err := buggify.ConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotZero(t, config.Seed)
		again, err := buggify.ConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.Equal(t, config, again)
		env[buggify.GomadSeedEnv] = "1"
		other, err := buggify.ConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotEqual(t, config.Seed, other.Seed)
	})

	t.Run("random", func(t *testing.T) {
		t.Parallel()
		env := map[string]string{buggify.SeedEnv: "random"}
		config, err := buggify.ConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotZero(t, config.Seed)
		other, err := buggify.ConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotEqual(t, config.Seed, other.Seed)
	})
}
