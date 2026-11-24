package failpoint_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

func lookupIn(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestBuggifyConfigFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		want    failpoint.BuggifyConfig
		wantErr string
	}{
		{name: "unset", env: nil, want: failpoint.BuggifyConfig{}},
		{name: "empty seed", env: map[string]string{failpoint.BuggifySeedEnv: ""}, want: failpoint.BuggifyConfig{}},
		{name: "gomad seed alone", env: map[string]string{failpoint.GomadSeedEnv: "7"}, want: failpoint.BuggifyConfig{}},
		{
			name: "seed",
			env:  map[string]string{failpoint.BuggifySeedEnv: "42"},
			want: failpoint.BuggifyConfig{Seed: 42, Activation: 0.25, Fire: 0.25},
		},
		{
			name: "probabilities",
			env:  map[string]string{failpoint.BuggifySeedEnv: "42", failpoint.BuggifyActivationEnv: "0.5", failpoint.BuggifyFireEnv: "0"},
			want: failpoint.BuggifyConfig{Seed: 42, Activation: 0.5, Fire: 0},
		},
		{name: "zero seed", env: map[string]string{failpoint.BuggifySeedEnv: "0"}, wantErr: "want a positive integer"},
		{name: "malformed seed", env: map[string]string{failpoint.BuggifySeedEnv: "x"}, wantErr: `invalid FAILPOINT_BUGGIFY_SEED "x"`},
		{name: "gomad without its seed", env: map[string]string{failpoint.BuggifySeedEnv: "gomad"}, wantErr: "FAILPOINT_BUGGIFY_SEED=gomad requires GOMADSEED"},
		{
			name:    "probability out of range",
			env:     map[string]string{failpoint.BuggifySeedEnv: "1", failpoint.BuggifyFireEnv: "1.5"},
			wantErr: `invalid FAILPOINT_BUGGIFY_FIRE "1.5"`,
		},
		{
			name:    "malformed probability",
			env:     map[string]string{failpoint.BuggifySeedEnv: "1", failpoint.BuggifyActivationEnv: "NaN"},
			wantErr: `invalid FAILPOINT_BUGGIFY_ACTIVATION "NaN"`,
		},
		{
			name:    "probability without a seed",
			env:     map[string]string{failpoint.BuggifyActivationEnv: "0.5"},
			wantErr: "FAILPOINT_BUGGIFY_ACTIVATION requires FAILPOINT_BUGGIFY_SEED",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config, err := failpoint.BuggifyConfigFromEnv(lookupIn(tc.env))
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
		env := map[string]string{failpoint.BuggifySeedEnv: "gomad", failpoint.GomadSeedEnv: "0"}
		config, err := failpoint.BuggifyConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotZero(t, config.Seed)
		again, err := failpoint.BuggifyConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.Equal(t, config, again)
		env[failpoint.GomadSeedEnv] = "1"
		other, err := failpoint.BuggifyConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotEqual(t, config.Seed, other.Seed)
	})

	t.Run("random", func(t *testing.T) {
		t.Parallel()
		env := map[string]string{failpoint.BuggifySeedEnv: "random"}
		config, err := failpoint.BuggifyConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotZero(t, config.Seed)
		other, err := failpoint.BuggifyConfigFromEnv(lookupIn(env))
		require.NoError(t, err)
		require.NotEqual(t, config.Seed, other.Seed)
	})
}
