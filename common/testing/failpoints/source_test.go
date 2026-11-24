package failpoints_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/failpoints"
)

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
}

func TestDecide(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		configEnabled bool
		env           map[string]string
		want          bool
	}{
		{name: "neither", want: false},
		{name: "only config", configEnabled: true, want: false},
		{name: "only env", env: map[string]string{failpoints.EnvVar: "1"}, want: false},
		{name: "config and env", configEnabled: true, env: map[string]string{failpoints.EnvVar: "1"}, want: true},
		{name: "config and env set to something else", configEnabled: true, env: map[string]string{failpoints.EnvVar: "true"}, want: false},
		{name: "config and env set to zero", configEnabled: true, env: map[string]string{failpoints.EnvVar: "0"}, want: false},
		{name: "config and empty env", configEnabled: true, env: map[string]string{failpoints.EnvVar: ""}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			decision := failpoints.Decide(tc.configEnabled, lookupFrom(tc.env))
			require.Equal(t, tc.want, decision.Enabled)
			require.NotEmpty(t, decision.Reason)
			source := failpoints.NewSource(decision)
			require.Equal(t, tc.want, source.Enabled())
			require.Equal(t, tc.want, source.Failpoints() != nil)
		})
	}
}

func TestSource_Nil(t *testing.T) {
	t.Parallel()

	var source *failpoints.Source
	require.Nil(t, source.Failpoints())
	require.False(t, source.Enabled())
}

func TestNewSource_FreshRegistries(t *testing.T) {
	t.Parallel()

	enabled := failpoints.Decision{Enabled: true}
	first := failpoints.NewSource(enabled).Failpoints()
	second := failpoints.NewSource(enabled).Failpoints()
	require.NotSame(t, first, second)
}
