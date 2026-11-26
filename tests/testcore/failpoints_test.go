package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/testx/failpoint/buggify"
)

func TestWithBuggify(t *testing.T) {
	t.Parallel()
	one := 1.0
	keys := []serverbuggify.Key{{Match: "matching.skipSyncMatch", FireProbability: &one}}
	// settingsOf returns the overrides of a test option, by key.
	settingsOf := func(opts ...TestOption) (testOptions, map[dynamicconfig.Key]any) {
		var o testOptions
		for _, opt := range opts {
			opt(&o)
		}
		settings := map[dynamicconfig.Key]any{}
		for _, c := range o.dynamicConfigSettings {
			settings[c.setting.Key()] = c.value
		}
		return o, settings
	}
	knob := dynamicconfig.TransferTaskBatchSize

	t.Run("sets the setting on a dedicated cluster", func(t *testing.T) {
		t.Parallel()
		o, settings := settingsOf(WithBuggify(42, keys...))
		require.True(t, o.dedicatedCluster)
		require.Equal(t, serverbuggify.Config{Seed: 42, Keys: keys}, settings[serverbuggify.Setting.Key()])
	})

	t.Run("randomizes the buggifiable settings from the seed", func(t *testing.T) {
		t.Parallel()
		randomized := map[dynamicconfig.Key]map[any]bool{}
		for seed := uint64(1); seed <= 200; seed++ {
			_, settings := settingsOf(WithBuggify(seed))
			for _, b := range buggifiable {
				want, ok := buggify.Pick(seed, b.setting.Key().String(), 0.25, b.values...)
				got, has := settings[b.setting.Key()]
				require.Equal(t, ok, has)
				if ok {
					require.Equal(t, want, got)
					if randomized[b.setting.Key()] == nil {
						randomized[b.setting.Key()] = map[any]bool{}
					}
					randomized[b.setting.Key()][got] = true
				}
			}
		}
		for _, b := range buggifiable {
			require.Len(t, randomized[b.setting.Key()], len(b.values), b.setting.Key())
		}
	})

	t.Run("a seed of 0 randomizes nothing", func(t *testing.T) {
		t.Parallel()
		_, settings := settingsOf(WithBuggify(0))
		require.Len(t, settings, 1)
	})

	t.Run("a setting of the test wins, in any order", func(t *testing.T) {
		t.Parallel()
		seed := uint64(1)
		for ; ; seed++ {
			if _, ok := buggify.Pick(seed, knob.Key().String(), 0.25, 1, 2, 10); ok {
				break
			}
		}
		_, before := settingsOf(WithDynamicConfig(knob, 5), WithBuggify(seed))
		require.Equal(t, 5, before[knob.Key()])
		_, after := settingsOf(WithBuggify(seed), WithDynamicConfig(knob, 5))
		require.Equal(t, 5, after[knob.Key()])
	})
}
