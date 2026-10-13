package config

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGoMemoryLimitValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  GoMemoryLimit
		err  string
	}{
		{name: "disabled"},
		{name: "enabled", cfg: GoMemoryLimit{Ratio: 0.8, RefreshInterval: time.Minute}},
		{name: "startup only", cfg: GoMemoryLimit{Ratio: 1}},
		{name: "negative ratio", cfg: GoMemoryLimit{Ratio: -0.1}, err: "global.goMemoryLimit.ratio"},
		{name: "ratio above one", cfg: GoMemoryLimit{Ratio: 1.1}, err: "global.goMemoryLimit.ratio"},
		{name: "nan", cfg: GoMemoryLimit{Ratio: math.NaN()}, err: "global.goMemoryLimit.ratio"},
		{name: "positive infinity", cfg: GoMemoryLimit{Ratio: math.Inf(1)}, err: "global.goMemoryLimit.ratio"},
		{name: "negative infinity", cfg: GoMemoryLimit{Ratio: math.Inf(-1)}, err: "global.goMemoryLimit.ratio"},
		{name: "negative interval", cfg: GoMemoryLimit{Ratio: 0.8, RefreshInterval: -time.Second}, err: "global.goMemoryLimit.refreshInterval"},
		{name: "disabled with negative interval", cfg: GoMemoryLimit{RefreshInterval: -time.Second}, err: "global.goMemoryLimit.refreshInterval"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == "" {
				require.NoError(t, tc.cfg.Validate())
			} else {
				require.ErrorContains(t, tc.cfg.Validate(), tc.err)
			}
		})
	}
}

func TestGoMemoryLimitConfigValidation(t *testing.T) {
	t.Setenv("DB", "sqlite")
	cfg, err := Load(WithEmbedded())
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	t.Setenv("GOMEMLIMIT", "1GiB")
	cfg.Global.GoMemoryLimit.Ratio = math.NaN()
	require.ErrorContains(t, cfg.Validate(), "global.goMemoryLimit.ratio")
}

func TestGoMemoryLimitConfig(t *testing.T) {
	t.Setenv("TEMPORAL_GO_MEMORY_LIMIT_RATIO", "0.9")
	t.Setenv("TEMPORAL_GO_MEMORY_LIMIT_REFRESH_INTERVAL", "2m")
	for _, tc := range []struct {
		name     string
		content  string
		ratio    float64
		interval time.Duration
	}{
		{name: "omitted", content: "global: {}"},
		{name: "empty", content: "global: {goMemoryLimit: {}}"},
		{name: "configured", content: "global: {goMemoryLimit: {ratio: 0.8, refreshInterval: 1m}}", ratio: 0.8, interval: time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(configBase), &cfg))
			require.NoError(t, loadAndUnmarshalContent([]byte(tc.content), "config.yaml", &cfg))
			require.InDelta(t, tc.ratio, cfg.Global.GoMemoryLimit.Ratio, 1e-10)
			require.Equal(t, tc.interval, cfg.Global.GoMemoryLimit.RefreshInterval)
			var rendered Config
			require.NoError(t, yaml.Unmarshal([]byte(cfg.String()), &rendered))
			require.InDelta(t, tc.ratio, rendered.Global.GoMemoryLimit.Ratio, 1e-10)
			require.Equal(t, tc.interval, rendered.Global.GoMemoryLimit.RefreshInterval)
		})
	}

	for _, content := range []string{
		"global: {goMemoryLimit: {ratio: wrong}}",
		"global: {goMemoryLimit: {refreshInterval: wrong}}",
	} {
		var cfg Config
		require.Error(t, loadAndUnmarshalContent([]byte(content), "config.yaml", &cfg))
	}
}

func TestGoMemoryLimitEmbeddedTemplate(t *testing.T) {
	t.Setenv("DB", "sqlite")
	for _, tc := range []struct {
		name     string
		ratio    string
		interval string
		want     float64
		duration time.Duration
	}{
		{name: "defaults"},
		{name: "configured", ratio: "0.8", interval: "1m", want: 0.8, duration: time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TEMPORAL_GO_MEMORY_LIMIT_RATIO", tc.ratio)
			t.Setenv("TEMPORAL_GO_MEMORY_LIMIT_REFRESH_INTERVAL", tc.interval)
			cfg, err := Load(WithEmbedded())
			require.NoError(t, err)
			require.InDelta(t, tc.want, cfg.Global.GoMemoryLimit.Ratio, 1e-10)
			require.Equal(t, tc.duration, cfg.Global.GoMemoryLimit.RefreshInterval)
		})
	}
}
