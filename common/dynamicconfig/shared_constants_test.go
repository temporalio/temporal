package dynamicconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestConvertServerVersionCheckSchedule(t *testing.T) {
	for name, tc := range map[string]struct {
		value    any
		expected []time.Duration
	}{
		"strings":           {value: []any{"5m", "1h", "24h"}, expected: []time.Duration{5 * time.Minute, time.Hour, 24 * time.Hour}},
		"single entry":      {value: []any{"24h"}, expected: []time.Duration{24 * time.Hour}},
		"typed durations":   {value: []time.Duration{time.Minute, time.Hour}, expected: []time.Duration{time.Minute, time.Hour}},
		"numeric as second": {value: []any{60, 3600}, expected: []time.Duration{time.Minute, time.Hour}},
	} {
		t.Run(name, func(t *testing.T) {
			schedule, err := convertServerVersionCheckSchedule(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.expected, schedule)
		})
	}
	for name, value := range map[string]any{
		"empty":          []any{},
		"zero":           []any{"0s", "1h"},
		"negative":       []any{"-1m", "1h"},
		"not increasing": []any{"1h", "30m"},
		"duplicate":      []any{"1h", "1h"},
		"invalid":        []any{"soon"},
		"not a list":     "5m",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := convertServerVersionCheckSchedule(value)
			require.Error(t, err)
		})
	}
}

func TestServerVersionCheckScheduleReplacesDefault(t *testing.T) {
	client := NewMemoryClient()
	collection := NewCollection(client, log.NewNoopLogger())
	get := ServerVersionCheckSchedule.Get(collection)
	require.Equal(t, []time.Duration{5 * time.Minute, 30 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour}, get())
	client.OverrideSetting(ServerVersionCheckSchedule, []any{"24h"})
	require.Equal(t, []time.Duration{24 * time.Hour}, get())
	client.OverrideSetting(ServerVersionCheckSchedule, []any{"1h", "5m"})
	require.Equal(t, []time.Duration{5 * time.Minute, 30 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour}, get())
}

func TestConvertHistoryReplicationRateLimiterSettings(t *testing.T) {
	testCases := map[string]struct {
		value    any
		expected HistoryReplicationRateLimiterSettings
	}{
		"bool":          {value: true, expected: HistoryReplicationRateLimiterSettings{Enabled: true, Scope: HistoryReplicationRateLimiterScopeAll}},
		"quoted bool":   {value: "true", expected: HistoryReplicationRateLimiterSettings{Enabled: true, Scope: HistoryReplicationRateLimiterScopeAll}},
		"bool disabled": {value: false, expected: HistoryReplicationRateLimiterSettings{Scope: HistoryReplicationRateLimiterScopeAll}},
		"all": {
			value:    map[string]any{"Enabled": true, "Scope": "all"},
			expected: HistoryReplicationRateLimiterSettings{Enabled: true, Scope: HistoryReplicationRateLimiterScopeAll},
		},
		"open force replication": {
			value:    map[string]any{"Enabled": true, "Scope": "openForceReplication"},
			expected: HistoryReplicationRateLimiterSettings{Enabled: true, Scope: HistoryReplicationRateLimiterScopeOpenForceReplication},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			settings, err := convertHistoryReplicationRateLimiterSettings(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.expected, settings)
		})
	}
}

func TestHistoryReplicationRateLimiterFailsClosed(t *testing.T) {
	testCases := map[string]any{
		"unknown scope":        map[string]any{"Enabled": true, "Scope": "open"},
		"missing scope":        map[string]any{"Enabled": true},
		"misspelled scope key": map[string]any{"Enabled": true, "Scop": HistoryReplicationRateLimiterScopeOpenForceReplication},
		"wrong field type":     map[string]any{"Enabled": true, "Scope": 1},
		"wrong value type":     42,
		"unparsable string":    "yes",
	}
	for name, value := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := convertHistoryReplicationRateLimiterSettings(value)
			require.Error(t, err)

			collection := NewCollection(StaticClient{HistoryReplicationRateLimiter.Key(): value}, log.NewNoopLogger())
			require.False(t, HistoryReplicationRateLimiter.Get(collection)("test-namespace").Enabled)
		})
	}
}
