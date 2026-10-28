package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

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
