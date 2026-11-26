package dynamicconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestConvertReplicationStreamImplementationSettings(t *testing.T) {
	t.Parallel()
	good := map[string]any{"RemoteCluster": "good", "Implementation": "lane", "ShardPercent": 100}
	goodRule := ReplicationStreamImplementationRule{RemoteCluster: "good", Implementation: "lane", ShardPercent: 100}
	bad := func(kv ...any) map[string]any {
		rule := map[string]any{"RemoteCluster": "bad", "Implementation": "lane"}
		for i := 0; i < len(kv); i += 2 {
			rule[kv[i].(string)] = kv[i+1]
		}
		return rule
	}
	for _, tc := range []struct {
		name          string
		bad           any
		remoteCluster string
	}{
		{"QuotedShardPercent", bad("ShardPercent", "10"), "bad"},
		{"ScalarShardIDs", bad("ShardIDs", 7), "bad"},
		{"StringShardID", bad("ShardIDs", []any{7, "8"}), "bad"},
		{"IntImplementation", bad("Implementation", 1, "ShardPercent", 10), "bad"},
		{"IntRemoteCluster", map[string]any{"RemoteCluster": 5, "Implementation": "lane", "ShardPercent": 10}, ""},
		{"NotAMap", "lane", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value := map[string]any{"Sender": []any{good, tc.bad}, "Receiver": []any{good}}
			settings, err := convertReplicationStreamImplementationSettings(value)
			require.NoError(t, err)
			require.Len(t, settings.Sender, 2)
			require.Equal(t, goodRule, settings.Sender[0])
			require.Equal(t, tc.remoteCluster, settings.Sender[1].RemoteCluster)
			require.Error(t, settings.Sender[1].DecodeError)
			require.Equal(t, ReplicationStreamImplementationRule{RemoteCluster: tc.remoteCluster, DecodeError: settings.Sender[1].DecodeError}, settings.Sender[1])
			require.Equal(t, []ReplicationStreamImplementationRule{goodRule}, settings.Receiver)

			// The collection keeps the rules that decode instead of falling back to the default.
			collection := NewCollection(StaticClient{ReplicationStreamImplementation.Key(): value}, log.NewNoopLogger())
			require.Equal(t, goodRule, ReplicationStreamImplementation.Get(collection)().Sender[0])
		})
	}

	t.Run("DirectionNotAList", func(t *testing.T) {
		t.Parallel()
		settings, err := convertReplicationStreamImplementationSettings(map[string]any{"Sender": bad("ShardPercent", 10), "Receiver": []any{good}})
		require.NoError(t, err)
		require.Len(t, settings.Sender, 1)
		require.Equal(t, "bad", settings.Sender[0].RemoteCluster)
		require.ErrorContains(t, settings.Sender[0].DecodeError, "not a list")
		require.Equal(t, []ReplicationStreamImplementationRule{goodRule}, settings.Receiver)
	})
	t.Run("FieldNamesIgnoreCase", func(t *testing.T) {
		t.Parallel()
		settings, err := convertReplicationStreamImplementationSettings(map[string]any{
			"sender": []any{map[string]any{"remotecluster": "good", "implementation": "lane", "shardpercent": 100}},
		})
		require.NoError(t, err)
		require.Equal(t, []ReplicationStreamImplementationRule{goodRule}, settings.Sender)
	})
	t.Run("DecodeErrorIsNotConfigurable", func(t *testing.T) {
		t.Parallel()
		settings, err := convertReplicationStreamImplementationSettings(map[string]any{
			"Sender": []any{map[string]any{"RemoteCluster": "good", "Implementation": "lane", "ShardPercent": 100, "DecodeError": "x"}},
		})
		require.NoError(t, err)
		require.Equal(t, []ReplicationStreamImplementationRule{goodRule}, settings.Sender)
	})
	t.Run("TypedValue", func(t *testing.T) {
		t.Parallel()
		typed := ReplicationStreamImplementationSettings{Sender: []ReplicationStreamImplementationRule{goodRule}}
		settings, err := convertReplicationStreamImplementationSettings(typed)
		require.NoError(t, err)
		require.Equal(t, typed, settings)
	})
	t.Run("Empty", func(t *testing.T) {
		t.Parallel()
		settings, err := convertReplicationStreamImplementationSettings(map[string]any{})
		require.NoError(t, err)
		require.Equal(t, ReplicationStreamImplementationSettings{}, settings)
	})
	t.Run("ValueNotAMap", func(t *testing.T) {
		t.Parallel()
		_, err := convertReplicationStreamImplementationSettings([]any{good})
		require.Error(t, err)
	})
}

// loadYAMLFile reads dynamic config the way a server does: from a file, through the file-based
// client and a collection.
func loadYAMLFile(t *testing.T, contents string) *Collection {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dynamicconfig.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	doneCh := make(chan any)
	t.Cleanup(func() { close(doneCh) })
	client, err := NewFileBasedClient(&FileBasedClientConfig{Filepath: path, PollInterval: time.Minute}, log.NewNoopLogger(), doneCh)
	require.NoError(t, err)
	return NewCollection(client, log.NewNoopLogger())
}

func TestReplicationStreamImplementationFromYAMLFile(t *testing.T) {
	t.Parallel()
	laneB := ReplicationStreamImplementationRule{RemoteCluster: "cluster-b", Implementation: "lane", ShardPercent: 100}

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()
		contents := `
history.ReplicationStreamImplementation:
  - value:
      Sender:
        - RemoteCluster: cluster-b
          Implementation: lane
          ShardPercent: 25
        - RemoteCluster: cluster-c
          Implementation: legacy
          ShardIDs: [1, 2, 3]
      Receiver:
        - remoteCluster: cluster-b
          implementation: lane
          shardPercent: 100
`
		expected := ReplicationStreamImplementationSettings{
			Sender: []ReplicationStreamImplementationRule{
				{RemoteCluster: "cluster-b", Implementation: "lane", ShardPercent: 25},
				{RemoteCluster: "cluster-c", Implementation: "legacy", ShardIDs: []int32{1, 2, 3}},
			},
			Receiver: []ReplicationStreamImplementationRule{laneB},
		}
		require.Equal(t, expected, ReplicationStreamImplementation.Get(loadYAMLFile(t, contents))())

		// A valid value decodes exactly as it did with ConvertStructure.
		loaded := LoadYamlFile([]byte(contents))
		require.NoError(t, loaded.Err())
		raw := loaded.Map[ReplicationStreamImplementation.Key()][0].Value
		previous, err := ConvertStructure(ReplicationStreamImplementationSettings{})(raw)
		require.NoError(t, err)
		require.Equal(t, expected, previous)
	})

	t.Run("BadlyTypedRules", func(t *testing.T) {
		t.Parallel()
		settings := ReplicationStreamImplementation.Get(loadYAMLFile(t, `
history.ReplicationStreamImplementation:
  - value:
      Sender:
        - RemoteCluster: cluster-b
          Implementation: lane
          ShardPercent: 100
        - RemoteCluster: cluster-c
          Implementation: lane
          ShardPercent: "10"
        - RemoteCluster: cluster-d
          Implementation: lane
          ShardIDs: 7
      Receiver:
        - RemoteCluster: cluster-b
          Implementation: lane
          ShardPercent: 100
`))()
		require.Len(t, settings.Sender, 3)
		require.Equal(t, laneB, settings.Sender[0])
		for i, remote := range []string{"cluster-c", "cluster-d"} {
			rule := settings.Sender[i+1]
			require.Equal(t, remote, rule.RemoteCluster)
			require.Error(t, rule.DecodeError)
			require.Empty(t, rule.Implementation)
		}
		require.Equal(t, []ReplicationStreamImplementationRule{laneB}, settings.Receiver)
	})

	t.Run("DirectionNotAList", func(t *testing.T) {
		t.Parallel()
		settings := ReplicationStreamImplementation.Get(loadYAMLFile(t, `
history.ReplicationStreamImplementation:
  - value:
      Sender:
        RemoteCluster: cluster-b
        Implementation: lane
        ShardPercent: 100
      Receiver:
        - RemoteCluster: cluster-b
          Implementation: lane
          ShardPercent: 100
`))()
		require.Len(t, settings.Sender, 1)
		require.Equal(t, "cluster-b", settings.Sender[0].RemoteCluster)
		require.ErrorContains(t, settings.Sender[0].DecodeError, "not a list")
		require.Equal(t, []ReplicationStreamImplementationRule{laneB}, settings.Receiver)
	})
}

func TestConvertReplicationLaneIsolationEntries(t *testing.T) {
	t.Parallel()
	good := map[string]any{"RemoteCluster": "good", "NamespaceID": "ns-a", "TasksPerSecond": 10}
	goodEntry := ReplicationLaneIsolationEntry{RemoteCluster: "good", NamespaceID: "ns-a", TasksPerSecond: 10}
	value := []any{good, map[string]any{"RemoteCluster": "bad", "NamespaceID": "ns-b", "TasksPerSecond": "10"}}

	entries, err := convertReplicationLaneIsolationEntries(value)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, goodEntry, entries[0])
	require.Equal(t, "bad", entries[1].RemoteCluster)
	require.Error(t, entries[1].DecodeError)
	require.Equal(t, ReplicationLaneIsolationEntry{RemoteCluster: "bad", DecodeError: entries[1].DecodeError}, entries[1])

	// The collection keeps the entries that decode instead of falling back to the default.
	collection := NewCollection(StaticClient{ReplicationLaneIsolation.Key(): value}, log.NewNoopLogger())
	require.Equal(t, goodEntry, ReplicationLaneIsolation.Get(collection)()[0])

	entries, err = convertReplicationLaneIsolationEntries(good)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "good", entries[0].RemoteCluster)
	require.ErrorContains(t, entries[0].DecodeError, "not a list")

	typed := []ReplicationLaneIsolationEntry{goodEntry}
	entries, err = convertReplicationLaneIsolationEntries(typed)
	require.NoError(t, err)
	require.Equal(t, typed, entries)
}

func TestReplicationLaneIsolationFromYAMLFile(t *testing.T) {
	t.Parallel()
	valid := []ReplicationLaneIsolationEntry{
		{RemoteCluster: "cluster-b", NamespaceID: "ns-a", TasksPerSecond: 2.5},
		{RemoteCluster: "cluster-b", NamespaceID: "ns-b", TasksPerSecond: 10},
	}
	const validYAML = `
history.ReplicationLaneIsolation:
  - value:
      - RemoteCluster: cluster-b
        NamespaceID: ns-a
        TasksPerSecond: 2.5
      - remoteCluster: cluster-b
        namespaceID: ns-b
        tasksPerSecond: 10
`
	require.Equal(t, valid, ReplicationLaneIsolation.Get(loadYAMLFile(t, validYAML))())

	// A valid value decodes exactly as it did with ConvertStructure.
	loaded := LoadYamlFile([]byte(validYAML))
	require.NoError(t, loaded.Err())
	previous, err := ConvertStructure([]ReplicationLaneIsolationEntry(nil))(loaded.Map[ReplicationLaneIsolation.Key()][0].Value)
	require.NoError(t, err)
	require.Equal(t, valid, previous)

	entries := ReplicationLaneIsolation.Get(loadYAMLFile(t, validYAML+`      - RemoteCluster: cluster-c
        NamespaceID: ns-c
        TasksPerSecond: "10"
`))()
	require.Len(t, entries, 3)
	require.Equal(t, valid, entries[:2])
	require.Equal(t, "cluster-c", entries[2].RemoteCluster)
	require.Error(t, entries[2].DecodeError)
	require.Empty(t, entries[2].NamespaceID)
}
