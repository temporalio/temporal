package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestComputeStorageUsageDelta(t *testing.T) {
	base := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		msSize      int64
		historySize int64
		last        time.Time
		now         time.Time
		expected    storageUsageDelta
	}{
		{
			name:        "no baseline",
			msSize:      1000,
			historySize: 5000,
			now:         base,
			expected:    storageUsageDelta{},
		},
		{
			name:        "whole seconds",
			msSize:      1000,
			historySize: 5000,
			last:        base,
			now:         base.Add(10 * time.Second),
			expected:    storageUsageDelta{MutableStateByteSeconds: 10000, HistoryByteSeconds: 50000},
		},
		{
			name:     "fractional seconds",
			msSize:   1000,
			last:     base,
			now:      base.Add(1500 * time.Millisecond),
			expected: storageUsageDelta{MutableStateByteSeconds: 1500},
		},
		{
			name:        "clock went backwards",
			msSize:      1000,
			historySize: 5000,
			last:        base,
			now:         base.Add(-5 * time.Second),
			expected:    storageUsageDelta{},
		},
		{
			name:     "zero size",
			last:     base,
			now:      base.Add(time.Hour),
			expected: storageUsageDelta{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, computeStorageUsageDelta(tc.msSize, tc.historySize, tc.last, tc.now))
		})
	}
}

func TestAddStorageUsage(t *testing.T) {
	usage := addStorageUsage(nil, storageUsageDelta{MutableStateByteSeconds: 1, HistoryByteSeconds: 2})
	require.NotNil(t, usage)
	require.Equal(t, int64(1), usage.GetMutableStateByteSeconds())
	require.Equal(t, int64(2), usage.GetHistoryByteSeconds())

	usage = addStorageUsage(
		&persistencespb.StorageUsage{MutableStateByteSeconds: 100, HistoryByteSeconds: 200},
		storageUsageDelta{MutableStateByteSeconds: 1500},
	)
	require.Equal(t, int64(1600), usage.GetMutableStateByteSeconds())
	require.Equal(t, int64(200), usage.GetHistoryByteSeconds())
}
