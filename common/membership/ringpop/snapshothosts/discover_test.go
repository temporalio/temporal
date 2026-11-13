package snapshothosts

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestHostsReturnsInitialSnapshotBeforeExpiry(t *testing.T) {
	t.Parallel()

	refreshCalls := 0
	provider := New(log.NewNoopLogger(), func() ([]string, error) {
		refreshCalls++
		return []string{"refreshed"}, nil
	}, time.Hour, "initial")

	hosts, err := provider.Hosts()

	require.NoError(t, err)
	require.Equal(t, []string{"initial"}, hosts)
	require.Equal(t, 0, refreshCalls)
}

func TestHostsReplacesExpiredSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		refreshedHost []string
	}{
		{
			name:          "non-empty snapshot",
			refreshedHost: []string{"new-host"},
		},
		{
			name:          "empty snapshot",
			refreshedHost: []string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			refreshCalls := 0
			provider := New(log.NewNoopLogger(), func() ([]string, error) {
				refreshCalls++
				return test.refreshedHost, nil
			}, time.Hour, "initial")
			provider.expiresAt = time.Time{}

			hosts, err := provider.Hosts()

			require.NoError(t, err)
			require.Equal(t, test.refreshedHost, hosts)
			require.Equal(t, 1, refreshCalls)
			require.True(t, provider.expiresAt.After(time.Now()))

			hosts, err = provider.Hosts()

			require.NoError(t, err)
			require.Equal(t, test.refreshedHost, hosts)
			require.Equal(t, 1, refreshCalls)
		})
	}
}

func TestHostsRetainsSnapshotAndRetriesAfterRefreshError(t *testing.T) {
	t.Parallel()

	refreshCalls := 0
	provider := New(log.NewNoopLogger(), func() ([]string, error) {
		refreshCalls++
		if refreshCalls == 1 {
			return nil, errors.New("database unavailable")
		}
		return []string{"refreshed"}, nil
	}, time.Hour, "initial")
	provider.expiresAt = time.Time{}

	hosts, err := provider.Hosts()

	require.NoError(t, err)
	require.Equal(t, []string{"initial"}, hosts)
	require.Equal(t, 1, refreshCalls)
	require.True(t, provider.expiresAt.Before(time.Now()))

	hosts, err = provider.Hosts()

	require.NoError(t, err)
	require.Equal(t, []string{"refreshed"}, hosts)
	require.Equal(t, 2, refreshCalls)
	require.True(t, provider.expiresAt.After(time.Now()))
}
