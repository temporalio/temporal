package ringpop

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestHostsReplacesView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		refreshedHost []string
	}{
		{
			name:          "non-empty view",
			refreshedHost: []string{"new-host"},
		},
		{
			name:          "empty view",
			refreshedHost: []string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			refreshCalls := 0
			provider := newRefreshingHosts(log.NewNoopLogger(), func() ([]string, error) {
				refreshCalls++
				return test.refreshedHost, nil
			}, "initial")

			hosts, err := provider.Hosts()

			require.NoError(t, err)
			require.Equal(t, test.refreshedHost, hosts)
			require.Equal(t, 1, refreshCalls)

			hosts, err = provider.Hosts()

			require.NoError(t, err)
			require.Equal(t, test.refreshedHost, hosts)
			require.Equal(t, 2, refreshCalls)
		})
	}
}

func TestHostsRetainsViewAndRetriesAfterRefreshError(t *testing.T) {
	t.Parallel()

	refreshCalls := 0
	provider := newRefreshingHosts(log.NewNoopLogger(), func() ([]string, error) {
		refreshCalls++
		if refreshCalls == 1 {
			return nil, errors.New("persistence unavailable")
		}
		return []string{"refreshed"}, nil
	}, "initial")

	hosts, err := provider.Hosts()

	require.NoError(t, err)
	require.Equal(t, []string{"initial"}, hosts)
	require.Equal(t, 1, refreshCalls)

	hosts, err = provider.Hosts()

	require.NoError(t, err)
	require.Equal(t, []string{"refreshed"}, hosts)
	require.Equal(t, 2, refreshCalls)
}
