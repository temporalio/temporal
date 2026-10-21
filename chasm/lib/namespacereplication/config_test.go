package namespacereplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestLocalRetryExpiration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		config *config
		want   time.Duration
	}{
		{
			name: "nil config uses the five-minute default",
			want: 5 * time.Minute,
		},
		{
			name: "configured value",
			config: &config{
				localRetryExpirationInterval: dynamicconfig.GetDurationPropertyFn(2 * time.Hour),
			},
			want: 2 * time.Hour,
		},
		{
			name: "invalid value uses default",
			config: &config{
				localRetryExpirationInterval: dynamicconfig.GetDurationPropertyFn(0),
			},
			want: 5 * time.Minute,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.config.localRetryExpiration())
		})
	}
}

func TestPeerRetryPolicy(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		config *config
		want   peerRetryPolicy
	}{
		{
			name: "nil config uses defaults",
			want: defaultPeerRetryPolicy(),
		},
		{
			name: "configured values",
			config: &config{
				peerRetryInitialInterval:    dynamicconfig.GetDurationPropertyFn(3 * time.Second),
				peerRetryMaximumInterval:    dynamicconfig.GetDurationPropertyFn(20 * time.Second),
				peerRetryExpirationInterval: dynamicconfig.GetDurationPropertyFn(2 * time.Hour),
			},
			want: peerRetryPolicy{
				initialInterval:    3 * time.Second,
				maximumInterval:    20 * time.Second,
				expirationInterval: 2 * time.Hour,
			},
		},
		{
			name: "invalid values fall back and maximum is not below initial",
			config: &config{
				peerRetryInitialInterval:    dynamicconfig.GetDurationPropertyFn(10 * time.Minute),
				peerRetryMaximumInterval:    dynamicconfig.GetDurationPropertyFn(-time.Second),
				peerRetryExpirationInterval: dynamicconfig.GetDurationPropertyFn(0),
			},
			want: peerRetryPolicy{
				initialInterval:    10 * time.Minute,
				maximumInterval:    10 * time.Minute,
				expirationInterval: defaultPeerRetryExpirationInterval,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.config.peerRetryPolicy())
		})
	}
}
