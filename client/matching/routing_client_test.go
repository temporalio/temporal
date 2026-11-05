package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

// routeOnlyClient is a client wrapper that forwards Route but doesn't know about gradual changes.
type routeOnlyClient struct {
	*matchingservicemock.MockMatchingServiceClient
}

func (routeOnlyClient) Route(tqid.Partition) (string, error) {
	return "host", nil
}

func TestWrappersKeepRoutingWithoutRouteChangeClient(t *testing.T) {
	inner := routeOnlyClient{matchingservicemock.NewMockMatchingServiceClient(gomock.NewController(t))}
	policy := backoff.NewExponentialRetryPolicy(time.Millisecond)
	isRetryable := func(error) bool { return false }

	wrappers := map[string]matchingservice.MatchingServiceClient{
		"metric":    NewMetricClient(inner, metrics.NoopMetricsHandler, log.NewNoopLogger(), log.NewNoopLogger()),
		"retryable": NewRetryableClient(inner, policy, policy, isRetryable),
	}
	family, err := tqid.NewTaskQueueFamily("ns", "tq")
	require.NoError(t, err)
	partition := family.TaskQueue(0).RootPartition()

	for name, client := range wrappers {
		t.Run(name, func(t *testing.T) {
			addr, err := client.(RoutingClient).Route(partition)
			require.NoError(t, err)
			require.Equal(t, "host", addr)
			require.True(t, client.(RouteChangeClient).NextRouteChange(partition).IsZero())
		})
	}
}
