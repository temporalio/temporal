//go:build test_dep

package failpointserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/testhooks"
)

func TestPeers_Targets_LimitsPeersToEnabledServices(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	testhooks.Set(testHooks, testhooks.FailpointFanOutServices,
		[]primitives.ServiceName{primitives.FrontendService, primitives.HistoryService, primitives.MatchingService},
		testhooks.GlobalScope)
	p := newTestPeers(newTestMonitor(), testHooks, nil)
	t.Cleanup(p.close)

	targets, err := p.targets(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"frontend/fe-2:7233", "history/history-1:7234", "matching/matching-1:7235"}, targetHosts(targets))
}
