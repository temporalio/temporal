package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

// TestWorkerGroupRetention verifies that registration uses the namespace's configured
// worker-group retention and leaves namespace retention in effect when unset.
func TestWorkerGroupRetention(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	client.OverrideSetting(WorkerGroupRetention, []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: "custom-retention"},
		Value:       6 * time.Hour,
	}})
	config := configProvider(dynamicconfig.NewCollection(client, log.NewNoopLogger()))
	group := newLibrary(nil, config).Components()[0]
	for _, tc := range []struct {
		name       string
		retention  time.Duration
		overridden bool
	}{
		{name: "custom-retention", retention: 6 * time.Hour, overridden: true},
		{name: "namespace-retention"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: tc.name}, nil, "active")
			retention, overridden := group.Retention(ns)
			require.Equal(t, tc.retention, retention)
			require.Equal(t, tc.overridden, overridden)
		})
	}
}
