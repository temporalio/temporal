package tests_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests"
	"go.temporal.io/server/tests/testcore"
)

type externalFakeClusterFactory struct {
	called bool
}

func (f *externalFakeClusterFactory) NewCluster(*testing.T, *testcore.ClusterConfig, log.Logger) (testcore.Cluster, error) {
	f.called = true
	return nil, errors.New("unexpected cluster creation")
}

func TestFunctionalTestRegistryExternalUse(t *testing.T) {
	entries := tests.FunctionalTestEntries()
	require.Len(t, entries, 137)
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name
	}
	require.True(t, slices.IsSorted(names))

	factory := &externalFakeClusterFactory{}
	tests.Run(t, factory, tests.WithFunctionalTestPredicate(func(tests.FunctionalTestEntry) bool {
		return false
	}))
	require.False(t, factory.called)
}
