package grpcfaultstest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint/failpointpb"
)

// TestNewGenerator_WithoutTestHooks runs in builds with and without test_dep: zero test hooks are
// disabled in both, and NewTestHooks only enables them with test_dep.
func TestNewGenerator_WithoutTestHooks(t *testing.T) {
	t.Parallel()

	require.Nil(t, grpcfaultstest.NewGenerator(testhooks.TestHooks{}, nil))
	testHooks := testhooks.NewTestHooks()
	require.Equal(t, testHooks.Enabled(), grpcfaultstest.NewGenerator(testHooks, nil) != nil)
}

func TestFailpointServicePrefix(t *testing.T) {
	t.Parallel()

	require.Equal(t, grpcfaultstest.FailpointServicePrefix, "/"+failpointpb.FailpointService_ServiceDesc.ServiceName+"/")
}
