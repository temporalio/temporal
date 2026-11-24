package httpfaultstest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
)

// TestNewGenerator_WithoutTestHooks runs in builds with and without test_dep: zero test hooks are
// disabled in both, and NewTestHooks only enables them with test_dep.
func TestNewGenerator_WithoutTestHooks(t *testing.T) {
	t.Parallel()

	require.Nil(t, httpfaultstest.NewGenerator(testhooks.TestHooks{}))
	testHooks := testhooks.NewTestHooks()
	require.Equal(t, testHooks.Enabled(), httpfaultstest.NewGenerator(testHooks) != nil)
}
