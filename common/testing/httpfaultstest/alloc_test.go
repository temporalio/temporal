//go:build test_dep

package httpfaultstest_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint/buggify"
)

// wrapAllocs is the number of allocations of a call of the wrapper itself, which the failpoints
// don't add to.
const wrapAllocs = 2

// TestWrap_Allocations is not parallel, since AllocsPerRun doesn't allow it.
func TestWrap_Allocations(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusOK}
	req, err := http.NewRequest(http.MethodPost, "http://example.com/callback", nil)
	require.NoError(t, err)

	buggifyHooks := func(activation float64) testhooks.TestHooks {
		testHooks := testhooks.NewTestHooks()
		require.NoError(t, buggify.For(testHooks.Failpoints()).Set(buggify.Config{Seed: 1, Activation: activation, Fire: 0, Error: "Unavailable"}))
		return testHooks
	}
	for name, testHooks := range map[string]testhooks.TestHooks{
		"no failpoints":               testhooks.NewTestHooks(),
		"BUGGIFY, not activated":      buggifyHooks(0),
		"BUGGIFY, activated, no fire": buggifyHooks(1),
	} {
		wrapped := httpfaults.Wrap(
			httpfaultstest.NewGenerator(testHooks),
			httpfaults.Scope{NamespaceID: "namespace-id"},
			func(*http.Request) (*http.Response, error) { return resp, nil },
		)
		_, _ = wrapped(req)
		allocs := testing.AllocsPerRun(100, func() { _, _ = wrapped(req) })
		require.InDelta(t, wrapAllocs, allocs, 0, name)
	}
}
