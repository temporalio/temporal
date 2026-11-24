//go:build test_dep

package httpfaultstest_test

import (
	"net/http"
	"testing"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
)

// BenchmarkWrap_NoFailpoints measures the cost of the HTTP fault wrapper in a test_dep build while
// no failpoint is registered.
func BenchmarkWrap_NoFailpoints(b *testing.B) {
	resp := &http.Response{StatusCode: http.StatusOK}
	wrapped := httpfaults.Wrap(
		httpfaultstest.NewGenerator(testhooks.NewTestHooks()),
		httpfaults.Scope{NamespaceID: "namespace-id"},
		func(*http.Request) (*http.Response, error) { return resp, nil },
	)
	req, err := http.NewRequest(http.MethodPost, "http://example.com/callback", nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = wrapped(req)
	}
}
