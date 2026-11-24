package httpfaultstest_test

import (
	"net/http"
	"testing"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/testx/failpoint/buggify"
)

// BenchmarkWrap_NoFailpoints measures the cost of the HTTP fault wrapper with failpoints enabled while
// no failpoint is registered.
func BenchmarkWrap_NoFailpoints(b *testing.B) {
	resp := &http.Response{StatusCode: http.StatusOK}
	wrapped := httpfaults.Wrap(
		httpfaultstest.NewGenerator(failpoints.NewEnabledSource()),
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

// BenchmarkWrap_Buggify measures the cost of the HTTP fault wrapper while BUGGIFY is enabled and no
// failpoint is registered, for an operation that BUGGIFY did not activate and one that it
// activated but whose execution doesn't fire.
func BenchmarkWrap_Buggify(b *testing.B) {
	resp := &http.Response{StatusCode: http.StatusOK}
	req, err := http.NewRequest(http.MethodPost, "http://example.com/callback", nil)
	if err != nil {
		b.Fatal(err)
	}
	for _, bc := range []struct {
		name       string
		activation float64
	}{
		{name: "not activated", activation: 0},
		{name: "activated", activation: 1},
	} {
		b.Run(bc.name, func(b *testing.B) {
			source := failpoints.NewEnabledSource()
			if err := buggify.For(source.Failpoints()).Set(buggify.Config{Seed: 1, Activation: bc.activation, Fire: 0, Error: "Unavailable"}); err != nil {
				b.Fatal(err)
			}
			wrapped := httpfaults.Wrap(
				httpfaultstest.NewGenerator(source),
				httpfaults.Scope{NamespaceID: "namespace-id"},
				func(*http.Request) (*http.Response, error) { return resp, nil },
			)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = wrapped(req)
			}
		})
	}
}
