package versioninfo_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/versioninfo"
	"go.uber.org/goleak"
)

func newTestCaller(t *testing.T, handler http.HandlerFunc) versioninfo.Caller {
	t.Helper()

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	u, err := url.Parse(ts.URL)
	require.NoError(t, err)

	return versioninfo.Caller{Scheme: u.Scheme, Host: u.Host}
}

func testRequest() *versioninfo.VersionCheckRequest {
	return &versioninfo.VersionCheckRequest{
		Product:   "server",
		Version:   "0.1",
		Arch:      "arm64",
		OS:        "darwin",
		DB:        "sqlite",
		ClusterID: "foo",
		Timestamp: 1,
		SDKInfo:   []versioninfo.SDKInfo{{Name: "sdk-go", Version: "1.0"}},
	}
}

func TestCall_NonOKResponse(t *testing.T) {
	caller := newTestCaller(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	_, err := caller.Call(testRequest())
	require.ErrorContains(t, err, "bad response code 500")
}

// A non-200 response must still close the body. Keep-alives are disabled, so a
// closed body ends the connection and its read and write loops exit; a leaked
// body leaves them running for the lifetime of the process.
func TestCall_NonOKResponseClosesBody(t *testing.T) {
	caller := newTestCaller(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		// A body large enough that it is not already buffered and drained: an
		// unread body is what pins the connection open.
		_, _ = w.Write([]byte(strings.Repeat("error details ", 4096)))
	})

	// Warm up so the server's per-connection goroutines are not mistaken for a
	// leak, then snapshot and measure a single call.
	_, err := caller.Call(testRequest())
	require.ErrorContains(t, err, "bad response code 500")

	baseline := goleak.IgnoreCurrent()
	_, err = caller.Call(testRequest())
	require.ErrorContains(t, err, "bad response code 500")
	require.NoError(t, goleak.Find(baseline))
}
