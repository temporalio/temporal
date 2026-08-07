package versioninfo

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/goleak"
)

// A stalled server must not hold the transport's goroutines open, which is what
// lets the persistConn read and write loop ignores stay out of goleakOpts. Not
// parallel, because goleak.Find observes the whole process.
func TestCall_StalledServerTimesOut(t *testing.T) {
	previous := callTimeout
	callTimeout = 100 * time.Millisecond
	t.Cleanup(func() { callTimeout = previous })

	release := make(chan struct{})
	stopHandler := sync.OnceFunc(func() { close(release) })
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	// Registered first so it runs last: Close waits for the handler to return.
	t.Cleanup(ts.Close)
	t.Cleanup(stopHandler)

	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	caller := Caller{Scheme: u.Scheme, Host: u.Host}
	request := &VersionCheckRequest{
		Product:   "server",
		Version:   "0.1",
		Arch:      "arm64",
		OS:        "darwin",
		DB:        "sqlite",
		ClusterID: "foo",
		Timestamp: 1,
	}

	baseline := goleak.IgnoreCurrent()
	done := make(chan error, 1)
	go func() {
		_, callErr := caller.Call(request)
		done <- callErr
	}()

	select {
	case callErr := <-done:
		require.ErrorContains(t, callErr, "Client.Timeout exceeded")
	case <-time.After(10 * time.Second):
		t.Fatal("Call must not outlive its timeout")
	}

	// Let the handler return so only the client's goroutines are left to observe.
	stopHandler()
	await.RequireTrue(t, func() bool { return goleak.Find(baseline) == nil },
		10*time.Second, 20*time.Millisecond)
}
