package versioninfo_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/versioninfo"
)

func TestPostInfo(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Method != POST (%s)", r.Method)
		}
		if r.URL.Path != "/check" {
			t.Errorf("URL.Path != /check (%s)", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type != application/json (%s)", r.Header.Get("Content-Type"))
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read request body %s", err)
		}
		versionCheckRequest := &versioninfo.VersionCheckRequest{}
		err = json.Unmarshal(body, versionCheckRequest)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %s", err)
		}
		// Unmarshalling works
		res, err := json.Marshal(versioninfo.VersionCheckResponse{
			Products: []versioninfo.ProductVersionReport{
				{
					Product: "server",
					Current: versioninfo.ReleaseInfo{
						Version:     "0.1",
						ReleaseTime: time.Now().UnixNano(),
						Notes:       "",
					},
					Recommended: versioninfo.ReleaseInfo{
						Version:     "0.1",
						ReleaseTime: time.Now().UnixNano(),
						Notes:       "",
					},
					Instructions: "instructions",
					Alerts:       []versioninfo.Alert{},
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to marshal response %s", err)
		}
		if _, err := w.Write(res); err != nil {
			t.Fatalf("Failed to write response %s", err)
		}
	}))
	t.Cleanup(ts.Close)
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("Request failed: %s", err)
	}
	caller := &versioninfo.Caller{Scheme: u.Scheme, Host: u.Host}
	sdkInfo := []versioninfo.SDKInfo{{
		Name:    "sdk-java",
		Version: "3.11",
	}}
	_, err = caller.Call(&versioninfo.VersionCheckRequest{
		Product:   "server",
		Version:   "0.1",
		ClusterID: "foo",
		DB:        "cassandra",
		OS:        "linux",
		Arch:      "arm64",
		Timestamp: time.Now().UnixNano(),
		SDKInfo:   sdkInfo,
	})
	if err != nil {
		t.Fatalf("Request failed: %s", err)
	}
}

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
		http.Error(w, "error details", http.StatusInternalServerError)
	})

	_, err := caller.Call(testRequest())
	require.ErrorContains(t, err, "bad response code 500")
}

func TestCall_StalledServerTimesOut(t *testing.T) {
	release := make(chan struct{})
	caller := newTestCaller(t, func(http.ResponseWriter, *http.Request) {
		<-release
	})
	// Registered after the server's Close so it runs first: Close waits for the
	// handler to return.
	t.Cleanup(func() { close(release) })
	caller.Timeout = 100 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := caller.Call(testRequest())
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	// Without the select, an unbounded Call hangs the package until its timeout.
	case <-time.After(10 * time.Second):
		t.Fatal("Call must not outlive its timeout")
	}
}
