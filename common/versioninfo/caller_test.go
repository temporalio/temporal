package versioninfo_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/versioninfo"
	"go.uber.org/goleak"
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
		w.WriteHeader(http.StatusInternalServerError)
	})

	_, err := caller.Call(testRequest())
	require.ErrorContains(t, err, "bad response code 500")
}

// A non-200 response must still close the body. Keep-alives are disabled, so
// closing ends the connection and its read and write loops exit.
//
// Must not run in parallel: goleak.Find observes the whole process.
func TestCall_NonOKResponseClosesBody(t *testing.T) {
	caller := newTestCaller(t, func(w http.ResponseWriter, _ *http.Request) {
		// An empty body is drained for us, so send one that must be read.
		http.Error(w, "error details", http.StatusInternalServerError)
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
