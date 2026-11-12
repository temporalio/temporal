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

	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
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

func TestCallerTimeout(t *testing.T) {
	t.Parallel()
	started := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		started <- err
		if err != nil {
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	t.Cleanup(server.CloseClientConnections)
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	caller := versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host, Timeout: 200 * time.Millisecond}
	ctx := testcontext.For(t, testcontext.WithTimeout(5*time.Second))
	_, err = caller.CallWithContext(ctx, validVersionCheckRequest())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, ctx.Err(), "the HTTP client deadline must expire before the parent context")
	require.NoError(t, await.Rcv(t, started))
}

func TestCallerCancellation(t *testing.T) {
	t.Parallel()
	for _, cancelBeforeRequest := range []bool{true, false} {
		name := "in flight"
		if cancelBeforeRequest {
			name = "already canceled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			started := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, err := io.Copy(io.Discard, r.Body)
				started <- err
				if err != nil {
					return
				}
				<-r.Context().Done()
			}))
			t.Cleanup(server.Close)
			t.Cleanup(server.CloseClientConnections)
			endpoint, err := url.Parse(server.URL)
			require.NoError(t, err)
			caller := versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host, Timeout: 10 * time.Second}
			ctx, cancel := context.WithCancel(testcontext.For(t, testcontext.WithTimeout(5*time.Second)))
			t.Cleanup(cancel)
			if cancelBeforeRequest {
				cancel()
			}
			result := make(chan error, 1)
			go func() {
				_, err := caller.CallWithContext(ctx, validVersionCheckRequest())
				result <- err
			}()
			if !cancelBeforeRequest {
				require.NoError(t, await.Rcv(t, started))
				cancel()
			}
			require.ErrorIs(t, await.Rcv(t, result), context.Canceled)
			if cancelBeforeRequest {
				select {
				case <-started:
					t.Fatal("an already canceled request reached the server")
				default:
				}
			}
		})
	}
}

func validVersionCheckRequest() *versioninfo.VersionCheckRequest {
	return &versioninfo.VersionCheckRequest{
		Product: "server", Version: "0.1", ClusterID: "cluster", DB: "cassandra",
		OS: "linux", Arch: "arm64", Timestamp: 1,
	}
}
