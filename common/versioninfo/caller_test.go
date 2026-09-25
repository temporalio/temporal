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

func TestCaller_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("Parse URL failed: %s", err)
	}
	caller := &versioninfo.Caller{
		Scheme:  u.Scheme,
		Host:    u.Host,
		Timeout: 50 * time.Millisecond,
	}
	_, err = caller.Call(&versioninfo.VersionCheckRequest{
		Product:   "server",
		Version:   "0.1",
		ClusterID: "foo",
		DB:        "cassandra",
		OS:        "linux",
		Arch:      "arm64",
		Timestamp: time.Now().UnixNano(),
	})
	if err == nil {
		t.Fatal("expected caller to time out, got nil error")
	}
}

func TestCaller_ContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("Parse URL failed: %s", err)
	}
	caller := &versioninfo.Caller{
		Scheme:  u.Scheme,
		Host:    u.Host,
		Timeout: 1 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = caller.CallWithContext(ctx, &versioninfo.VersionCheckRequest{
		Product:   "server",
		Version:   "0.1",
		ClusterID: "foo",
		DB:        "cassandra",
		OS:        "linux",
		Arch:      "arm64",
		Timestamp: time.Now().UnixNano(),
	})
	if err == nil {
		t.Fatal("expected caller to return error on cancelled context, got nil")
	}
}
