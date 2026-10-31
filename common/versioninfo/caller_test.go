package versioninfo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
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
	_, err = caller.Call(t.Context(), &versioninfo.VersionCheckRequest{
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

// TestCallIsBoundedByContext pins the fix for #11943. A server that accepts the
// request and never responds used to block Call forever: the client had no
// Timeout and the request carried no context, so neither the caller's deadline
// nor VersionChecker.Stop could reach it.
//
// The handler signals on entry and the context is cancelled only after that
// signal, so the request is provably in flight when it is cancelled. Cancelling
// earlier would let the test pass on a request that never reached the server,
// which is not what Stop relies on.
func TestCallIsBoundedByContext(t *testing.T) {
	t.Parallel()

	arrived := make(chan struct{})
	released := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(arrived) })
		<-released // accept, then never answer
	}))
	defer func() {
		close(released)
		ts.Close()
	}()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse url: %s", err)
	}
	caller := &versioninfo.Caller{Scheme: u.Scheme, Host: u.Host}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, callErr := caller.Call(ctx, &versioninfo.VersionCheckRequest{
			Product:   "server",
			Version:   "0.1",
			ClusterID: "foo",
			DB:        "cassandra",
			OS:        "linux",
			Arch:      "arm64",
			Timestamp: time.Now().UnixNano(),
			SDKInfo:   []versioninfo.SDKInfo{{Name: "sdk-java", Version: "3.11"}},
		})
		done <- callErr
	}()

	// Only cancel once the server has the request, so this exercises an in-flight
	// call rather than one cancelled before it left.
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("request never reached the server")
	}
	cancel()

	select {
	case callErr := <-done:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("Call returned %v, want context.Canceled", callErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Call did not return after its context was cancelled; the request is not bound to the context")
	}
}
