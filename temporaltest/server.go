// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package temporaltest provides utilities for end to end Temporal server testing.
package temporaltest

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/internal/temporalite"
	"go.temporal.io/server/temporal"
)

// A TestServer is a Temporal server listening on a system-chosen port on the
// local loopback interface, for use in end-to-end tests.
//
// Methods on TestServer are not safe for concurrent use.
type TestServer struct {
	server               *temporalite.LiteServer
	defaultTestNamespace string
	defaultClient        client.Client
	clients              []client.Client
	workers              []worker.Worker
	t                    *testing.T
	defaultClientOptions client.Options
	defaultWorkerOptions worker.Options
	serverOptions        []temporal.ServerOption
}

func (ts *TestServer) fatal(err error) {
	if ts.t == nil {
		panic(err)
	}
	ts.t.Fatal(err)
}

// NewWorker registers and starts a Temporal worker on the specified task queue.
func (ts *TestServer) NewWorker(taskQueue string, registerFunc func(registry worker.Registry)) worker.Worker {
	return ts.NewWorkerWithOptions(taskQueue, registerFunc, ts.defaultWorkerOptions)
}

// NewWorkerWithOptions returns a Temporal worker on the specified task queue.
//
// WorkflowPanicPolicy is always set to worker.FailWorkflow so that workflow executions
// fail fast when workflow code panics or detects non-determinism.
func (ts *TestServer) NewWorkerWithOptions(taskQueue string, registerFunc func(registry worker.Registry), opts worker.Options) worker.Worker {
	opts.WorkflowPanicPolicy = worker.FailWorkflow

	w := worker.New(ts.GetDefaultClient(), taskQueue, opts)
	registerFunc(w)
	ts.workers = append(ts.workers, w)

	if err := w.Start(); err != nil {
		ts.fatal(err)
	}

	return w
}

// GetDefaultClient returns the default Temporal client configured for making requests to the server.
//
// It is configured to use a pre-registered test namespace and will be closed on TestServer.Stop.
func (ts *TestServer) GetDefaultClient() client.Client {
	if ts.defaultClient == nil {
		ts.defaultClient = ts.NewClientWithOptions(ts.defaultClientOptions)
	}
	return ts.defaultClient
}

// GetDefaultNamespace returns the randomly generated namespace which has been pre-registered with the test server.
func (ts *TestServer) GetDefaultNamespace() string {
	return ts.defaultTestNamespace
}

// NewClientWithOptions returns a new Temporal client configured for making requests to the server.
//
// If no namespace option is set it will use a pre-registered test namespace.
// The returned client will be closed on TestServer.Stop.
func (ts *TestServer) NewClientWithOptions(opts client.Options) client.Client {
	if opts.Namespace == "" {
		opts.Namespace = ts.defaultTestNamespace
	}
	if opts.Logger == nil {
		opts.Logger = &testLogger{ts.t}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := ts.server.NewClientWithOptions(ctx, opts)
	if err != nil {
		ts.fatal(fmt.Errorf("error creating client: %w", err))
	}

	ts.clients = append(ts.clients, c)

	return c
}

// Stop closes test clients and shuts down the server.
func (ts *TestServer) Stop() {
	for _, w := range ts.workers {
		w.Stop()
	}
	for _, c := range ts.clients {
		c.Close()
	}
	if err := ts.server.Stop(); err != nil {
		// Log instead of throwing error because there's no need to fail the test
		// if it already succeeded.
		ts.t.Logf("error shutting down Temporal server: %s", err)
	}
}

// NewServer starts and returns a new TestServer.
//
// If not specifying the WithT option, the caller should execute Stop when finished to close
// the server and release resources.
func NewServer(opts ...TestServerOption) *TestServer {
	testNamespace := fmt.Sprintf("temporaltest-%d", rand.Intn(1e6))

	ts := TestServer{
		defaultTestNamespace: testNamespace,
	}

	// Apply options
	for _, opt := range opts {
		opt.apply(&ts)
	}

	if ts.t != nil {
		ts.t.Cleanup(ts.Stop)
	}

	s, err := temporalite.NewLiteServer(&temporalite.LiteServerConfig{
		Namespaces: []string{ts.defaultTestNamespace},
		Ephemeral:  true,
		Logger:     log.NewNoopLogger(),
		DynamicConfig: dynamicconfig.StaticClient{
			dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Key(): []dynamicconfig.ConstrainedValue{{Value: true}},
		},
		// Disable "accept incoming network connections?" prompt on macOS
		FrontendIP: "127.0.0.1",
	}, ts.serverOptions...)
	if err != nil {
		ts.fatal(fmt.Errorf("error creating server: %w", err))
	}
	ts.server = s

	// Start does not block as long as InterruptOn is unset.
	if err := s.Start(); err != nil {
		ts.fatal(err)
	}

	// This sleep helps avoid a panic in github.com/temporalio/ringpop-go@v0.0.0-20230606200434-b5c079f412d3/swim/labels.go:175
	time.Sleep(100 * time.Millisecond)

	return &ts
}
