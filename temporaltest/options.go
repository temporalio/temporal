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

package temporaltest

import (
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"go.temporal.io/server/temporal"
)

type TestServerOption interface {
	apply(*TestServer)
}

type applyFunc func(*TestServer)

func (f applyFunc) apply(s *TestServer) { f(s) }

// WithT directs all worker and client logs to the test logger.
//
// If this option is specified, then server will automatically be stopped when the
// test completes.
func WithT(t *testing.T) TestServerOption {
	return applyFunc(func(server *TestServer) {
		server.t = t
	})
}

// WithBaseClientOptions configures options for the default clients and workers connected to the test server.
func WithBaseClientOptions(o client.Options) TestServerOption {
	return applyFunc(func(server *TestServer) {
		server.defaultClientOptions = o
	})
}

// WithBaseWorkerOptions configures default options for workers connected to the test server.
//
// WorkflowPanicPolicy is always set to worker.FailWorkflow so that workflow executions
// fail fast when workflow code panics or detects non-determinism.
func WithBaseWorkerOptions(o worker.Options) TestServerOption {
	o.WorkflowPanicPolicy = worker.FailWorkflow
	return applyFunc(func(server *TestServer) {
		server.defaultWorkerOptions = o
	})
}

// WithBaseServerOptions enables configuring additional server options not directly exposed via temporaltest.
func WithBaseServerOptions(options ...temporal.ServerOption) TestServerOption {
	return applyFunc(func(server *TestServer) {
		server.serverOptions = append(server.serverOptions, options...)
	})
}
