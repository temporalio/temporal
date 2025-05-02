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
