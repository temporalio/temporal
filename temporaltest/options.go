// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

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

// WithT directs all worker and client logs to the test logger.
//
// If this option is specified, then server will automatically be stopped when the
// test completes.
func WithT(t *testing.T) TestServerOption {
	return newApplyFuncContainer(func(server *TestServer) {
		server.t = t
	})
}

// WithBaseClientOptions configures options for the default clients and workers connected to the test server.
func WithBaseClientOptions(o client.Options) TestServerOption {
	return newApplyFuncContainer(func(server *TestServer) {
		server.defaultClientOptions = o
	})
}

// With WithBaseWorkerOptions configures default options for workers connected to the test server.
//
// WorkflowPanicPolicy is always set to worker.FailWorkflow so that workflow executions
// fail fast when workflow code panics or detects non-determinism.
func WithBaseWorkerOptions(o worker.Options) TestServerOption {
	o.WorkflowPanicPolicy = worker.FailWorkflow
	return newApplyFuncContainer(func(server *TestServer) {
		server.defaultWorkerOptions = o
	})
}

// WithServerOptions provides the ability to use additional options
func WithServerOptions(options ...temporal.ServerOption) TestServerOption {
	return newApplyFuncContainer(func(server *TestServer) {
		server.serverOptions = append(server.serverOptions, options...)
	})
}

type applyFuncContainer struct {
	applyInternal func(*TestServer)
}

func (fso *applyFuncContainer) apply(ts *TestServer) {
	fso.applyInternal(ts)
}

func newApplyFuncContainer(apply func(*TestServer)) *applyFuncContainer {
	return &applyFuncContainer{
		applyInternal: apply,
	}
}
