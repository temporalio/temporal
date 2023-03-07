// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporaltest

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/schema/sqlite"
	"go.temporal.io/server/temporal"
)

// A TestServer is a Temporal server listening on a system-chosen port on the
// local loopback interface, for use in end-to-end tests.
type TestServer struct {
	server               temporal.Server
	frontendHostPort     string
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
	w := worker.New(ts.DefaultClient(), taskQueue, ts.defaultWorkerOptions)
	registerFunc(w)
	ts.workers = append(ts.workers, w)

	if err := w.Start(); err != nil {
		ts.fatal(err)
	}

	return w
}

// NewWorkerWithOptions returns a Temporal worker on the specified task queue.
//
// WorkflowPanicPolicy is always set to worker.FailWorkflow so that workflow executions
// fail fast when workflow code panics or detects non-determinism.
func (ts *TestServer) NewWorkerWithOptions(taskQueue string, registerFunc func(registry worker.Registry), opts worker.Options) worker.Worker {
	opts.WorkflowPanicPolicy = worker.FailWorkflow

	w := worker.New(ts.DefaultClient(), taskQueue, opts)
	registerFunc(w)
	ts.workers = append(ts.workers, w)

	if err := w.Start(); err != nil {
		ts.fatal(err)
	}

	return w
}

// DefaultClient returns the default Temporal client configured for making requests to the server.
//
// It is configured to use a pre-registered test namespace and will be closed on TestServer.Stop.
func (ts *TestServer) DefaultClient() client.Client {
	if ts.defaultClient == nil {
		ts.defaultClient = ts.NewClientWithOptions(ts.defaultClientOptions)
	}
	return ts.defaultClient
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

	opts.HostPort = ts.frontendHostPort
	c, err := client.Dial(opts)
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
	ts.server.Stop()
}

// NewServer starts and returns a new TestServer.
//
// If not specifying the WithT option, the caller should execute Stop when finished to close
// the server and release resources.
func NewServer(opts ...TestServerOption) *TestServer {
	rand.Seed(time.Now().UnixNano())
	testNamespace := fmt.Sprintf("temporaltest-%d", rand.Intn(999999))

	ts := TestServer{
		defaultTestNamespace: testNamespace,
	}

	// Apply options
	for _, opt := range opts {
		opt.apply(&ts)
	}

	if ts.t != nil {
		ts.t.Cleanup(func() {
			ts.Stop()
		})
	}

	cfg, err := newDefaultConfig()
	if err != nil {
		ts.fatal(fmt.Errorf("unable to create config: %w", err))
	}

	dynCfg := newDefaultDynamicConfig()

	ts.frontendHostPort = cfg.PublicClient.HostPort
	ts.serverOptions = append(ts.serverOptions,
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg),
		temporal.WithLogger(log.NewNoopLogger()),
		temporal.WithDynamicConfigClient(dynCfg),
	)

	// Pre-create namespaces
	sqlConfig := cfg.Persistence.DataStores["sqlite-default"].SQL
	namespaces := []*sqlite.NamespaceConfig{
		sqlite.NewNamespaceConfig(cfg.ClusterMetadata.CurrentClusterName, ts.defaultTestNamespace, false),
	}
	if err := sqlite.CreateNamespaces(sqlConfig, namespaces...); err != nil {
		ts.fatal(fmt.Errorf("unable to create default namespace: %w", err))
	}

	s, err := temporal.NewServer(ts.serverOptions...)
	if err != nil {
		ts.fatal(fmt.Errorf("unable to create server: %w", err))
	}
	ts.server = s

	go func() {
		if err := s.Start(); err != nil {
			ts.fatal(fmt.Errorf("unable to start server: %w", err))
		}
	}()

	return &ts
}
