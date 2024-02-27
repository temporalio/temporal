// The MIT License
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

package temporal_test

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite" // needed to register the sqlite plugin
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testutils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewServer verifies that NewServer doesn't cause any fx errors, and that there are no unexpected error logs after
// running for a few seconds.
func TestNewServer(t *testing.T) {
	t.Parallel()

	cfg := loadConfig(t)
	logDetector := newErrorLogDetector(t, log.NewTestLogger())
	logDetector.Start()

	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg),
		temporal.WithLogger(logDetector),
		temporal.WithChainedFrontendGrpcInterceptors(getFrontendInterceptors()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		logDetector.Stop()
		assert.NoError(t, server.Stop())
	})
	require.NoError(t, server.Start())
	time.Sleep(10 * time.Second)
}

func loadConfig(t *testing.T) *config.Config {
	cfg := loadSQLiteConfig(t)
	setTestPorts(cfg)

	return cfg
}

// loadSQLiteConfig loads the config for the sqlite persistence store. We use sqlite because it doesn't require any
// external dependencies, so it's easy to run this test in isolation.
func loadSQLiteConfig(t *testing.T) *config.Config {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.LoadConfig("development-sqlite", configDir, "")
	require.NoError(t, err)

	cfg.DynamicConfigClient.Filepath = path.Join(configDir, "dynamicconfig", "development-sql.yaml")

	return cfg
}

// setTestPorts sets the ports of all services to something different from the default ports, so that we can run the
// tests in parallel.
func setTestPorts(cfg *config.Config) {
	port := 10000

	for k, v := range cfg.Services {
		rpc := v.RPC
		rpc.GRPCPort = port
		port++
		rpc.MembershipPort = port
		port++
		v.RPC = rpc
		cfg.Services[k] = v
	}
}

func getFrontendInterceptors() func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		switch info.Server.(type) {
		case *frontend.Handler, *frontend.OperatorHandler, *frontend.AdminHandler, *frontend.WorkflowHandler:
			return handler(ctx, req)
		default:
			panic("Frontend gRPC interceptor provided to non-frontend handler")
		}
	}
}

type errorLogDetector struct {
	t      testing.TB
	on     atomic.Bool
	logger log.Logger
}

func (d *errorLogDetector) logUnexpected(operation string, msg string, tags []tag.Tag) {
	if !d.on.Load() {
		return
	}

	msg = fmt.Sprintf("unexpected %v log: %v", operation, msg)
	d.t.Error(msg)
	d.logger.Error(msg, tags...)
}

func (d *errorLogDetector) Debug(string, ...tag.Tag) {}

func (d *errorLogDetector) Info(string, ...tag.Tag) {}

func (d *errorLogDetector) DPanic(msg string, tags ...tag.Tag) {
	d.logUnexpected("DPanic", msg, tags)
}

func (d *errorLogDetector) Panic(msg string, tags ...tag.Tag) {
	d.logUnexpected("Panic", msg, tags)
}

func (d *errorLogDetector) Fatal(msg string, tags ...tag.Tag) {
	d.logUnexpected("Fatal", msg, tags)
}

func (d *errorLogDetector) Start() {
	d.on.Store(true)
}

func (d *errorLogDetector) Stop() {
	d.on.Store(false)
}

func (d *errorLogDetector) Warn(msg string, tags ...tag.Tag) {
	for _, s := range []string{
		"error creating sdk client",
		"Failed to poll for task",
	} {
		if strings.Contains(msg, s) {
			return
		}
	}

	d.logUnexpected("Warn", msg, tags)
}

func (d *errorLogDetector) Error(msg string, tags ...tag.Tag) {
	for _, s := range []string{
		"Unable to process new range",
		"Unable to call",
		"service failures",
	} {
		if strings.Contains(msg, s) {
			return
		}
	}

	d.logUnexpected("Error", msg, tags)
}

// newErrorLogDetector returns a logger that fails the test if it logs any errors or warnings, except for the ones that
// are expected. Ideally, there are no "expected" errors or warnings, but we still want this test to avoid introducing
// any new ones while we are working on removing the existing ones.
func newErrorLogDetector(t testing.TB, baseLogger log.Logger) *errorLogDetector {
	return &errorLogDetector{
		t:      t,
		logger: baseLogger,
	}
}

type fakeTest struct {
	testing.TB
	errorLogs []string
}

func (f *fakeTest) Error(args ...any) {
	require.Len(f.TB, args, 1)
	msg, ok := args[0].(string)
	require.True(f.TB, ok)
	f.errorLogs = append(f.errorLogs, msg)
}

func TestErrorLogDetector(t *testing.T) {
	t.Parallel()

	f := &fakeTest{TB: t}
	d := newErrorLogDetector(f, log.NewNoopLogger())
	d.Start()
	d.Debug("debug")
	d.Info("info")
	d.Warn("error creating sdk client")
	d.Warn("unexpected warning")
	d.Warn("Failed to poll for task")
	d.Error("Unable to process new range")
	d.Error("Unable to call matching.PollActivityTaskQueue")
	d.Error("service failures")
	d.Error("unexpected error")
	d.DPanic("dpanic")
	d.Panic("panic")
	d.Fatal("fatal")

	expectedMsgs := []string{
		"unexpected Warn log: unexpected warning",
		"unexpected Error log: unexpected error",
		"unexpected DPanic log: dpanic",
		"unexpected Panic log: panic",
		"unexpected Fatal log: fatal",
	}
	assert.Equal(t, expectedMsgs, f.errorLogs)

	d.Stop()

	f.errorLogs = nil

	d.Warn("unexpected warning")
	d.Error("unexpected error")
	d.DPanic("dpanic")
	d.Panic("panic")
	d.Fatal("fatal")
	assert.Empty(t, f.errorLogs, "should not fail the test if the detector is stopped")
}
