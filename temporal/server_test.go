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
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite" // needed to register the sqlite plugin
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
	logDetector := newErrorLogDetector(t)
	logDetector.Start()

	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg),
		temporal.WithLogger(logDetector),
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

type errorLogDetector struct {
	t  testing.TB
	on atomic.Bool
	log.Logger
}

func (d *errorLogDetector) Start() {
	d.on.Store(true)
}

func (d *errorLogDetector) Stop() {
	d.on.Store(false)
}

func (d *errorLogDetector) Warn(msg string, tags ...tag.Tag) {
	d.Logger.Warn(msg, tags...)

	if !d.on.Load() {
		return
	}

	if strings.Contains(msg, "error creating sdk client") {
		return
	}

	d.t.Errorf("unexpected warning log: %s", msg)
}

func (d *errorLogDetector) Error(msg string, tags ...tag.Tag) {
	d.Logger.Error(msg, tags...)

	if !d.on.Load() {
		return
	}

	if strings.Contains(msg, "Unable to process new range") {
		return
	}

	d.t.Errorf("unexpected error log: %s", msg)
}

// newErrorLogDetector returns a logger that fails the test if it logs any errors or warnings, except for the ones that
// are expected. Ideally, there are no "expected" errors or warnings, but we still want this test to avoid introducing
// any new ones while we are working on removing the existing ones.
func newErrorLogDetector(t testing.TB) *errorLogDetector {
	return &errorLogDetector{
		t:      t,
		Logger: log.NewCLILogger(),
	}
}

type fakeTest struct {
	testing.TB
	errorfMsgs []string
}

func (f *fakeTest) Errorf(msg string, args ...any) {
	f.errorfMsgs = append(f.errorfMsgs, fmt.Sprintf(msg, args...))
}

func TestErrorLogDetector(t *testing.T) {
	t.Parallel()

	f := &fakeTest{TB: t}
	d := newErrorLogDetector(f)
	d.Start()
	d.Warn("error creating sdk client")
	d.Error("Unable to process new range")
	d.Error("unexpected error")
	d.Warn("unexpected warning")

	assert.Equal(t, []string{
		"unexpected error log: unexpected error",
		"unexpected warning log: unexpected warning",
	}, f.errorfMsgs, "should fail the test if there are any unexpected errors or warnings")

	d.Stop()

	f.errorfMsgs = nil

	d.Error("unexpected error")
	d.Warn("unexpected warning")
	assert.Empty(t, f.errorfMsgs, "should not fail the test if the detector is stopped")
}
