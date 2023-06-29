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

package temporal

import (
	"path"
	"strings"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite" // needed to register the sqlite plugin
	"go.temporal.io/server/tests/testutils"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewServer verifies that NewServer doesn't cause any fx errors, and that there are no unexpected error logs after
// running for a few seconds.
func TestNewServer(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	cfg := loadConfig(t)
	logger := newErrorLogDetector(t, ctrl)

	server, err := NewServer(
		WithConfig(cfg),
		WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
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

// newErrorLogDetector returns a logger that fails the test if it logs any errors or warnings, except for the ones that
// are expected. Ideally, there are no "expected" errors or warnings, but we still want this test to avoid introducing
// any new ones while we are working on removing the existing ones.
func newErrorLogDetector(t *testing.T, ctrl *gomock.Controller) *log.MockLogger {
	logger := log.NewMockLogger(ctrl)
	logger.EXPECT().Debug(gomock.Any(), gomock.Any()).AnyTimes()
	logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
	logger.EXPECT().Warn(gomock.Any(), gomock.Any()).AnyTimes().Do(func(msg string, tags ...tag.Tag) {
		if strings.Contains(msg, "error creating sdk client") {
			return
		}

		if strings.Contains(msg, "poll for task") {
			return
		}

		if strings.Contains(msg, "error in prometheus") {
			return
		}

		t.Errorf("unexpected warning: %v", msg)
	})
	logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes().Do(func(msg string, tags ...tag.Tag) {
		if strings.Contains(msg, "looking up host for shardID") {
			return
		}

		t.Errorf("unexpected error: %v", msg)
	})

	return logger
}
