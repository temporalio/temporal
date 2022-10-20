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

package host

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests"
)

// TestNewServer verifies that NewServer doesn't cause any fx errors
func TestNewServer(t *testing.T) {
	dir, err := tests.GetTemporalPackageDir()
	require.NoError(t, err)
	configDir := path.Join(dir, "config")
	var cfg config.Config
	err = config.Load("", configDir, "cass", &cfg)
	require.NoError(t, err)
	cfg.DynamicConfigClient.Filepath = path.Join(configDir, "dynamicconfig", "development-cass.yaml")
	_, err = temporal.NewServer(
		temporal.ForServices(temporal.Services),
		temporal.WithConfig(&cfg),
	)
	assert.NoError(t, err)
	// TODO: add tests for Server.Run(), etc.
}
