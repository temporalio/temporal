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

package rpc_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
)

func TestStartListener_InvalidPort(t *testing.T) {
	t.Parallel()

	_, err := rpc.StartServiceListener(&config.RPC{
		GRPCPort: -1,
	}, log.NewTestLogger(), primitives.FrontendService)
	assert.ErrorIs(t, err, rpc.ErrStartListener)
}

func TestStartListener_Ok(t *testing.T) {
	t.Parallel()

	listener, err := rpc.StartServiceListener(&config.RPC{
		GRPCPort: 0,
	}, log.NewTestLogger(), primitives.FrontendService)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
	})
	assert.NotZero(t, listener.Addr().(*net.TCPAddr).Port)
}
