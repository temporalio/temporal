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

package nettest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"go.temporal.io/server/internal/nettest"
)

func TestRPCFactory_GetFrontendGRPCServerOptions(t *testing.T) {
	t.Parallel()

	rpcFactory := newRPCFactory()
	opts, err := rpcFactory.GetFrontendGRPCServerOptions()
	require.NoError(t, err)
	assert.Empty(t, opts)
}

func TestRPCFactory_GetInternodeGRPCServerOptions(t *testing.T) {
	t.Parallel()

	rpcFactory := newRPCFactory()
	opts, err := rpcFactory.GetInternodeGRPCServerOptions()
	require.NoError(t, err)
	assert.Empty(t, opts)
}

func TestRPCFactory_CreateInternodeGRPCConnection(t *testing.T) {
	t.Parallel()

	testDialer(t, "test-addr", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateInternodeGRPCConnection("test-addr")
	})
}

func TestRPCFactory_CreateLocalFrontendGRPCConnection(t *testing.T) {
	t.Parallel()

	testDialer(t, ":0", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateLocalFrontendGRPCConnection()
	})
}

func TestRPCFactory_CreateRemoteFrontendGRPCConnection(t *testing.T) {
	t.Parallel()

	testDialer(t, "test-addr", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateRemoteFrontendGRPCConnection("test-addr")
	})
}

func testDialer(t *testing.T, target string, dial func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn) {
	t.Helper()

	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()

		rpcFactory := newRPCFactory()
		errs := make(chan error, 1)

		go func() {
			_, err := rpcFactory.GetGRPCListener().Accept()
			errs <- err
		}()

		conn := dial(rpcFactory)

		require.NoError(t, <-errs)
		assert.Equal(t, target, conn.Target())
		assert.NoError(t, conn.Close())
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		t.Parallel()

		// NOTE: we need to force the dial to block until the connection is established
		// or else dial won't see that the context has been cancelled
		rpcFactory := newRPCFactory(grpc.WithBlock())
		rpcFactory.SetContextFactory(func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			return ctx
		})
		assert.Panics(t, func() {
			dial(rpcFactory)
		})
	})
}

func newRPCFactory(dialOptions ...grpc.DialOption) *nettest.RPCFactory {
	return nettest.NewRPCFactory(nettest.NewListener(nettest.NewPipe()), dialOptions...)
}
