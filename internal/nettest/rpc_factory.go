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

package nettest

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.temporal.io/server/common"
)

// RPCFactory is a common.RPCFactory implementation that uses a PipeListener to create connections. It is useful for
// testing gRPC servers.
type RPCFactory struct {
	listener       *PipeListener
	contextFactory func() context.Context
	dialOptions    []grpc.DialOption
}

var _ common.RPCFactory = (*RPCFactory)(nil)

// NewRPCFactory creates a new RPCFactory backed by a PipeListener.
func NewRPCFactory(listener *PipeListener, dialOptions ...grpc.DialOption) *RPCFactory {
	return &RPCFactory{
		listener: listener,
		contextFactory: func() context.Context {
			return context.Background()
		},
		dialOptions: dialOptions,
	}
}

// SetContextFactory sets the context factory used to create contexts for dialing connections. The default returns a
// background context on each call.
func (f *RPCFactory) SetContextFactory(factory func() context.Context) {
	f.contextFactory = factory
}

func (f *RPCFactory) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (f *RPCFactory) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (f *RPCFactory) GetGRPCListener() net.Listener {
	return f.listener
}

func (f *RPCFactory) CreateRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return f.dial(rpcAddress)
}

func (f *RPCFactory) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	return f.dial(f.listener.Addr().String())
}

func (f *RPCFactory) CreateLocalFrontendHTTPClient() (*common.FrontendHTTPClient, error) {
	panic("unimplemented in the nettest package")
}

func (f *RPCFactory) CreateInternodeGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return f.dial(rpcAddress)
}

func (f *RPCFactory) dial(rpcAddress string) *grpc.ClientConn {
	dialOptions := append(f.dialOptions,
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return f.listener.Connect(ctx.Done())
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	conn, err := grpc.DialContext(
		f.contextFactory(),
		rpcAddress,
		dialOptions...,
	)
	if err != nil {
		panic(err)
	}

	return conn
}
