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

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination rpc_mock.go

package common

import (
	"net"
	"net/http"

	"google.golang.org/grpc"
)

// RPCFactory creates gRPC listeners and connections, and frontend HTTP clients.
type RPCFactory interface {
	GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error)
	GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error)
	GetGRPCListener() net.Listener
	CreateRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn
	CreateLocalFrontendGRPCConnection() *grpc.ClientConn
	CreateInternodeGRPCConnection(rpcAddress string) *grpc.ClientConn
	CreateLocalFrontendHTTPClient() (*FrontendHTTPClient, error)
}

type FrontendHTTPClient struct {
	http.Client
	// Address is the host:port pair of this HTTP client.
	Address string
	// Scheme is the URL scheme of this HTTP client.
	Scheme string
}

// BaseURL is the scheme and address of this HTTP client.
func (c *FrontendHTTPClient) BaseURL() string {
	return c.Scheme + "://" + c.Address
}
