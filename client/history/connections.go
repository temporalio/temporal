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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination connections_mock.go

package history

import (
	"sync"

	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/membership"
)

type (
	clientConnection struct {
		historyClient historyservice.HistoryServiceClient
		grpcConn      *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl struct {
		mu struct {
			sync.RWMutex
			conns map[rpcAddress]clientConnection
		}

		historyServiceResolver membership.ServiceResolver
		rpcFactory             RPCFactory
	}

	// RPCFactory is a subset of the [go.temporal.io/server/common/rpc.RPCFactory] interface to make testing easier.
	RPCFactory interface {
		CreateInternodeGRPCConnection(rpcAddress string) *grpc.ClientConn
	}

	connectionPool interface {
		getOrCreateClientConn(addr rpcAddress) clientConnection
		getAllClientConns() []clientConnection
		resetConnectBackoff(clientConnection)
	}
)

func newConnectionPool(
	historyServiceResolver membership.ServiceResolver,
	rpcFactory RPCFactory,
) *connectionPoolImpl {
	c := &connectionPoolImpl{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection)
	return c
}

func (c *connectionPoolImpl) getOrCreateClientConn(addr rpcAddress) clientConnection {
	c.mu.RLock()
	cc, ok := c.mu.conns[addr]
	c.mu.RUnlock()
	if ok {
		return cc
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok = c.mu.conns[addr]; ok {
		return cc
	}

	grpcConn := c.rpcFactory.CreateInternodeGRPCConnection(string(addr))
	cc = clientConnection{
		historyClient: historyservice.NewHistoryServiceClient(grpcConn),
		grpcConn:      grpcConn,
	}

	c.mu.conns[addr] = cc
	return cc
}

func (c *connectionPoolImpl) getAllClientConns() []clientConnection {
	hostInfos := c.historyServiceResolver.Members()

	var clientConns []clientConnection
	for _, hostInfo := range hostInfos {
		cc := c.getOrCreateClientConn(rpcAddress(hostInfo.GetAddress()))
		clientConns = append(clientConns, cc)
	}

	return clientConns
}

func (c *connectionPoolImpl) resetConnectBackoff(cc clientConnection) {
	cc.grpcConn.ResetConnectBackoff()
}
