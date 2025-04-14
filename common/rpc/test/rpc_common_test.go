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

package rpc

import (
	"context"
	"crypto/tls"
	"math/rand"
	"net"
	"strings"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/testservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var (
	_ testservice.TestServiceServer = (*TestServiceServerHandler)(nil)
)

type (
	// TestServiceServerHandler - gRPC handler interface for test service
	TestServiceServerHandler struct {
		testservice.UnimplementedTestServiceServer
	}
)

type ServerUsageType int32

const (
	Frontend ServerUsageType = iota
	Internode
	RemoteCluster
)

const (
	localhostIPv4 = "127.0.0.1"
	localhost     = "localhost"
)

type TestFactory struct {
	*rpc.RPCFactory
	serverUsage ServerUsageType
}

// SendHello implements testservice.TestServiceServer
func (s *TestServiceServerHandler) SendHello(ctx context.Context, in *testservice.SendHelloRequest) (*testservice.SendHelloResponse, error) {
	return &testservice.SendHelloResponse{Message: "Hello " + in.Name}, nil
}

var (
	rpcTestCfgDefault = &config.RPC{
		GRPCPort:       0,
		MembershipPort: 7600,
		BindOnIP:       localhostIPv4,
	}
	cfg = &config.Config{
		Services: map[string]config.Service{
			"frontend": {
				RPC: *rpcTestCfgDefault,
			},
		},
	}
	serverCfgInsecure = &config.Global{
		Membership: config.Membership{
			MaxJoinDuration:  5,
			BroadcastAddress: localhostIPv4,
		},
	}
)

func startTestServiceServer(s *suite.Suite, factory *TestFactory) (*grpc.Server, string) {
	var opts []grpc.ServerOption
	var err error
	if factory.serverUsage == Internode {
		opts, err = factory.GetInternodeGRPCServerOptions()
	} else {
		opts, err = factory.GetFrontendGRPCServerOptions()
	}
	s.NoError(err)

	server := grpc.NewServer(opts...)
	testServiceHandler := &TestServiceServerHandler{}
	testservice.RegisterTestServiceServer(server, testServiceHandler)

	listener := factory.GetGRPCListener()

	port := strings.Split(listener.Addr().String(), ":")[1]
	s.NoError(err)
	go func() {
		err = server.Serve(listener)
	}()
	s.NoError(err)
	return server, port
}

func runTestServerTest(s *suite.Suite, host string, serverFactory *TestFactory, clientFactory *TestFactory, isValid bool) {
	server, port := startTestServiceServer(s, serverFactory)
	defer server.Stop()
	err := dialHello(s, host+":"+port, clientFactory, serverFactory.serverUsage)

	if isValid {
		s.NoError(err)
	} else {
		s.Error(err)
	}
}

func runTestServerMultipleDials(
	s *suite.Suite,
	host string,
	serverFactory *TestFactory,
	clientFactory *TestFactory,
	nDials int,
	validator func(*credentials.TLSInfo, error),
) {

	server, port := startTestServiceServer(s, serverFactory)
	defer server.Stop()

	for i := 0; i < nDials; i++ {
		tlsInfo, err := dialTestServiceAndGetTLSInfo(s, host+":"+port, clientFactory, serverFactory.serverUsage)
		validator(tlsInfo, err)
	}
}

func dialHello(s *suite.Suite, hostport string, clientFactory *TestFactory, serverType ServerUsageType) error {
	_, err := dialTestServiceAndGetTLSInfo(s, hostport, clientFactory, serverType)
	return err
}

func dialTestServiceAndGetTLSInfo(
	s *suite.Suite,
	hostport string,
	clientFactory *TestFactory,
	serverType ServerUsageType,
) (*credentials.TLSInfo, error) {

	logger := log.NewNoopLogger()
	var cfg *tls.Config
	var err error
	switch serverType {
	case Internode:
		cfg, err = clientFactory.GetInternodeClientTlsConfig()
		s.NoError(err)
	case Frontend:
		cfg, err = clientFactory.GetFrontendClientTlsConfig()
		s.NoError(err)
	case RemoteCluster:
		host, _, err := net.SplitHostPort(hostport)
		s.NoError(err)
		cfg, err = clientFactory.GetRemoteClusterClientConfig(host)
		s.NoError(err)
	}

	clientConn, err := rpc.Dial(hostport, cfg, logger)
	s.NoError(err)

	client := testservice.NewTestServiceClient(clientConn)

	request := &testservice.SendHelloRequest{Name: convert.Uint64ToString(rand.Uint64())}
	var reply *testservice.SendHelloResponse
	peer := new(peer.Peer)
	reply, err = client.SendHello(context.Background(), request, grpc.Peer(peer))
	tlsInfo, _ := peer.AuthInfo.(credentials.TLSInfo)

	if err == nil {
		s.NotNil(reply)
		s.True(strings.Contains(reply.Message, request.Name))
	}

	_ = clientConn.Close()
	return &tlsInfo, err
}
