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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/peer"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc"
)

// HelloServer is used to implement helloworld.GreeterServer.
type HelloServer struct {
	helloworld.UnimplementedGreeterServer
}

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

// SayHello implements helloworld.GreeterServer
func (s *HelloServer) SayHello(ctx context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	return &helloworld.HelloReply{Message: "Hello " + in.Name}, nil
}

var (
	rpcTestCfgDefault = &config.RPC{
		GRPCPort:       0,
		MembershipPort: 7600,
		BindOnIP:       localhostIPv4,
	}
	serverCfgInsecure = &config.Global{
		Membership: config.Membership{
			MaxJoinDuration:  5,
			BroadcastAddress: localhostIPv4,
		},
	}
	clusterMetadata = &cluster.Config{
		CurrentClusterName: "test",
		ClusterInformation: map[string]cluster.ClusterInformation{"test": {RPCAddress: localhostIPv4 + ":1234"}},
	}
)

func startHelloWorldServer(s *suite.Suite, factory *TestFactory) (*grpc.Server, string) {
	var opts []grpc.ServerOption
	var err error
	if factory.serverUsage == Internode {
		opts, err = factory.GetInternodeGRPCServerOptions()
	} else {
		opts, err = factory.GetFrontendGRPCServerOptions()
	}
	s.NoError(err)

	server := grpc.NewServer(opts...)
	greeter := &HelloServer{}
	helloworld.RegisterGreeterServer(server, greeter)

	listener := factory.GetGRPCListener()

	port := strings.Split(listener.Addr().String(), ":")[1]
	s.NoError(err)
	go func() {
		err = server.Serve(listener)
	}()
	s.NoError(err)
	return server, port
}

func runHelloWorldTest(s *suite.Suite, host string, serverFactory *TestFactory, clientFactory *TestFactory, isValid bool) {
	server, port := startHelloWorldServer(s, serverFactory)
	defer server.Stop()
	err := dialHello(s, host+":"+port, clientFactory, serverFactory.serverUsage)

	if isValid {
		s.NoError(err)
	} else {
		s.Error(err)
	}
}

func runHelloWorldMultipleDials(
	s *suite.Suite,
	host string,
	serverFactory *TestFactory,
	clientFactory *TestFactory,
	nDials int,
	validator func(*credentials.TLSInfo, error),
) {

	server, port := startHelloWorldServer(s, serverFactory)
	defer server.Stop()

	for i := 0; i < nDials; i++ {
		tlsInfo, err := dialHelloAndGetTLSInfo(s, host+":"+port, clientFactory, serverFactory.serverUsage)
		validator(tlsInfo, err)
	}
}

func dialHello(s *suite.Suite, hostport string, clientFactory *TestFactory, serverType ServerUsageType) error {
	_, err := dialHelloAndGetTLSInfo(s, hostport, clientFactory, serverType)
	return err
}

func dialHelloAndGetTLSInfo(
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

	client := helloworld.NewGreeterClient(clientConn)

	request := &helloworld.HelloRequest{Name: convert.Uint64ToString(rand.Uint64())}
	var reply *helloworld.HelloReply
	peer := new(peer.Peer)
	reply, err = client.SayHello(context.Background(), request, grpc.Peer(peer))
	tlsInfo, _ := peer.AuthInfo.(credentials.TLSInfo)

	if err == nil {
		s.NotNil(reply)
		s.True(strings.Contains(reply.Message, request.Name))
	}

	_ = clientConn.Close()
	return &tlsInfo, err
}
