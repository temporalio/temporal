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
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/rpc"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/examples/helloworld/helloworld"

	"go.temporal.io/server/common/service/config"
)

// HelloServer is used to implement helloworld.GreeterServer.
type HelloServer struct{}

type ServerUsageType int32

const (
	Frontend ServerUsageType = iota
	Internode
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
		BindOnIP:       "127.0.0.1",
	}
	serverCfgInsecure = &config.Global{
		Membership: config.Membership{
			MaxJoinDuration:  5,
			BroadcastAddress: "127.0.0.1",
		},
	}
)

func startHelloWorldServer(s suite.Suite, factory *TestFactory) (*grpc.Server, string) {
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
		err := server.Serve(listener)
		s.NoError(err)
	}()
	return server, port
}

func runHelloWorldTest(s suite.Suite, host string, serverFactory *TestFactory, clientFactory *TestFactory, isValid bool) {
	server, port := startHelloWorldServer(s, serverFactory)
	defer server.Stop()
	err := dialHello(s, host+":"+port, clientFactory, serverFactory.serverUsage)

	if isValid {
		s.NoError(err)
	} else {
		s.Error(err)
	}
}

func runHelloWorldMultipleDials(s suite.Suite, host string, serverFactory *TestFactory, clientFactory *TestFactory, nDials int) {
	server, port := startHelloWorldServer(s, serverFactory)
	defer server.Stop()

	for i := 0; i < nDials; i++ {

		tlsInfo, err := dialHelloAndGetTLSInfo(s, host+":"+port, clientFactory, serverFactory.serverUsage)

		if i != 2 {
			s.NoError(err)
			s.NotNil(tlsInfo)
			s.NotNil(tlsInfo.State.PeerCertificates)
			sn := (*tlsInfo.State.PeerCertificates[0].SerialNumber).Int64()
			s.True(sn == int64((i+1)%2+100))
		} else {
			// on third iteration the test cert provider returns a wrong CA cert
			// this is how we test that CA certs are served dynamically
			s.Error(err)
		}
	}
}

func dialHello(s suite.Suite, hostport string, clientFactory *TestFactory, serverType ServerUsageType) error {
	_, err := dialHelloAndGetTLSInfo(s, hostport, clientFactory, serverType)
	return err
}

func dialHelloAndGetTLSInfo(
	s suite.Suite,
	hostport string,
	clientFactory *TestFactory,
	serverType ServerUsageType) (*credentials.TLSInfo, error) {

	var cfg *tls.Config
	var err error
	if serverType == Internode {
		cfg, err = clientFactory.GetInternodeClientTlsConfig()
	} else {
		cfg, err = clientFactory.GetFrontendClientTlsConfig()
	}

	s.NoError(err)
	clientConn, err := rpc.Dial(hostport, cfg)
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
