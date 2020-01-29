// Copyright (c) 2017 Uber Technologies, Inc.
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

package cli

import (
	"context"

	"github.com/urfave/cli"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/common"
)

const (
	cadenceClientName      = "cadence-client"
	cadenceFrontendService = "cadence-frontend"
)

// ClientFactory is used to construct rpc clients
type ClientFactory interface {
	FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient
	ServerAdminClient(c *cli.Context) adminservice.AdminServiceYARPCClient
}

type clientFactory struct {
	dispatcher *yarpc.Dispatcher
	logger     *zap.Logger
}

// NewClientFactory creates a new ClientFactory
func NewClientFactory() ClientFactory {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return &clientFactory{
		logger: logger,
	}
}

// FrontendClient builds a frontend client
func (b *clientFactory) FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient {
	hostPort := localHostPortGRPC

	if addr := c.GlobalString(FlagAddress); addr != "" {
		hostPort = addr
	}

	connection, err := grpc.Dial(hostPort, grpc.WithInsecure())
	if err != nil {
		b.logger.Fatal("Failed to create connection", zap.Error(err))
		return nil
	}

	return workflowservice.NewWorkflowServiceClient(connection)
}

// ServerAdminClient builds an admin client (based on server side thrift interface)
func (b *clientFactory) ServerAdminClient(c *cli.Context) adminservice.AdminServiceYARPCClient {
	b.ensureDispatcher(c)
	return adminservice.NewAdminServiceYARPCClient(b.dispatcher.ClientConfig(cadenceFrontendService))
}

func (b *clientFactory) ensureDispatcher(c *cli.Context) {
	if b.dispatcher != nil {
		return
	}

	hostPort := localHostPort
	if addr := c.GlobalString(FlagAddress); addr != "" {
		hostPort = addr
	}

	ch, err := tchannel.NewChannelTransport(tchannel.ServiceName(cadenceClientName), tchannel.ListenAddr("127.0.0.1:0"))
	if err != nil {
		b.logger.Fatal("Failed to create transport channel", zap.Error(err))
	}
	unaryOutbound := ch.NewSingleOutbound(hostPort)

	b.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name: cadenceClientName,
		Outbounds: yarpc.Outbounds{
			cadenceFrontendService: {Unary: unaryOutbound},
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary: &versionMiddleware{},
		},
	})

	if err := b.dispatcher.Start(); err != nil {
		b.dispatcher.Stop()
		b.logger.Fatal("Failed to create outbound transport channel: %v", zap.Error(err))
	}
}

type versionMiddleware struct {
}

func (vm *versionMiddleware) Call(ctx context.Context, request *transport.Request, out transport.UnaryOutbound) (*transport.Response, error) {
	request.Headers = request.Headers.With(common.LibraryVersionHeaderName, "1.0.0").With(common.FeatureVersionHeaderName, "1.0.0").With(common.ClientImplHeaderName, "cli")
	return out.Call(ctx, request)
}
