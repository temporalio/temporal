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
	"errors"

	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/urfave/cli"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
)

const (
	_cadenceClientName      = "cadence-client"
	_cadenceFrontendService = "cadence-frontend"
)

// WorkflowClientBuilderInterface is an interface to build client to cadence service.
// User can customize builder by implementing this interface, and call SetBulder after initialize cli.
// (See cadence/cmd/tools/cli/main.go for example)
// The customized builder may have more processing on Env, Address and other info.
type WorkflowClientBuilderInterface interface {
	BuildServiceClient(c *cli.Context) (workflowserviceclient.Interface, error)
	BuildAdminServiceClient(c *cli.Context) (adminserviceclient.Interface, error)
}

// WorkflowClientBuilder build client to cadence service
type WorkflowClientBuilder struct {
	hostPort   string
	dispatcher *yarpc.Dispatcher
	logger     *zap.Logger
}

// NewBuilder creates a new WorkflowClientBuilder
func NewBuilder() *WorkflowClientBuilder {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return &WorkflowClientBuilder{
		logger: logger,
	}
}

// BuildServiceClient builds a rpc service client to cadence service
func (b *WorkflowClientBuilder) BuildServiceClient(c *cli.Context) (workflowserviceclient.Interface, error) {
	b.hostPort = localHostPort
	if addr := c.GlobalString(FlagAddress); addr != "" {
		b.hostPort = addr
	}

	if err := b.build(); err != nil {
		return nil, err
	}

	if b.dispatcher == nil {
		b.logger.Fatal("No RPC dispatcher provided to create a connection to Cadence Service")
	}

	return workflowserviceclient.New(b.dispatcher.ClientConfig(_cadenceFrontendService)), nil
}

// BuildAdminServiceClient builds a rpc service client to cadence admin service
func (b *WorkflowClientBuilder) BuildAdminServiceClient(c *cli.Context) (adminserviceclient.Interface, error) {
	b.hostPort = localHostPort
	if addr := c.GlobalString(FlagAddress); addr != "" {
		b.hostPort = addr
	}

	if err := b.build(); err != nil {
		return nil, err
	}

	if b.dispatcher == nil {
		b.logger.Fatal("No RPC dispatcher provided to create a connection to Cadence Service")
	}

	return adminserviceclient.New(b.dispatcher.ClientConfig(_cadenceFrontendService)), nil
}

func (b *WorkflowClientBuilder) build() error {
	if b.dispatcher != nil {
		return nil
	}

	if len(b.hostPort) == 0 {
		return errors.New("HostPort is empty")
	}

	ch, err := tchannel.NewChannelTransport(
		tchannel.ServiceName(_cadenceClientName), tchannel.ListenAddr("127.0.0.1:0"))
	if err != nil {
		b.logger.Fatal("Failed to create transport channel", zap.Error(err))
	}

	b.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name: _cadenceClientName,
		Outbounds: yarpc.Outbounds{
			_cadenceFrontendService: {Unary: ch.NewSingleOutbound(b.hostPort)},
		},
	})

	if b.dispatcher != nil {
		if err := b.dispatcher.Start(); err != nil {
			b.logger.Fatal("Failed to create outbound transport channel: %v", zap.Error(err))
		}
	}

	return nil
}
