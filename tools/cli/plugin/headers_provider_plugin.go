// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

package plugin

import (
	"context"
	"fmt"
	"net/rpc"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/metadata"
)

type HeadersProvider interface {
	GetHeaders(context.Context) (map[string]string, error)
}

type HeadersProviderInternal interface {
	GetHeaders(map[string][]string) (map[string]string, error)
}

type HeadersProviderRPCServer struct {
	Impl HeadersProviderInternal
}

type HeadersProviderPlugin struct {
	Impl HeadersProviderInternal
}

type HeadersProviderRPC struct {
	client *rpc.Client
}

type HeadersProviderPluginWrapper struct {
	client   *plugin.Client
	provider HeadersProviderInternal
}

func (w HeadersProviderPluginWrapper) GetHeaders(ctx context.Context) (map[string]string, error) {
	var outgoingHeaders map[string][]string
	if headers, ok := metadata.FromIncomingContext(ctx); ok {
		outgoingHeaders = headers
	}

	return w.provider.GetHeaders(outgoingHeaders)
}

func NewHeadersProviderPlugin(name string) (HeadersProvider, error) {
	pluginClient := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: PluginHandshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(name),
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:  "tctl",
			Level: hclog.LevelFromString("INFO"),
		}),
	})

	rpcClient, err := pluginClient.Client()
	if err != nil {
		return nil, fmt.Errorf("error creating plugin client: %w", err)
	}

	raw, err := rpcClient.Dispense("HeadersProvider")
	if err != nil {
		return nil, fmt.Errorf("error registering plugin: %w", err)
	}

	headersProvider, ok := raw.(HeadersProviderInternal)
	if !ok {
		return nil, fmt.Errorf("incorrect plugin type")
	}

	return HeadersProviderPluginWrapper{
		client:   pluginClient,
		provider: headersProvider,
	}, nil
}

func (g *HeadersProviderRPC) GetHeaders(outgoingHeaders map[string][]string) (map[string]string, error) {
	var result map[string]string

	err := g.client.Call("Plugin.GetHeaders", outgoingHeaders, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *HeadersProviderRPCServer) GetHeaders(outgoingHeaders map[string][]string, resp *map[string]string) error {
	var err error
	*resp, err = s.Impl.GetHeaders(outgoingHeaders)
	return err
}

func (p *HeadersProviderPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &HeadersProviderRPCServer{Impl: p.Impl}, nil
}

func (HeadersProviderPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &HeadersProviderRPC{client: c}, nil
}
