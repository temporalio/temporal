package cli

import (
	"context"
	"fmt"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
)

type HeadersProviderRPC struct {
	client *rpc.Client
}

func (g *HeadersProviderRPC) GetHeaders(ctx context.Context) (map[string]string, error) {
	var result map[string]string

	err := g.client.Call("Plugin.GetHeaders", ctx, &result)
	if err != nil {
		fmt.Printf("%v\n", err)
		return nil, err
	}

	return result, nil
}

type HeadersProvider interface {
	GetHeaders(ctx context.Context) (map[string]string, error)
}

type HeadersProviderRPCServer struct {
	Impl HeadersProvider
}

func (s *HeadersProviderRPCServer) GetHeaders(ctx context.Context, result *map[string]string) error {
	headers, err := s.Impl.GetHeaders(ctx)
	if err != nil {
		return err
	}

	result = &headers

	return nil
}

type HeadersProviderPlugin struct {
	Impl HeadersProvider
}

func (p *HeadersProviderPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &HeadersProviderRPCServer{Impl: p.Impl}, nil
}

func (HeadersProviderPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &HeadersProviderRPC{client: c}, nil
}
