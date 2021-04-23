package cli

import (
	"context"
	"fmt"
	"net/rpc"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/metadata"
)

var (
	pluginClient *plugin.Client
	pluginMap    = map[string]plugin.Plugin{
		"HeadersProvider": &HeadersProviderPlugin{},
	}
	handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "TEMPORAL_TCTL_PLUGIN",
		MagicCookieValue: "abb3e448baf947eba1847b10a38554db",
	}
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

func (w HeadersProviderPluginWrapper) Kill() {
	w.client.Kill()
}

func NewHeadersProviderPlugin(name string) (HeadersProvider, error) {
	pluginClient := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(name),
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:  "tctl",
			Level: hclog.LevelFromString("DEBUG"),
		}),
	})

	rpcClient, err := pluginClient.Client()
	if err != nil {
		return nil, fmt.Errorf("error creating plugin client: %v\n", err)
	}

	raw, err := rpcClient.Dispense("HeadersProvider")
	if err != nil {
		return nil, fmt.Errorf("error registering plugin: %v\n", err)
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
