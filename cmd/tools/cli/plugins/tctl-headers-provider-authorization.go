package main

import (
	"github.com/hashicorp/go-plugin"
	"go.temporal.io/server/tools/cli"
)

var handshakeConfig = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "TEMPORAL_TCTL_PLUGIN",
	MagicCookieValue: "abb3e448baf947eba1847b10a38554db",
}

type provider struct{}

func (provider) GetHeaders(outgoingHeaders map[string][]string) (map[string]string, error) {
	return map[string]string{
		"Test": "testing",
	}, nil
}

func main() {
	var pluginMap = map[string]plugin.Plugin{
		"HeadersProvider": &cli.HeadersProviderPlugin{
			Impl: &provider{},
		},
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}
