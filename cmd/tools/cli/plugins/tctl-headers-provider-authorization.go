package main

import (
	"flag"
	"fmt"

	"github.com/hashicorp/go-plugin"
	"go.temporal.io/server/tools/cli"
)

var handshakeConfig = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "TEMPORAL_TCTL_PLUGIN",
	MagicCookieValue: "abb3e448baf947eba1847b10a38554db",
}

type provider struct {
	token string
}

func (p provider) GetHeaders(outgoingHeaders map[string][]string) (map[string]string, error) {
	if p.token == "" {
		return nil, fmt.Errorf("token is not set")
	}

	return map[string]string{
		"Authorization": p.token,
	}, nil
}

func main() {
	var p provider

	flag.StringVar(&p.token, "token", "", "token to set")
	flag.Parse()

	var pluginMap = map[string]plugin.Plugin{
		"HeadersProvider": &cli.HeadersProviderPlugin{
			Impl: &p,
		},
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}
