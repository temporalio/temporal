package main

import (
	"log"
	"os"

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
	return map[string]string{
		"Authorization": p.token,
	}, nil
}

func main() {
	var p provider

	p.token = os.Args[1]
	if p.token == "" {
		p.token = os.Getenv("TEMPORAL_CLI_AUTHORIZATION_TOKEN")
	}

	if p.token == "" {
		log.Fatalf("authorization token not set")
	}

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
