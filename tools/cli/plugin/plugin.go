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
	"fmt"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

const (
	DataConverterPluginType   = "DataConverter"
	HeadersProviderPluginType = "HeadersProvider"
)

var (
	PluginHandshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "TEMPORAL_CLI_PLUGIN",
		MagicCookieValue: "abb3e448baf947eba1847b10a38554db",
	}

	pluginMap = map[string]plugin.Plugin{
		DataConverterPluginType:   &DataConverterPlugin{},
		HeadersProviderPluginType: &HeadersProviderPlugin{},
	}
)

func newPluginClient(kind string, name string) (interface{}, error) {
	pluginClient := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: PluginHandshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(name),
		Managed:         true,
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:  "tctl",
			Level: hclog.LevelFromString("INFO"),
		}),
	})

	rpcClient, err := pluginClient.Client()
	if err != nil {
		return nil, fmt.Errorf("unable to create plugin client: %w", err)
	}

	return rpcClient.Dispense(kind)
}

func StopPlugins() {
	plugin.CleanupClients()
}
