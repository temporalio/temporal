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

package main

import (
	"context"
	"os"

	"github.com/hashicorp/go-plugin"
	cliplugin "go.temporal.io/server/tools/cli/plugin"
)

type provider struct {
	token string
}

func (p provider) GetHeaders(ctx context.Context) (map[string]string, error) {
	return map[string]string{
		"Authorization": p.token,
	}, nil
}

func main() {
	var p provider

	if len(os.Args) > 1 {
		p.token = os.Args[1]
	}
	if p.token == "" {
		p.token = os.Getenv("TEMPORAL_CLI_AUTHORIZATION_TOKEN")
	}

	var pluginMap = map[string]plugin.Plugin{
		cliplugin.HeadersProviderPluginType: &cliplugin.HeadersProviderPlugin{
			Impl: &p,
		},
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: cliplugin.PluginHandshakeConfig,
		Plugins:         pluginMap,
	})
}
