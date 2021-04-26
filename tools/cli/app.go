// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"fmt"

	"github.com/urfave/cli"

	"go.temporal.io/server/common/headers"
)

// SetFactory is used to set the ClientFactory global
func SetFactory(factory ClientFactory) {
	cFactory = factory
}

// NewCliApp instantiates a new instance of the CLI application.
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "tctl"
	app.Usage = "A command-line tool for Temporal users"
	app.Version = headers.CLIVersion
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   FlagAddressWithAlias,
			Value:  "",
			Usage:  "host:port for Temporal frontend service",
			EnvVar: "TEMPORAL_CLI_ADDRESS",
		},
		cli.StringFlag{
			Name:   FlagNamespaceWithAlias,
			Value:  "default",
			Usage:  "Temporal workflow namespace",
			EnvVar: "TEMPORAL_CLI_NAMESPACE",
		},
		cli.IntFlag{
			Name:   FlagContextTimeoutWithAlias,
			Value:  defaultContextTimeoutInSeconds,
			Usage:  "optional timeout for context of RPC call in seconds",
			EnvVar: "TEMPORAL_CONTEXT_TIMEOUT",
		},
		cli.BoolFlag{
			Name:   FlagAutoConfirm,
			Usage:  "automatically confirm all prompts",
			Hidden: true,
		},
		cli.StringFlag{
			Name:   FlagTLSCertPath,
			Value:  "",
			Usage:  "path to x509 certificate",
			EnvVar: "TEMPORAL_CLI_TLS_CERT",
		},
		cli.StringFlag{
			Name:   FlagTLSKeyPath,
			Value:  "",
			Usage:  "path to private key",
			EnvVar: "TEMPORAL_CLI_TLS_KEY",
		},
		cli.StringFlag{
			Name:   FlagTLSCaPath,
			Value:  "",
			Usage:  "path to server CA certificate",
			EnvVar: "TEMPORAL_CLI_TLS_CA",
		},
		cli.BoolFlag{
			Name:   FlagTLSEnableHostVerification,
			Usage:  "validates hostname of temporal cluster against server certificate",
			EnvVar: "TEMPORAL_CLI_TLS_ENABLE_HOST_VERIFICATION",
		},
		cli.StringFlag{
			Name:   FlagTLSServerName,
			Value:  "",
			Usage:  "override for target server name",
			EnvVar: "TEMPORAL_CLI_TLS_SERVER_NAME",
		},
		cli.StringFlag{
			Name:   FlagHeadersProviderPluginWithAlias,
			Value:  "",
			Usage:  "headers provider plugin",
			EnvVar: "TEMPORAL_CLI_PLUGIN_HEADERS_PROVIDER",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:        "namespace",
			Aliases:     []string{"n"},
			Usage:       "Operate Temporal namespace",
			Subcommands: newNamespaceCommands(),
		},
		{
			Name:        "workflow",
			Aliases:     []string{"wf"},
			Usage:       "Operate Temporal workflow",
			Subcommands: newWorkflowCommands(),
		},
		{
			Name:        "activity",
			Aliases:     []string{"act"},
			Usage:       "operate activities of workflow",
			Subcommands: newActivityCommands(),
		},
		{
			Name:        "taskqueue",
			Aliases:     []string{"tq"},
			Usage:       "Operate Temporal task queue",
			Subcommands: newTaskQueueCommands(),
		},
		{
			Name:        "batch",
			Usage:       "batch operation on a list of workflows from query.",
			Subcommands: newBatchCommands(),
		},
		{
			Name:    "admin",
			Aliases: []string{"adm"},
			Usage:   "Run admin operation",
			Subcommands: []cli.Command{
				{
					Name:        "workflow",
					Aliases:     []string{"wf"},
					Usage:       "Run admin operation on workflow",
					Subcommands: newAdminWorkflowCommands(),
				},
				{
					Name:        "shard",
					Aliases:     []string{"shar"},
					Usage:       "Run admin operation on specific shard",
					Subcommands: newAdminShardManagementCommands(),
				},
				{
					Name:        "history_host",
					Aliases:     []string{"hist"},
					Usage:       "Run admin operation on history host",
					Subcommands: newAdminHistoryHostCommands(),
				},
				{
					Name:        "namespace",
					Aliases:     []string{"d"},
					Usage:       "Run admin operation on namespace",
					Subcommands: newAdminNamespaceCommands(),
				},
				{
					Name:        "elasticsearch",
					Aliases:     []string{"es"},
					Usage:       "Run admin operation on ElasticSearch",
					Subcommands: newAdminElasticSearchCommands(),
				},
				{
					Name:        "taskqueue",
					Aliases:     []string{"tq"},
					Usage:       "Run admin operation on taskQueue",
					Subcommands: newAdminTaskQueueCommands(),
				},
				{
					Name:        "membership",
					Usage:       "Run admin operation on membership",
					Subcommands: newAdminMembershipCommands(),
				},
				{
					Name:        "cluster",
					Aliases:     []string{"cl"},
					Usage:       "Run admin operation on cluster",
					Subcommands: newAdminClusterCommands(),
				},
				{
					Name:        "dlq",
					Aliases:     []string{"dlq"},
					Usage:       "Run admin operation on DLQ",
					Subcommands: newAdminDLQCommands(),
				},
				{
					Name:        "db",
					Aliases:     []string{"db"},
					Usage:       "Run admin operations on database",
					Subcommands: newDBCommands(),
				},
				{
					Name:        "decode",
					Usage:       "Decode payload",
					Subcommands: newDecodeCommands(),
				},
			},
		},
		{
			Name:        "cluster",
			Aliases:     []string{"cl"},
			Usage:       "Operate Temporal cluster",
			Subcommands: newClusterCommands(),
		},
	}
	app.Before = LoadPlugins
	app.After = KillPlugins

	// set builder if not customized
	if cFactory == nil {
		SetFactory(NewClientFactory())
	}
	return app
}

func LoadPlugins(ctx *cli.Context) error {
	headersProviderPlugin := ctx.String(FlagHeadersProviderPlugin)
	if headersProviderPlugin != "" {
		name := "tctl-plugin-headers-provider-" + headersProviderPlugin

		var err error
		cliHeadersProvider, err = NewHeadersProviderPlugin(name)
		if err != nil {
			fmt.Printf("unable to register headers provider plugin %s: %v\n", headersProviderPlugin, err)
			return err
		}
	}

	return nil
}

func KillPlugins(ctx *cli.Context) error {
	if cliHeadersProvider != nil {
		cliHeadersProvider.(HeadersProviderPluginWrapper).Kill()
	}

	return nil
}
