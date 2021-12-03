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
	"github.com/urfave/cli"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/tools/cli/dataconverter"
	"go.temporal.io/server/tools/cli/headersprovider"
	"go.temporal.io/server/tools/cli/plugin"
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
			Usage:  "Optional timeout for context of RPC call in seconds",
			EnvVar: "TEMPORAL_CONTEXT_TIMEOUT",
		},
		cli.BoolFlag{
			Name:  FlagAutoConfirm,
			Usage: "Automatically confirm all prompts",
		},
		cli.StringFlag{
			Name:   FlagTLSCertPath,
			Value:  "",
			Usage:  "Path to x509 certificate",
			EnvVar: "TEMPORAL_CLI_TLS_CERT",
		},
		cli.StringFlag{
			Name:   FlagTLSKeyPath,
			Value:  "",
			Usage:  "Path to private key",
			EnvVar: "TEMPORAL_CLI_TLS_KEY",
		},
		cli.StringFlag{
			Name:   FlagTLSCaPath,
			Value:  "",
			Usage:  "Path to server CA certificate",
			EnvVar: "TEMPORAL_CLI_TLS_CA",
		},
		cli.BoolFlag{
			Name:   FlagTLSDisableHostVerification,
			Usage:  "Disable tls host name verification (tls must be enabled)",
			EnvVar: "TEMPORAL_CLI_TLS_DISABLE_HOST_VERIFICATION",
		},
		cli.StringFlag{
			Name:   FlagTLSServerName,
			Value:  "",
			Usage:  "Override for target server name",
			EnvVar: "TEMPORAL_CLI_TLS_SERVER_NAME",
		},
		cli.StringFlag{
			Name:   FlagHeadersProviderPluginWithAlias,
			Value:  "",
			Usage:  "Headers provider plugin executable name",
			EnvVar: "TEMPORAL_CLI_PLUGIN_HEADERS_PROVIDER",
		},
		cli.StringFlag{
			Name:   FlagDataConverterPluginWithAlias,
			Value:  "",
			Usage:  "Data converter plugin executable name",
			EnvVar: "TEMPORAL_CLI_PLUGIN_DATA_CONVERTER",
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
			Usage:       "Operate activities of workflow",
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
			Usage:       "Batch operation on a list of workflows from query.",
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
		{
			Name:        "dataconverter",
			Aliases:     []string{"dc"},
			Usage:       "Operate Custom Data Converter",
			Subcommands: newDataConverterCommands(),
		},
	}
	app.Before = loadPlugins
	app.After = stopPlugins

	// set builder if not customized
	if cFactory == nil {
		SetFactory(NewClientFactory())
	}
	return app
}

func loadPlugins(c *cli.Context) error {
	dcPlugin := c.String(FlagDataConverterPlugin)
	if dcPlugin != "" {
		dataConverter, err := plugin.NewDataConverterPlugin(dcPlugin)
		if err != nil {
			ErrorAndExit("unable to load data converter plugin", err)
		}

		dataconverter.SetCurrent(dataConverter)
	}

	hpPlugin := c.String(FlagHeadersProviderPlugin)
	if hpPlugin != "" {
		headersProvider, err := plugin.NewHeadersProviderPlugin(hpPlugin)
		if err != nil {
			ErrorAndExit("unable to load headers provider plugin", err)
		}

		headersprovider.SetCurrent(headersProvider)
	}

	return nil
}

func stopPlugins(c *cli.Context) error {
	plugin.StopPlugins()

	return nil
}
