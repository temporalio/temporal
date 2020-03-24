// Copyright (c) 2017 Uber Technologies, Inc.
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
)

const (
	// Version is the controlled version string. It should be updated every time
	// before we release a new version.
	Version = "0.9.1"
)

// SetFactory is used to set the ClientFactory global
func SetFactory(factory ClientFactory) {
	cFactory = factory
}

// NewCliApp instantiates a new instance of the CLI application.
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal"
	app.Usage = "A command-line tool for temporal users"
	app.Version = Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   FlagAddressWithAlias,
			Value:  "",
			Usage:  "host:port for temporal frontend service",
			EnvVar: "TEMPORAL_CLI_ADDRESS",
		},
		cli.StringFlag{
			Name:   FlagDomainWithAlias,
			Value:  "default",
			Usage:  "temporal workflow domain",
			EnvVar: "TEMPORAL_CLI_DOMAIN",
		},
		cli.IntFlag{
			Name:   FlagContextTimeoutWithAlias,
			Value:  defaultContextTimeoutInSeconds,
			Usage:  "optional timeout for context of RPC call in seconds",
			EnvVar: "TEMPORAL_CONTEXT_TIMEOUT",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:        "domain",
			Aliases:     []string{"d"},
			Usage:       "Operate temporal domain",
			Subcommands: newDomainCommands(),
		},
		{
			Name:        "workflow",
			Aliases:     []string{"wf"},
			Usage:       "Operate temporal workflow",
			Subcommands: newWorkflowCommands(),
		},
		{
			Name:        "tasklist",
			Aliases:     []string{"tl"},
			Usage:       "Operate temporal tasklist",
			Subcommands: newTaskListCommands(),
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
					Name:        "kafka",
					Aliases:     []string{"ka"},
					Usage:       "Run admin operation on kafka messages",
					Subcommands: newAdminKafkaCommands(),
				},
				{
					Name:        "domain",
					Aliases:     []string{"d"},
					Usage:       "Run admin operation on domain",
					Subcommands: newAdminDomainCommands(),
				},
				{
					Name:        "elasticsearch",
					Aliases:     []string{"es"},
					Usage:       "Run admin operation on ElasticSearch",
					Subcommands: newAdminElasticSearchCommands(),
				},
				{
					Name:        "tasklist",
					Aliases:     []string{"tl"},
					Usage:       "Run admin operation on taskList",
					Subcommands: newAdminTaskListCommands(),
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
			},
		},
		{
			Name:        "cluster",
			Aliases:     []string{"cl"},
			Usage:       "Operate temporal cluster",
			Subcommands: newClusterCommands(),
		},
	}

	// set builder if not customized
	if cFactory == nil {
		SetFactory(NewClientFactory())
	}
	return app
}
