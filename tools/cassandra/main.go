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

package cassandra

import (
	"fmt"
	"os"

	"github.com/urfave/cli"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tools/common/schema"
)

// RunTool runs the temporal-cassandra-tool command line tool
func RunTool(args []string) error {
	app := buildCLIOptions()
	return app.Run(args)
}

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context, logger log.Logger) error, logger log.Logger) {
	quiet := c.GlobalBool(schema.CLIOptQuiet)
	err := handler(c, logger)
	if err != nil && !quiet {
		os.Exit(1)
	}
}

func buildCLIOptions() *cli.App {

	app := cli.NewApp()
	app.Name = "temporal-cassandra-tool"
	app.Usage = "Command line tool for temporal cassandra operations"
	app.Version = "0.0.1"
	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   schema.CLIFlagEndpoint,
			Value:  "127.0.0.1",
			Usage:  "hostname or ip address of cassandra host to connect to",
			EnvVar: "CASSANDRA_HOST",
		},
		cli.IntFlag{
			Name:   schema.CLIFlagPort,
			Value:  environment.GetCassandraPort(),
			Usage:  "Port of cassandra host to connect to",
			EnvVar: "CASSANDRA_PORT",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagUser,
			Value:  "",
			Usage:  "User name used for authentication for connecting to cassandra host",
			EnvVar: "CASSANDRA_USER",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagPassword,
			Value:  "",
			Usage:  "Password used for authentication for connecting to cassandra host",
			EnvVar: "CASSANDRA_PASSWORD",
		},
		cli.IntFlag{
			Name:   schema.CLIFlagTimeout,
			Value:  defaultTimeout,
			Usage:  "request Timeout in seconds used for cql client",
			EnvVar: "CASSANDRA_TIMEOUT",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagKeyspace,
			Value:  "temporal",
			Usage:  "name of the cassandra Keyspace",
			EnvVar: "CASSANDRA_KEYSPACE",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagDatacenter,
			Value:  "",
			Usage:  "enable NetworkTopologyStrategy by providing datacenter name",
			EnvVar: "CASSANDRA_DATACENTER",
		},
		cli.BoolFlag{
			Name:  schema.CLIFlagQuiet,
			Usage: "Don't set exit status to 1 on error",
		},
		cli.BoolFlag{
			Name:   schema.CLIFlagDisableInitialHostLookup,
			Usage:  "instructs gocql driver to only connect to the supplied hosts vs. attempting to lookup additional hosts via the system.peers table",
			EnvVar: "CASSANDRA_DISABLE_INITIAL_HOST_LOOKUP",
		},
		cli.BoolFlag{
			Name:   schema.CLIFlagEnableTLS,
			Usage:  "enable TLS",
			EnvVar: "CASSANDRA_ENABLE_TLS",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSCertFile,
			Usage:  "TLS cert file",
			EnvVar: "CASSANDRA_TLS_CERT",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSKeyFile,
			Usage:  "TLS key file",
			EnvVar: "CASSANDRA_TLS_KEY",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSCaFile,
			Usage:  "TLS CA file",
			EnvVar: "CASSANDRA_TLS_CA",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSHostName,
			Value:  "",
			Usage:  "override for target server name",
			EnvVar: "CASSANDRA_TLS_SERVER_NAME",
		},
		cli.BoolFlag{
			Name:   schema.CLIFlagTLSDisableHostVerification,
			Usage:  "disable tls host name verification (tls must be enabled)",
			EnvVar: "CASSANDRA_TLS_DISABLE_HOST_VERIFICATION",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of cassandra schema",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagVersion,
					Usage: "initial version of the schema, cannot be used with disable-versioning",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagSchemaFile,
					Usage: "path to the .cql schema file; if un-specified, will just setup versioning tables",
				},
				cli.BoolFlag{
					Name:  schema.CLIFlagDisableVersioning,
					Usage: "disable setup of schema versioning",
				},
				cli.BoolFlag{
					Name:  schema.CLIFlagOverwrite,
					Usage: "drop all existing tables before setting up new schema",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, setupSchema, logger)
			},
		},
		{
			Name:    "update-schema",
			Aliases: []string{"update"},
			Usage:   "update cassandra schema to a specific version",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagTargetVersion,
					Usage: "target version for the schema update, defaults to latest",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagSchemaDir,
					Usage: "path to directory containing versioned schema",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, updateSchema, logger)
			},
		},
		{
			Name:    "create-keyspace",
			Aliases: []string{"create", "create-Keyspace"},
			Usage:   "creates a keyspace with simple strategy or network topology if datacenter name is provided",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagKeyspace,
					Usage: "name of the keyspace",
				},
				cli.IntFlag{
					Name:  schema.CLIFlagReplicationFactor,
					Value: 1,
					Usage: "replication factor for the keyspace",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagDatacenter,
					Value: "",
					Usage: "enable NetworkTopologyStrategy by providing datacenter name",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, createKeyspace, logger)
			},
		},
		{
			Name:    "drop-keyspace",
			Aliases: []string{"drop"},
			Usage:   "drops a keyspace with simple strategy or network topology if datacenter name is provided",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagKeyspace,
					Usage: "name of the keyspace",
				},
				cli.IntFlag{
					Name:  schema.CLIFlagReplicationFactor,
					Value: 1,
					Usage: "replication factor for the keyspace",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagDatacenter,
					Value: "",
					Usage: "enable NetworkTopologyStrategy by providing datacenter name",
				},
				cli.BoolFlag{
					Name:  schema.CLIFlagForce,
					Usage: "don't prompt for confirmation",
				},
			},
			Action: func(c *cli.Context) {
				drop := c.Bool(schema.CLIOptForce)
				if !drop {
					keyspace := c.String(schema.CLIOptKeyspace)
					fmt.Printf("Are you sure you want to drop keyspace %q (y/N)? ", keyspace)
					y := ""
					_, _ = fmt.Scanln(&y)
					if y == "y" || y == "Y" {
						drop = true
					}
				}
				if drop {
					cliHandler(c, dropKeyspace, logger)
				}
			},
		},
		{
			Name:    "validate-health",
			Aliases: []string{"vh"},
			Usage:   "validates health of cassandra by attempting to establish CQL session to system keyspace",
			Action: func(c *cli.Context) {
				cliHandler(c, validateHealth, logger)
			},
		},
	}

	return app
}
