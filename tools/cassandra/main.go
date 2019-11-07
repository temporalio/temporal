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

package cassandra

import (
	"os"

	"github.com/urfave/cli"

	"github.com/uber/cadence/tools/common/schema"
)

// RunTool runs the cadence-cassandra-tool command line tool
func RunTool(args []string) error {
	app := buildCLIOptions()
	return app.Run(args)
}

// SetupSchema setups the cassandra schema
func SetupSchema(config *SetupSchemaConfig) error {
	if err := validateCQLClientConfig(&config.CQLClientConfig, false); err != nil {
		return err
	}
	db, err := newCQLClient(&config.CQLClientConfig)
	if err != nil {
		return err
	}
	return schema.SetupFromConfig(&config.SetupConfig, db)
}

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context) error) {
	quiet := c.GlobalBool(schema.CLIOptQuiet)
	err := handler(c)
	if err != nil && !quiet {
		os.Exit(1)
	}
}

func buildCLIOptions() *cli.App {

	app := cli.NewApp()
	app.Name = "cadence-cassandra-tool"
	app.Usage = "Command line tool for cadence cassandra operations"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   schema.CLIFlagEndpoint,
			Value:  "127.0.0.1",
			Usage:  "hostname or ip address of cassandra host to connect to",
			EnvVar: "CASSANDRA_HOST",
		},
		cli.IntFlag{
			Name:   schema.CLIFlagPort,
			Value:  defaultCassandraPort,
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
			Value:  "cadence",
			Usage:  "name of the cassandra Keyspace",
			EnvVar: "CASSANDRA_KEYSPACE",
		},
		cli.BoolFlag{
			Name:  schema.CLIFlagQuiet,
			Usage: "Don't set exit status to 1 on error",
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
		cli.BoolFlag{
			Name:   schema.CLIFlagTLSEnableHostVerification,
			Usage:  "TLS host verification",
			EnvVar: "CASSANDRA_TLS_VERIFY_HOST",
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
				cliHandler(c, setupSchema)
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
				cli.BoolFlag{
					Name:  schema.CLIFlagDryrun,
					Usage: "do a dryrun",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, updateSchema)
			},
		},
		{
			Name:    "create-Keyspace",
			Aliases: []string{"create"},
			Usage:   "creates a Keyspace with simple strategy",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagKeyspace,
					Usage: "name of the Keyspace",
				},
				cli.IntFlag{
					Name:  schema.CLIFlagReplicationFactor,
					Value: 1,
					Usage: "replication factor for the Keyspace",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, createKeyspace)
			},
		},
	}

	return app
}
