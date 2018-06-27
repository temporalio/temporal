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
	"github.com/urfave/cli"
	"os"
)

// RunTool runs the cadence-cassandra-tool command line tool
func RunTool(args []string) error {
	app := buildCLIOptions()
	return app.Run(args)
}

// SetupSchema setups the cassandra schema
func SetupSchema(config *SetupSchemaConfig) error {
	if err := validateSetupSchemaConfig(config); err != nil {
		return err
	}
	return handleSetupSchema(config)
}

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context) error) {
	quiet := c.GlobalBool(cliOptQuiet)
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
			Name:   cliFlagEndpoint,
			Value:  "127.0.0.1",
			Usage:  "hostname or ip address of cassandra host to connect to",
			EnvVar: "CASSANDRA_HOST",
		},
		cli.IntFlag{
			Name:   cliFlagPort,
			Value:  defaultCassandraPort,
			Usage:  "port of cassandra host to connect to",
			EnvVar: "CASSANDRA_PORT",
		},
		cli.StringFlag{
			Name:   cliFlagUser,
			Value:  "",
			Usage:  "user name used for authentication for connecting to cassandra host",
			EnvVar: "CASSANDRA_USER",
		},
		cli.StringFlag{
			Name:   cliFlagPassword,
			Value:  "",
			Usage:  "password used for authentication for connecting to cassandra host",
			EnvVar: "CASSANDRA_PASSWORD",
		},
		cli.IntFlag{
			Name:   cliFlagTimeout,
			Value:  defaultTimeout,
			Usage:  "request timeout in seconds used for cql client",
			EnvVar: "CASSANDRA_TIMEOUT",
		},
		cli.StringFlag{
			Name:   cliFlagKeyspace,
			Value:  "cadence",
			Usage:  "name of the cassandra keyspace",
			EnvVar: "CASSANDRA_KEYSPACE",
		},
		cli.BoolFlag{
			Name:  cliFlagQuiet,
			Usage: "Don't set exit status to 1 on error",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of cassandra schema",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  cliFlagVersion,
					Usage: "initial version of the schema, cannot be used with disable-versioning",
				},
				cli.StringFlag{
					Name:  cliFlagSchemaFile,
					Usage: "path to the .cql schema file; if un-specified, will just setup versioning tables",
				},
				cli.BoolFlag{
					Name:  cliFlagDisableVersioning,
					Usage: "disable setup of schema versioning",
				},
				cli.BoolFlag{
					Name:  cliFlagOverwrite,
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
					Name:  cliFlagTargetVersion,
					Usage: "target version for the schema update, defaults to latest",
				},
				cli.StringFlag{
					Name:  cliFlagSchemaDir,
					Usage: "path to directory containing versioned schema",
				},
				cli.BoolFlag{
					Name:  cliFlagDryrun,
					Usage: "do a dryrun",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, updateSchema)
			},
		},
		{
			Name:    "create-keyspace",
			Aliases: []string{"create"},
			Usage:   "creates a keyspace with simple strategy",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  cliFlagKeyspace,
					Usage: "name of the keyspace",
				},
				cli.IntFlag{
					Name:  cliFlagReplicationFactor,
					Value: 1,
					Usage: "replication factor for the keyspace",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, createKeyspace)
			},
		},
	}

	return app
}
