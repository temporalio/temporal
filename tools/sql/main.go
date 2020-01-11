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

package sql

import (
	"os"

	"github.com/urfave/cli"

	"github.com/uber/cadence/tools/common/schema"
)

const defaultSQLPort = 3306

// RunTool runs the cadence-cassandra-tool command line tool
func RunTool(args []string) error {
	app := BuildCLIOptions()
	return app.Run(args)
}

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context) error) {
	quiet := c.GlobalBool(schema.CLIOptQuiet)
	err := handler(c)
	if err != nil && !quiet {
		os.Exit(1)
	}
}

// BuildCLIOptions builds the options for cli
func BuildCLIOptions() *cli.App {

	app := cli.NewApp()
	app.Name = "cadence-sql-tool"
	app.Usage = "Command line tool for cadence sql operations"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   schema.CLIFlagEndpoint,
			Value:  "127.0.0.1",
			Usage:  "hostname or ip address of sql host to connect to",
			EnvVar: "SQL_HOST",
		},
		cli.IntFlag{
			Name:   schema.CLIFlagPort,
			Value:  defaultSQLPort,
			Usage:  "port of sql host to connect to",
			EnvVar: "SQL_PORT",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagUser,
			Value:  "",
			Usage:  "user name used for authentication when connecting to sql host",
			EnvVar: "SQL_USER",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagPassword,
			Value:  "",
			Usage:  "password used for authentication when connecting to sql host",
			EnvVar: "SQL_PASSWORD",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagDatabase,
			Value:  "cadence",
			Usage:  "name of the sql database",
			EnvVar: "SQL_DATABASE",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagPluginName,
			Value:  "mysql",
			Usage:  "name of the sql plugin",
			EnvVar: "SQL_PLUGIN",
		},
		cli.BoolFlag{
			Name:  schema.CLIFlagQuiet,
			Usage: "Don't set exit status to 1 on error",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagConnectAttributes,
			Usage:  "sql connect attributes",
			EnvVar: "SQL_CONNECT_ATTRIBUTES",
		},
		cli.BoolFlag{
			Name:   schema.CLIFlagEnableTLS,
			Usage:  "enable TLS over sql connection",
			EnvVar: "SQL_TLS",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSCertFile,
			Usage:  "sql tls client cert path (tls must be enabled)",
			EnvVar: "SQL_TLS_CERT_FILE",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSKeyFile,
			Usage:  "sql tls client key path (tls must be enabled)",
			EnvVar: "SQL_TLS_KEY_FILE",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagTLSCaFile,
			Usage:  "sql tls client ca file (tls must be enabled)",
			EnvVar: "SQL_TLS_CA_FILE",
		},
		cli.BoolFlag{
			Name:   schema.CLIFlagTLSEnableHostVerification,
			Usage:  "sql tls verify hostname and server cert (tls must be enabled)",
			EnvVar: "SQL_TLS_ENABLE_HOST_VERIFICATION",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of sql schema",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagVersion,
					Usage: "initial version of the schema, cannot be used with disable-versioning",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagSchemaFile,
					Usage: "path to the .sql schema file; if un-specified, will just setup versioning tables",
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
			Usage:   "update sql schema to a specific version",
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
			Name:    "create-database",
			Aliases: []string{"create"},
			Usage:   "creates a database",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagDatabase,
					Usage: "name of the database",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, createDatabase)
			},
		},
	}

	return app
}
