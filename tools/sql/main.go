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

package sql

import (
	"fmt"
	"os"

	"github.com/urfave/cli"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tools/common/schema"
)

const defaultSQLPort = 3306

// RunTool runs the temporal-sql-tool command line tool
func RunTool(args []string) error {
	app := BuildCLIOptions()
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

// BuildCLIOptions builds the options for cli
func BuildCLIOptions() *cli.App {

	app := cli.NewApp()
	app.Name = "temporal-sql-tool"
	app.Usage = "Command line tool for temporal sql operations"
	app.Version = "0.0.1"

	logger := log.NewCLILogger()

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
			Value:  "temporal",
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
		cli.StringFlag{
			Name:   schema.CLIFlagTLSHostName,
			Value:  "",
			Usage:  "override for target server name",
			EnvVar: "SQL_TLS_SERVER_NAME",
		},
		cli.BoolFlag{
			Name:   schema.CLIFlagTLSDisableHostVerification,
			Usage:  "disable tls host name verification (tls must be enabled)",
			EnvVar: "SQL_TLS_DISABLE_HOST_VERIFICATION",
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
				cliHandler(c, setupSchema, logger)
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
			},
			Action: func(c *cli.Context) {
				cliHandler(c, updateSchema, logger)
			},
		},
		{
			Name:    "create-database",
			Aliases: []string{"create"},
			Usage:   "creates a database",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIOptDefaultDb,
					Usage: "optional default db to connect to, this is not the db to be created",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, createDatabase, logger)
			},
		},
		{
			Name:    "drop-database",
			Aliases: []string{"drop"},
			Usage:   "drops a database",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIOptDefaultDb,
					Usage: "optional default db to connect to, not the db to be deleted",
				},
				cli.BoolFlag{
					Name:  schema.CLIFlagForce,
					Usage: "don't prompt for confirmation",
				},
			},
			Action: func(c *cli.Context) {
				drop := c.Bool(schema.CLIOptForce)
				if !drop {
					database := c.GlobalString(schema.CLIOptDatabase)
					fmt.Printf("Are you sure you want to drop database %q (y/N)? ", database)
					y := ""
					_, _ = fmt.Scanln(&y)
					if y == "y" || y == "Y" {
						drop = true
					}
				}
				if drop {
					cliHandler(c, dropDatabase, logger)
				}
			},
		},
	}

	return app
}
