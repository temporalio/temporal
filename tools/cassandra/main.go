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
		cli.StringFlag{
			Name:   cliFlagKeyspace,
			Value:  "cadence",
			Usage:  "name of the cassandra keyspace",
			EnvVar: "CASSANDRA_KEYSPACE",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of cassandra schema",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  cliFlagVersion,
					Usage: "initial version of the schema, cannot be used with disable-versioning",
				},
				cli.StringFlag{
					Name:  cliFlagSchemaFile,
					Usage: "path to the .cql schema file",
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
				setupSchema(c)
			},
		},
	}

	return app
}
