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

package elasticsearch

import (
	"os"

	"github.com/urfave/cli"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tools/common/schema"
)

// RunTool runs the temporal-elasticsearch-tool command line tool
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
	app.Name = "temporal-elasticsearch-tool"
	app.Usage = "Command line tool for temporal elasticsearch operations"
	app.Version = "0.0.1"

	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   CLIFlagESURL,
			Value:  "http://127.0.0.1:9200",
			Usage:  "hostname or ip address of elasticsearch server",
			EnvVar: "ES_SERVER",
		},
		cli.StringFlag{
			Name:   CLIFlagESUsername,
			Value:  "",
			Usage:  "username for elasticsearch or aws_access_key_id if using static aws credentials",
			EnvVar: "ES_USER",
		},
		cli.StringFlag{
			Name:   CLIFlagESPassword,
			Value:  "",
			Usage:  "password for elasticsearch or aws_secret_access_key if using static aws credentials",
			EnvVar: "ES_PWD",
		},
		cli.StringFlag{
			Name:   CLIFlagESVersion,
			Value:  "v7",
			Usage:  "elasticsearch version",
			EnvVar: "ES_VERSION",
		},
		cli.StringFlag{
			Name:   CLIFlagAWSCredentials,
			Value:  "",
			Usage:  "AWS credentials provider (supported ['static', 'environment', 'aws-sdk-default'])",
			EnvVar: "ES_AWS_CREDENTIALS",
		},
		cli.StringFlag{
			Name:   CLIFlagAWSToken,
			Value:  "",
			Usage:  "AWS token for use with 'static' AWS credentials provider",
			EnvVar: "ES_AWS_TOKEN",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of elasticsearch schema and index",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  CLIFlagSettingsFile,
					Usage: "path to the .json cluster settings file",
				},
				cli.StringFlag{
					Name:  CLIFlagTemplateFile,
					Usage: "path to the .json index template file",
				},
				cli.StringFlag{
					Name:  CLIFlagVisibilityIndex,
					Usage: "name of the visibility index to create",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, setup, logger)
			},
		},
		{
			Name:  "ping",
			Usage: "continuously pings the elasticsearch host until a successful response is received",
			Flags: []cli.Flag{},
			Action: func(c *cli.Context) {
				cliHandler(c, ping, logger)
			},
		},
	}

	return app
}
