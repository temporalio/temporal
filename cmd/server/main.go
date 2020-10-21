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

package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/urfave/cli/v2"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/headers"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"      // needed to load mysql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // needed to load postgresql plugin
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/temporal"
)

// main entry point for the temporal server
func main() {
	app := buildCLI()
	_ = app.Run(os.Args)
}

// buildCLI is the main entry point for the temporal server
func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal"
	app.Usage = "Temporal server"
	app.Version = headers.ServerVersion
	app.ArgsUsage = " "
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "root",
			Aliases: []string{"r"},
			Value:   ".",
			Usage:   "root directory of execution environment",
			EnvVars: []string{config.EnvKeyRoot},
		},
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "config",
			Usage:   "config dir path relative to root",
			EnvVars: []string{config.EnvKeyConfigDir},
		},
		&cli.StringFlag{
			Name:    "env",
			Aliases: []string{"e"},
			Value:   "development",
			Usage:   "runtime environment",
			EnvVars: []string{config.EnvKeyEnvironment},
		},
		&cli.StringFlag{
			Name:    "zone",
			Aliases: []string{"az"},
			Usage:   "availability zone",
			EnvVars: []string{config.EnvKeyAvailabilityZone},
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:      "start",
			Usage:     "Start Temporal server",
			ArgsUsage: " ",
			Flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:    "services",
					Aliases: []string{"s"},
					Value:   cli.NewStringSlice(temporal.Services...),
					Usage:   "List of services to start",
				},
			},
			Before: func(c *cli.Context) error {
				if c.Args().Len() > 0 {
					return cli.NewExitError("ERROR: start command doesn't support arguments. Use --services flags instead.", 1)
				}
				return nil
			},
			Action: func(c *cli.Context) error {
				env := c.String("env")
				zone := c.String("zone")
				configDir := path.Join(c.String("root"), c.String("config"))

				services := splitServices(c.StringSlice("services"))

				s := temporal.NewServer(
					temporal.ForServices(services),
					temporal.WithConfigLoader(configDir, env, zone),
					temporal.InterruptOn(temporal.InterruptCh()),
					temporal.WithAuthorizer(authorization.NewNopAuthorizer()),
				)

				err := s.Start()
				if err != nil {
					return cli.NewExitError(fmt.Sprintf("Unable to start server: %v.", err), 1)
				}
				return cli.NewExitError("All services are stopped.", 0)
			},
		},
	}
	return app
}

// For backward compatiblity to support old flag format (i.e. `--services=frontend,history,matching`).
func splitServices(services []string) []string {
	var result []string
	for _, service := range services {
		if strings.Contains(service, ",") {
			result = append(result, strings.Split(service, ",")...)
			log.Println("WARNING: comma separated format for --services flag is depricated. Specify multiply --services flags instead.")
		} else {
			result = append(result, service)
		}
	}
	return result
}
