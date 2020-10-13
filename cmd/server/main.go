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
	"os"
	"path"
	"strings"

	"github.com/urfave/cli"

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

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "root, r",
			Value:  ".",
			Usage:  "root directory of execution environment",
			EnvVar: config.EnvKeyRoot,
		},
		cli.StringFlag{
			Name:   "config, c",
			Value:  "config",
			Usage:  "config dir path relative to root",
			EnvVar: config.EnvKeyConfigDir,
		},
		cli.StringFlag{
			Name:   "env, e",
			Value:  "development",
			Usage:  "runtime environment",
			EnvVar: config.EnvKeyEnvironment,
		},
		cli.StringFlag{
			Name:   "zone, az",
			Usage:  "availability zone",
			EnvVar: config.EnvKeyAvailabilityZone,
		},
	}

	allServicesStringSlice := cli.StringSlice(temporal.Services)
	app.Commands = []cli.Command{
		{
			Name:  "start",
			Usage: "Start Temporal server",
			Flags: []cli.Flag{
				cli.StringSliceFlag{
					Name:  "services, s",
					Value: &allServicesStringSlice,
					Usage: "List of services to start",
				},
			},
			Action: func(c *cli.Context) error {
				env := c.GlobalString("env")
				zone := c.GlobalString("zone")
				configDir := path.Join(c.GlobalString("root"), c.GlobalString("config"))

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
		result = append(result, strings.Split(service, ",")...)
	}
	return result
}
