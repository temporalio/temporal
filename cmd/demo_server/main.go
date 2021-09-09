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

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/headers"
	tlog "go.temporal.io/server/common/log"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"      // needed to load mysql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // needed to load postgresql plugin
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
			EnvVars: []string{config.EnvKeyAvailabilityZone, config.EnvKeyAvailabilityZoneTypo},
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:      "start",
			Usage:     "Start Temporal server",
			ArgsUsage: " ",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "services",
					Aliases: []string{"s"},
					Usage:   "comma separated list of services to start. Deprecated",
					Hidden:  true,
				},
				&cli.StringSliceFlag{
					Name:    "service",
					Aliases: []string{"svc"},
					Value:   cli.NewStringSlice(temporal.Services...),
					Usage:   "service(s) to start",
				},
			},
			Before: func(c *cli.Context) error {
				if c.Args().Len() > 0 {
					return cli.Exit("ERROR: start command doesn't support arguments. Use --service flag instead.", 1)
				}
				return nil
			},
			Action: run,
		},
	}
	return app
}

func MyLoggerProvider(cfg *config.Config) tlog.Logger {
	panic("My logger provider haha")
	// return tlog.NewZapLogger(tlog.BuildZapLogger(cfg.Log))
}


func run(c *cli.Context) error {
	s, err := temporal.InitializeServer(c,
		temporal.WithLoggerProvider(MyLoggerProvider))
	if err != nil {
		return cli.Exit(fmt.Sprintf("Unable to initialize server. Error: %v", err), 1)
	}

	err = s.Start()

	if err != nil {
		return cli.Exit(fmt.Sprintf("Unable to start server. Error: %v", err), 1)
	}
	return cli.Exit("All services are stopped.", 0)
}
