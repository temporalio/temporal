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
	stdlog "log"
	"os"
	"path"
	"strings"
	_ "time/tzdata" // embed tzdata as a fallback

	"github.com/urfave/cli/v2"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/build"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"      // needed to load mysql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // needed to load postgresql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"     // needed to load sqlite plugin
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
			Action: func(c *cli.Context) error {
				env := c.String("env")
				zone := c.String("zone")
				configDir := path.Join(c.String("root"), c.String("config"))
				services := c.StringSlice("service")

				// For backward compatibility to support old flag format (i.e. `--services=frontend,history,matching`).
				if c.IsSet("services") {
					stdlog.Println("WARNING: --services flag is deprecated. Specify multiply --service flags instead.")
					services = strings.Split(c.String("services"), ",")
				}

				cfg, err := config.LoadConfig(env, configDir, zone)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to load configuration: %v.", err), 1)
				}

				logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
				logger.Info("Build info.",
					tag.NewTimeTag("git-time", build.InfoData.GitTime),
					tag.NewStringTag("git-revision", build.InfoData.GitRevision),
					tag.NewBoolTag("git-modified", build.InfoData.GitModified),
					tag.NewStringTag("go-arch", build.InfoData.GoArch),
					tag.NewStringTag("go-os", build.InfoData.GoOs),
					tag.NewStringTag("go-version", build.InfoData.GoVersion),
					tag.NewBoolTag("cgo-enabled", build.InfoData.CgoEnabled),
					tag.NewStringTag("server-version", headers.ServerVersion),
				)

				var dynamicConfigClient dynamicconfig.Client
				if cfg.DynamicConfigClient != nil {
					dynamicConfigClient, err = dynamicconfig.NewFileBasedClient(cfg.DynamicConfigClient, logger, temporal.InterruptCh())
					if err != nil {
						return cli.Exit(fmt.Sprintf("Unable to create dynamic config client. Error: %v", err), 1)
					}
				} else {
					dynamicConfigClient = dynamicconfig.NewNoopClient()
					logger.Info("Dynamic config client is not configured. Using noop client.")
				}

				authorizer, err := authorization.GetAuthorizerFromConfig(
					&cfg.Global.Authorization,
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate authorizer. Error: %v", err), 1)
				}

				claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate claim mapper: %v.", err), 1)
				}
				s := temporal.NewServer(
					temporal.ForServices(services),
					temporal.WithConfig(cfg),
					temporal.WithDynamicConfigClient(dynamicConfigClient),
					temporal.WithLogger(logger),
					temporal.InterruptOn(temporal.InterruptCh()),
					temporal.WithAuthorizer(authorizer),
					temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
						return claimMapper
					}),
				)

				err = s.Start()
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to start server. Error: %v", err), 1)
				}
				return cli.Exit("All services are stopped.", 0)
			},
		},
	}
	return app
}
