package main

import (
	"fmt"
	stdlog "log"
	"os"
	"path"
	"strings"
	"text/template"
	_ "time/tzdata" // embed tzdata as a fallback

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/build"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
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
			Usage:   "root directory of execution environment (deprecated)",
			EnvVars: []string{config.EnvKeyRoot},
		},
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "config",
			Usage:   "config dir path relative to root (deprecated)",
			EnvVars: []string{config.EnvKeyConfigDir},
		},
		&cli.StringFlag{
			Name:    "env",
			Aliases: []string{"e"},
			Value:   "development",
			Usage:   "runtime environment (deprecated)",
			EnvVars: []string{config.EnvKeyEnvironment},
		},
		&cli.StringFlag{
			Name:    "zone",
			Aliases: []string{"az"},
			Usage:   "availability zone (deprecated)",
			EnvVars: []string{config.EnvKeyAvailabilityZone, config.EnvKeyAvailabilityZoneTypo},
		},
		&cli.StringFlag{
			Name:    "config-file",
			Usage:   "path to config file (absolute or relative to current working directory)",
			EnvVars: []string{config.EnvKeyConfigFile},
		},
		&cli.BoolFlag{
			Name:    "allow-no-auth",
			Usage:   "allow no authorizer",
			EnvVars: []string{config.EnvKeyAllowNoAuth},
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:         "validate-dynamic-config",
			Usage:        "Validate a dynamic config file[s] with known keys and types",
			ArgsUsage:    "<file> ...",
			OnUsageError: helpfulSubcommandFlagError,
			Action: func(c *cli.Context) error {
				total := 0
				for _, fileName := range c.Args().Slice() {
					contents, err := os.ReadFile(fileName)
					if err != nil {
						return err
					}
					result := dynamicconfig.LoadYamlFile(contents)
					total += len(result.Errors)
					fmt.Println(fileName)
					t := template.Must(template.New("").Parse(
						"{{range .Errors}}  error: {{.}}\n" +
							"{{end}}{{range .Warnings}}  warning: {{.}}\n" +
							"{{end}}",
					))
					_ = t.Execute(os.Stdout, result)
				}
				if total > 0 {
					return fmt.Errorf("%d total errors", total)
				}
				return nil
			},
		},
		{
			Name:         "render-config",
			Usage:        "Render server config template",
			ArgsUsage:    " ",
			OnUsageError: helpfulSubcommandFlagError,
			Action: func(c *cli.Context) error {
				cfg, err := config.Load(
					config.WithEnv(c.String("env")),
					config.WithConfigDir(c.String("config")),
					config.WithZone(c.String("zone")),
				)
				if err != nil {
					return cli.Exit(fmt.Errorf("Unable to load configuration: %w", err), 1)
				}
				fmt.Println(cfg.String())
				return nil
			},
		},
		{
			Name:         "start",
			Usage:        "Start Temporal server",
			ArgsUsage:    " ",
			OnUsageError: helpfulSubcommandFlagError,
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
					Value:   cli.NewStringSlice(temporal.DefaultServices...),
					Usage:   "service(s) to start",
					EnvVars: []string{"TEMPORAL_SERVICES"},
				},
			},
			Before: func(c *cli.Context) error {
				if c.Args().Len() > 0 {
					return cli.Exit("ERROR: start command doesn't support arguments. Use --service flag instead.", 1)
				}

				if c.IsSet("config-file") && (c.IsSet("config") || c.IsSet("env") || c.IsSet("zone") || c.IsSet("root")) {
					return cli.Exit("ERROR: can not use --config, --env, --zone, or --root with --config-file", 1)
				}
				return nil
			},
			Action: func(c *cli.Context) error {
				services := c.StringSlice("service")
				allowNoAuth := c.Bool("allow-no-auth")

				// For backward compatibility to support old flag format (i.e. `--services=frontend,history,matching`).
				if c.IsSet("services") {
					stdlog.Println("WARNING: --services flag is deprecated. Specify multiply --service flags instead.")
					services = strings.Split(c.String("services"), ",")
				}

				var cfg *config.Config
				var err error

				switch {
				case c.IsSet("config-file"):
					cfg, err = config.Load(config.WithConfigFile(c.String("config-file")))
				case c.IsSet("config") || c.IsSet("env") || c.IsSet("zone"):
					cfg, err = config.Load(
						config.WithEnv(c.String("env")),
						config.WithConfigDir(path.Join(c.String("root"), c.String("config"))),
						config.WithZone(c.String("zone")),
					)
				default:
					cfg, err = config.Load(config.WithEmbedded())
				}

				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to load configuration: %v.", err), 1)
				}

				logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
				logger.Info("Build info.",
					tag.Time("git-time", build.InfoData.GitTime),
					tag.String("git-revision", build.InfoData.GitRevision),
					tag.Bool("git-modified", build.InfoData.GitModified),
					tag.String("go-arch", build.InfoData.GoArch),
					tag.String("go-os", build.InfoData.GoOs),
					tag.String("go-version", build.InfoData.GoVersion),
					tag.Bool("cgo-enabled", build.InfoData.CgoEnabled),
					tag.String("server-version", headers.ServerVersion),
					tag.Bool("debug-mode", debug.Enabled),
				)

				authorizer, err := authorization.GetAuthorizerFromConfig(
					&cfg.Global.Authorization,
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate authorizer. Error: %v", err), 1)
				}
				if authorization.IsNoopAuthorizer(authorizer) && !allowNoAuth {
					logger.Warn(
						"Not using any authorizer and flag `--allow-no-auth` not detected. " +
							"Future versions will require using the flag `--allow-no-auth` " +
							"if you do not want to set an authorizer.",
					)
				}

				// Authorization mappers: claim and audience
				claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate claim mapper: %v.", err), 1)
				}

				audienceMapper, err := authorization.GetAudienceMapperFromConfig(&cfg.Global.Authorization)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate audience mapper: %v.", err), 1)
				}

				s, err := temporal.NewServer(
					temporal.ForServices(services),
					temporal.WithConfig(cfg),
					temporal.WithLogger(logger),
					temporal.InterruptOn(temporal.InterruptCh()),
					temporal.WithAuthorizer(authorizer),
					temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
						return claimMapper
					}),
					temporal.WithAudienceGetter(func(cfg *config.Config) authorization.JWTAudienceMapper {
						return audienceMapper
					}),
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to create server. Error: %v.", err), 1)
				}

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

// helpfulSubcommandFlagError rewrites the urfave/cli "flag provided but not
// defined" error to a more actionable message when the unknown flag turns out
// to be a known global flag passed in the wrong position (after a subcommand
// instead of before it).
//
// The global flags `--root`, `--config`, `--env`, `--zone` and `--config-file`
// must be passed before the subcommand (for example, `temporal-server --env
// docker start`, not `temporal-server start --env docker`). Without this
// rewrite the bare "flag provided but not defined: -e" message is hard to
// debug — see https://github.com/temporalio/temporal/issues/6226.
func helpfulSubcommandFlagError(cCtx *cli.Context, err error, isSubcommand bool) error {
	if err == nil {
		return nil
	}
	out := err
	if rewritten, ok := rewriteAsGlobalFlagPlacementError(cCtx, err); ok {
		out = rewritten
	}
	// Mirror urfave/cli's default "Incorrect Usage: ..." behavior, which is
	// otherwise suppressed when OnUsageError is set. The production `main`
	// discards the returned error, so without this print the user would see
	// nothing.
	if cCtx != nil && cCtx.App != nil && cCtx.App.Writer != nil {
		_, _ = fmt.Fprintf(cCtx.App.Writer, "Incorrect Usage: %s\n", out.Error())
	}
	return out
}

// rewriteAsGlobalFlagPlacementError returns a friendlier error and true when
// the urfave/cli "flag provided but not defined: -X" error refers to a flag
// that is actually defined at the global (app) level. Returns the input error
// and false in all other cases.
func rewriteAsGlobalFlagPlacementError(cCtx *cli.Context, err error) (error, bool) {
	const prefix = "flag provided but not defined: -"
	msg := err.Error()
	if !strings.HasPrefix(msg, prefix) {
		return err, false
	}
	name := strings.TrimLeft(strings.TrimPrefix(msg, prefix), "-")
	if cCtx == nil || cCtx.App == nil {
		return err, false
	}
	var canonical string
	for _, fl := range cCtx.App.Flags {
		for _, n := range fl.Names() {
			if n == name {
				canonical = fl.Names()[0]
				break
			}
		}
		if canonical != "" {
			break
		}
	}
	if canonical == "" {
		return err, false
	}
	subcommand := "the subcommand"
	example := "<subcommand>"
	if cCtx.Command != nil && cCtx.Command.Name != "" {
		subcommand = "`" + cCtx.Command.Name + "`"
		example = cCtx.Command.Name
	}
	return fmt.Errorf(
		"--%s is a global flag and must be passed before %s "+
			"(for example, `%s --%s <value> %s ...`)",
		canonical, subcommand, cCtx.App.Name, canonical, example,
	), true
}
