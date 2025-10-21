package tdbg

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// Params which are customizable for the CLI application.
	Params struct {
		// ClientFactory creates Temporal service clients for tdbg to use.
		ClientFactory ClientFactory
		// TaskCategoryRegistry is used to determine which task categories are available for tdbg to use.
		TaskCategoryRegistry tasks.TaskCategoryRegistry
		// Writer is used to write output from tdbg. The default is os.Stdout.
		Writer io.Writer
		// ErrWriter is used to write errors from tdbg. The default is os.Stderr.
		ErrWriter io.Writer
		// TaskBlobEncoder is needed for custom task serialization. The default uses PredefinedTaskBlobDeserializer.
		TaskBlobEncoder TaskBlobEncoder
	}
	// Option modifies the Params for tdbg.
	Option func(params *Params)
)

// NewCliApp instantiates a new instance of the CLI application.
func NewCliApp(opts ...Option) *cli.App {
	params := Params{
		ClientFactory:        NewClientFactory(),
		TaskCategoryRegistry: tasks.NewDefaultTaskCategoryRegistry(),
		Writer:               os.Stdout,
		ErrWriter:            os.Stderr,
		TaskBlobEncoder:      NewProtoTaskBlobEncoder(NewPredefinedTaskBlobDeserializer()),
	}
	for _, opt := range opts {
		opt(&params)
	}
	app := cli.NewApp()
	app.Name = "tdbg"
	app.Usage = "A command-line tool for Temporal server debugging"
	app.Version = headers.ServerVersion
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    FlagAddress,
			Value:   "",
			Usage:   "host:port for Temporal frontend service",
			EnvVars: []string{"TEMPORAL_CLI_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    FlagNamespace,
			Aliases: FlagNamespaceAlias,
			Value:   "default",
			Usage:   "Temporal workflow namespace",
			EnvVars: []string{"TEMPORAL_CLI_NAMESPACE"},
		},
		&cli.IntFlag{
			Name:    FlagContextTimeout,
			Aliases: FlagContextTimeoutAlias,
			Value:   defaultContextTimeoutInSeconds,
			Usage:   "Optional timeout for context of RPC call in seconds",
			EnvVars: []string{"TEMPORAL_CONTEXT_TIMEOUT"},
		},
		&cli.BoolFlag{
			Name:  FlagYes,
			Usage: "Automatically confirm all prompts",
		},
		&cli.StringFlag{
			Name:    FlagTLSCertPath,
			Value:   "",
			Usage:   "Path to x509 certificate",
			EnvVars: []string{"TEMPORAL_CLI_TLS_CERT"},
		},
		&cli.StringFlag{
			Name:    FlagTLSKeyPath,
			Value:   "",
			Usage:   "Path to private key",
			EnvVars: []string{"TEMPORAL_CLI_TLS_KEY"},
		},
		&cli.StringFlag{
			Name:    FlagTLSCaPath,
			Value:   "",
			Usage:   "Path to server CA certificate",
			EnvVars: []string{"TEMPORAL_CLI_TLS_CA"},
		},
		&cli.BoolFlag{
			Name:    FlagTLSDisableHostVerification,
			Usage:   "Disable tls host name verification (tls must be enabled)",
			EnvVars: []string{"TEMPORAL_CLI_TLS_DISABLE_HOST_VERIFICATION"},
		},
		&cli.StringFlag{
			Name:    FlagTLSServerName,
			Value:   "",
			Usage:   "Override for target server name",
			EnvVars: []string{"TEMPORAL_CLI_TLS_SERVER_NAME"},
		},
		&cli.StringFlag{
			Name:  "color",
			Usage: fmt.Sprintf("When to use color: %v, %v, %v.", "auto", "always", "never"),
			Value: "auto",
		},
	}
	prompterFactory := NewPrompterFactory()
	app.Before = func(ctx *cli.Context) error {
		colorFlag := ctx.String("color")
		switch colorFlag {
		case "always":
			color.NoColor = false
		case "never":
			color.NoColor = true
		default:
			// fatih/color will inspect the enviroment and terminal and set a reasonable default.
		}
		return nil
	}
	app.Commands = getCommands(
		params.ClientFactory,
		NewDLQServiceProvider(
			params.ClientFactory,
			params.TaskBlobEncoder,
			params.TaskCategoryRegistry,
			params.Writer,
			prompterFactory,
		),
		params.TaskCategoryRegistry,
		prompterFactory,
		params.TaskBlobEncoder,
	)
	app.ExitErrHandler = handleError
	app.Writer = params.Writer
	app.ErrWriter = params.ErrWriter

	return app
}

func handleError(c *cli.Context, err error) {
	if err == nil {
		return
	}

	_, _ = fmt.Fprintf(c.App.ErrWriter, "%s %+v\n", color.RedString("Error:"), err)
	if os.Getenv(showErrorStackEnv) != `` {
		_, _ = fmt.Fprintln(c.App.ErrWriter, color.MagentaString("Stack trace:"))
		debug.PrintStack()
	} else {
		_, _ = fmt.Fprintf(c.App.ErrWriter, "('export %s=1' to see stack traces)\n", showErrorStackEnv)
	}

	cli.OsExiter(1)
}
