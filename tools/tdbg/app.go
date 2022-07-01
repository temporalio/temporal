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

package tdbg

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/headers"

	"github.com/temporalio/tctl-kit/pkg/color"
)

// SetFactory is used to set the ClientFactory global
func SetFactory(factory ClientFactory) {
	cFactory = factory
}

// NewCliApp instantiates a new instance of the CLI application.
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "tdbg"
	app.Usage = "A command-line tool for Temporal server debugging"
	app.Version = headers.ServerVersion
	app.Suggest = true
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
			Name:  color.FlagColor,
			Usage: fmt.Sprintf("when to use color: %v, %v, %v.", color.Auto, color.Always, color.Never),
			Value: string(color.Auto),
		},
	}
	app.Commands = commands
	app.ExitErrHandler = handleError

	// set builder if not customized
	if cFactory == nil {
		SetFactory(NewClientFactory())
	}

	return app
}

func handleError(c *cli.Context, err error) {
	if err == nil {
		return
	}

	fmt.Fprintf(os.Stderr, "%s %+v\n", color.Red(c, "Error:"), err)
	if os.Getenv(showErrorStackEnv) != `` {
		fmt.Fprintln(os.Stderr, color.Magenta(c, "Stack trace:"))
		debug.PrintStack()
	} else {
		fmt.Fprintf(os.Stderr, "('export %s=1' to see stack traces)\n", showErrorStackEnv)
	}

	cli.OsExiter(1)
}
