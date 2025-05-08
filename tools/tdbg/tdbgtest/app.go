package tdbgtest

import (
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/tools/tdbg"
)

// NewCliApp is a wrapper around [tdbg.NewCliApp] that sets the [cli.App.ExitErrHandler] to a no-op function.
func NewCliApp(opts ...tdbg.Option) *cli.App {
	app := tdbg.NewCliApp(opts...)
	app.ExitErrHandler = func(context *cli.Context, err error) {}
	return app
}
