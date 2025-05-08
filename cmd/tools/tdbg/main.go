package main

import (
	"os"

	"go.temporal.io/server/tools/tdbg"
)

func main() {
	app := tdbg.NewCliApp()
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
