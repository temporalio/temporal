package main

import (
	"context"
	"os"

	"go.temporal.io/server/tools/tdbg"
)

func main() {
	// check env for ENABLE_TDBG_V2.
	if os.Getenv("ENABLE_TDBG_V2") == "true" {
		tdbg.Execute(context.Background(), tdbg.CommandOptions{})
	} else {
		app := tdbg.NewCliApp()
		if err := app.Run(os.Args); err != nil {
			os.Exit(1)
		}
	}

}
