package main

import (
	"os"

	"github.com/temporalio/temporal/tools/cli"
)

// Start using this CLI tool with command
// See temporal/tools/cli/README.md for usage
func main() {
	app := cli.NewCliApp()
	app.Run(os.Args)
}
