package main

import (
	"os"

	"github.com/temporalio/temporal/cmd/server/temporal"
	_ "github.com/temporalio/temporal/common/persistence/sql/sqlplugin/mysql"    // needed to load mysql plugin
	_ "github.com/temporalio/temporal/common/persistence/sql/sqlplugin/postgres" // needed to load postgres plugin
)

// main entry point for the temporal server
func main() {
	app := temporal.BuildCLI()
	app.Run(os.Args)
}
