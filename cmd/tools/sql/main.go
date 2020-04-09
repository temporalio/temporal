package main

import (
	"os"

	_ "github.com/temporalio/temporal/common/persistence/sql/sqlplugin/mysql"    // needed to load mysql plugin
	_ "github.com/temporalio/temporal/common/persistence/sql/sqlplugin/postgres" // needed to load postgres plugin
	"github.com/temporalio/temporal/tools/sql"
)

func main() {
	sql.RunTool(os.Args) //nolint:errcheck
}
