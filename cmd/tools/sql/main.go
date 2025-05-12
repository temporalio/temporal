package main

import (
	"os"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"      // needed to load mysql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // needed to load postgresql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"     // needed to load sqlite plugin
	"go.temporal.io/server/tools/sql"
)

func main() {
	if err := sql.RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}
