package main

import (
	"os"

	"github.com/temporalio/temporal/tools/cassandra"
)

func main() {
	cassandra.RunTool(os.Args) //nolint:errcheck
}
