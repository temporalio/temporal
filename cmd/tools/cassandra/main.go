package main

import (
	"os"

	"go.temporal.io/server/tools/cassandra"
)

func main() {
	if err := cassandra.RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}
