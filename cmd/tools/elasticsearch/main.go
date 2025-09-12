package main

import (
	"os"

	"go.temporal.io/server/tools/elasticsearch"
)

func main() {
	if err := elasticsearch.RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}
