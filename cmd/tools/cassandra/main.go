package main

import (
	"github.com/uber/cadence/tools/cassandra"
	"os"
)

func main() {
	cassandra.RunTool(os.Args)
}
