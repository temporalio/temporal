package main

import (
	"os"

	"go.temporal.io/server/tools/mutationtest"
)

func main() {
	os.Exit(mutationtest.Main())
}
