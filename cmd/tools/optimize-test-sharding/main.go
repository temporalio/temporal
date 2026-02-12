package main

import (
	"fmt"
	"os"

	optimizetestsharding "go.temporal.io/server/tools/optimize-test-sharding"
)

func main() {
	if err := optimizetestsharding.Main(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
