package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/parallelize"
)

func main() {
	if err := parallelize.Main(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
