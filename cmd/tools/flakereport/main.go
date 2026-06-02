package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/flakereport"
)

func main() {
	if err := flakereport.NewCliApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error running flakereport: %v\n", err)
		os.Exit(1)
	}
}
