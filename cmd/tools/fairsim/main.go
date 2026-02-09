package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/fairsim"
)

func main() {
	if err := fairsim.RunTool(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
