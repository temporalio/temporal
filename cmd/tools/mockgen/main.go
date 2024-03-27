package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/mockgen"
)

func main() {
	if err := mockgen.Run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
