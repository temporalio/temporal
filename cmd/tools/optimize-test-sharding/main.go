package main

import (
	"fmt"
	"os"

	optimizetestsharding "go.temporal.io/server/tools/optimize-test-sharding"
)

func main() {
	salt, err := optimizetestsharding.Main()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	fmt.Println(salt)
}
