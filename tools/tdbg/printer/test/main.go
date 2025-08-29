package main

import (
	"os"

	"go.temporal.io/server/tools/tdbg/printer"
)

// This main function is used to test that the printer package don't panic if
// the CLI is run without a STDOUT. This is a tricky thing to validate, as it
// must be done in a subprocess and as `go test` has its own internal fix for
// improper STDOUT. This was fixed in Go 1.22, but keeping this here as a
// regression test. See https://github.com/temporalio/cli/issues/544.
func main() {
	p := &printer.Printer{
		Output: os.Stdout,
		JSON:   false,
	}
	p.Println("Test writing to stdout using Printer")
	os.Exit(0)
}
