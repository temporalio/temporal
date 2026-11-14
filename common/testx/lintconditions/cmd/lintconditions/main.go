// Command lintconditions checks boolean expression chains.
package main

import (
	"go.temporal.io/testx/lintconditions"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(lintconditions.Analyzer)
}
