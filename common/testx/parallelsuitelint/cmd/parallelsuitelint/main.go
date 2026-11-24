// Command parallelsuitelint checks suite ownership and test conventions.
package main

import (
	"go.temporal.io/testx/parallelsuitelint"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(parallelsuitelint.Analyzer)
}
