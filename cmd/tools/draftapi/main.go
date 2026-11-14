package main

import (
	"go.temporal.io/server/tools/draftapi"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() {
	unitchecker.Main(draftapi.Analyzer)
}
