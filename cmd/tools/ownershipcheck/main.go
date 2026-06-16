// Command ownershipcheck runs the ownershipcheck analyzer as a standalone
// vet-style tool: go run ./cmd/tools/ownershipcheck ./...
package main

import (
	"go.temporal.io/server/tools/ownershipcheck"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(ownershipcheck.Analyzer)
}
