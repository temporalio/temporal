// Package lintconditions checks the size of boolean expression chains.
//
// Run lintconditions from the module to check:
//
//	go run go.temporal.io/testx/lintconditions/cmd/lintconditions ./...
//
// The -max-terms flag sets the limit (default 3) and must be at least 1:
//
//	go run go.temporal.io/testx/lintconditions/cmd/lintconditions -max-terms=4 ./...
package lintconditions
