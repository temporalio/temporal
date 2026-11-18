// Package parallelsuitelint checks parallelsuite ownership, style and test entrypoints.
//
//	go run go.temporal.io/testx/parallelsuitelint/cmd/parallelsuitelint ./...
//
// TestXxx functions must call parallelsuite.Run or parallelsuite.RunLegacySequential.
// Project wrappers also count if they pass the same test handle and suite
// straight through to one of those runners.
//
// To allow plain TestXxx functions and testify suites, pass a comma-separated
// list of import paths to -allow-standard-tests. For example:
//
//	-allow-standard-tests=example.com/project/unit,example.com/project/legacy/...
//
// This allows them in unit, legacy, and legacy's subpackages. Use ... alone
// to allow them everywhere. Checks on parallelsuite usage still apply.
package parallelsuitelint
