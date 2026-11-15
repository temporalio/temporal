// Package parallelsuitelint checks parallelsuite ownership, style and test entrypoints.
//
//	go run go.temporal.io/testx/parallelsuitelint/cmd/parallelsuitelint ./...
//
// -allow-standard-tests accepts comma-separated import paths, path/... for subtrees,
// or ... for all packages. It disables only the test-adoption policy.
// -runner-packages trusts additional packages that wrap the shared suite runner.
package parallelsuitelint
