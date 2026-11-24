// Package parallelsuitelint checks parallelsuite conventions.
//
//	go run go.temporal.io/testx/parallelsuitelint/cmd/parallelsuitelint ./...
//
// The analyzer recognizes await wrappers that export T as an alias of
// go.temporal.io/testx/await.T and provide functions matching the corresponding
// testx await signatures.
package parallelsuitelint
