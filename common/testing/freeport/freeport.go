// Package freeport is kept until all importers have moved to go.temporal.io/testx/freeport.
//
// Deprecated: Use go.temporal.io/testx/freeport instead.
package freeport

import testxfreeport "go.temporal.io/testx/freeport"

// MustGetFreePort returns a TCP port that is available to listen on.
//
// Deprecated: Use [testxfreeport.MustGetFreePort] instead.
func MustGetFreePort() int {
	return testxfreeport.MustGetFreePort()
}
