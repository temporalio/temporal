//go:build !test_dep

// Package failpointserver serves the FailpointService on a server's gRPC server in test builds, so
// that tests and tools outside the process can configure its failpoints.
package failpointserver

import "go.uber.org/fx"

// Module serves nothing without test hooks.
var Module = fx.Options()

// FanOutModule serves nothing without test hooks.
var FanOutModule = fx.Options()
