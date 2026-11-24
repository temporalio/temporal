//go:build !test_dep

package failpointrules

import "go.uber.org/fx"

// Module configures nothing without test hooks.
var Module = fx.Options()

// ReleaseModule releases nothing without test hooks.
var ReleaseModule = fx.Options()
