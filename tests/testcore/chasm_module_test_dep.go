//go:build test_dep

package testcore

import (
	"go.temporal.io/server/chasm"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.uber.org/fx"
)

var chasmModule = fx.Options(
	chasm.Module,
	chasmtests.Module,
)
