//go:build test_dep

package tests

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
)

// Retention sets the PayloadStore archetype retention via chasm.WithRetention, so functional tests
// can exercise archetype retention. The default of 0 means no override.
var Retention = dynamicconfig.NewNamespaceDurationSetting(
	"chasm.tests.payloadStoreRetention",
	0,
	`Retention overrides the namespace retention for PayloadStore executions. Test only.`,
)

var Module = fx.Module(
	"chasm.lib.tests",
	// Each cluster registers its own library instance so the retention reads that cluster's
	// dynamic config.
	fx.Invoke(func(registry *chasm.Registry, dc *dynamicconfig.Collection) error {
		return registry.Register(&library{retention: Retention.Get(dc)})
	}),
)
