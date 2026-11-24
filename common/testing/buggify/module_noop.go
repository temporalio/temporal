//go:build !test_dep

package buggify

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

// Module configures nothing without test hooks.
var Module = fx.Options()

// WrapDynamicConfigClient returns client: builds without test hooks never randomize settings.
func WrapDynamicConfigClient(client dynamicconfig.Client, _ log.Logger) (dynamicconfig.Client, Startup, error) {
	return client, Startup{}, nil
}
