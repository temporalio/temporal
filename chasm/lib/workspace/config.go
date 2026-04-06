package workspace

import (
	"go.temporal.io/server/common/dynamicconfig"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"workspace.enableStandalone",
		false,
		`Toggles standalone workspace functionality on the server.`,
	)
)

// Config holds workspace CHASM component configuration.
type Config struct {
	Enabled dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// ConfigProvider creates a workspace config from dynamic config.
func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled: Enabled.Get(dc),
	}
}
