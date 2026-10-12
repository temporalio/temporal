package visibility

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// SecondaryVisibilityWritingModeOff means do not write to advanced visibility store
	SecondaryVisibilityWritingModeOff = "off"
	// SecondaryVisibilityWritingModeOn means only write to advanced visibility store
	SecondaryVisibilityWritingModeOn = "on"
	// SecondaryVisibilityWritingModeDual means write to both normal visibility and advanced visibility store
	SecondaryVisibilityWritingModeDual = "dual"
)

func AllowListForValidation(
	storeNames []string,
	allowList dynamicconfig.BoolPropertyFnWithNamespaceFilter,
) dynamicconfig.BoolPropertyFnWithNamespaceFilter {
	if len(storeNames) == 0 {
		return dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	}

	switch storeNames[0] {
	case sqlplugin.PluginNameMySQL, sqlplugin.PluginNamePostgreSQL, sqlplugin.PluginNamePostgreSQLPGX, sqlplugin.PluginNameSQLite:
		// Advanced visibility with SQL DB don't support list of values
		return dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	default:
		// Otherwise (ES), check dynamic config
		return allowList
	}
}
