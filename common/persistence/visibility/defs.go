package visibility

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
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
	case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
		// Advanced visibility with SQL DB don't support list of values
		return dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	default:
		// Otherwise (ES), check dynamic config
		return allowList
	}
}
