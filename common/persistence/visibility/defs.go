// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

// DefaultAdvancedVisibilityWritingMode returns default advancedVisibilityWritingMode based on whether related config exists in static config file.
func DefaultAdvancedVisibilityWritingMode(advancedVisibilityConfigExist bool) string {
	if advancedVisibilityConfigExist {
		return SecondaryVisibilityWritingModeOn
	}
	return SecondaryVisibilityWritingModeOff
}

//nolint:revive
func GetVisibilityPersistenceMaxReadQPS(
	dc *dynamicconfig.Collection,
	advancedVisibilityStoreConfigExists bool,
) dynamicconfig.IntPropertyFn {
	if dc.HasKey(dynamicconfig.VisibilityPersistenceMaxReadQPS) {
		return dc.GetIntProperty(dynamicconfig.VisibilityPersistenceMaxReadQPS, 9000)
	}
	if advancedVisibilityStoreConfigExists {
		return dc.GetIntProperty(dynamicconfig.AdvancedVisibilityPersistenceMaxReadQPS, 9000)
	}
	return dc.GetIntProperty(dynamicconfig.StandardVisibilityPersistenceMaxReadQPS, 9000)
}

//nolint:revive
func GetVisibilityPersistenceMaxWriteQPS(
	dc *dynamicconfig.Collection,
	advancedVisibilityStoreConfigExists bool,
) dynamicconfig.IntPropertyFn {
	if dc.HasKey(dynamicconfig.VisibilityPersistenceMaxWriteQPS) {
		return dc.GetIntProperty(dynamicconfig.VisibilityPersistenceMaxWriteQPS, 9000)
	}
	if advancedVisibilityStoreConfigExists {
		return dc.GetIntProperty(dynamicconfig.AdvancedVisibilityPersistenceMaxWriteQPS, 9000)
	}
	return dc.GetIntProperty(dynamicconfig.StandardVisibilityPersistenceMaxWriteQPS, 9000)
}

//nolint:revive
func GetEnableReadFromSecondaryVisibilityConfig(
	dc *dynamicconfig.Collection,
	visibilityStoreConfigExists bool,
	advancedVisibilityStoreConfigExists bool,
) dynamicconfig.BoolPropertyFnWithNamespaceFilter {
	if dc.HasKey(dynamicconfig.EnableReadFromSecondaryVisibility) {
		return dc.GetBoolPropertyFnWithNamespaceFilter(
			dynamicconfig.EnableReadFromSecondaryVisibility,
			false,
		)
	}
	if visibilityStoreConfigExists && advancedVisibilityStoreConfigExists {
		return dc.GetBoolPropertyFnWithNamespaceFilter(
			dynamicconfig.EnableReadVisibilityFromES,
			advancedVisibilityStoreConfigExists,
		)
	}
	if advancedVisibilityStoreConfigExists {
		return dc.GetBoolPropertyFnWithNamespaceFilter(
			dynamicconfig.EnableReadFromSecondaryAdvancedVisibility,
			false,
		)
	}
	return dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
}

//nolint:revive
func GetSecondaryVisibilityWritingModeConfig(
	dc *dynamicconfig.Collection,
	visibilityStoreConfigExists bool,
	advancedVisibilityStoreConfigExists bool,
) dynamicconfig.StringPropertyFn {
	if dc.HasKey(dynamicconfig.SecondaryVisibilityWritingMode) {
		return dc.GetStringProperty(
			dynamicconfig.SecondaryVisibilityWritingMode,
			SecondaryVisibilityWritingModeOff,
		)
	}
	if visibilityStoreConfigExists && advancedVisibilityStoreConfigExists {
		return dc.GetStringProperty(
			dynamicconfig.AdvancedVisibilityWritingMode,
			DefaultAdvancedVisibilityWritingMode(advancedVisibilityStoreConfigExists),
		)
	}
	if advancedVisibilityStoreConfigExists {
		enableWriteToSecondaryAdvancedVisibility := dc.GetBoolProperty(
			dynamicconfig.EnableWriteToSecondaryAdvancedVisibility,
			false,
		)
		if enableWriteToSecondaryAdvancedVisibility() {
			return dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeDual)
		}
	}
	return dynamicconfig.GetStringPropertyFn(SecondaryVisibilityWritingModeOff)
}

func AllowListForValidation(storeNames []string) bool {
	if len(storeNames) == 0 {
		return false
	}

	if len(storeNames) > 1 {
		// If more than one store is configured then it means that dual visibility is enabled.
		// Dual visibility is used for migration to advanced, don't allow list of values because it will be removed soon.
		return false
	}

	switch storeNames[0] {
	case mysql.PluginNameV8, postgresql.PluginNameV12, sqlite.PluginName:
		// Advanced visibility with SQL DB don't support list of values
		return false
	default:
		// Otherwise, enable for backward compatibility.
		return true
	}
}
