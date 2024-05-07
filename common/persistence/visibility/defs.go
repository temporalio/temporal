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

//nolint:revive
func GetVisibilityPersistenceMaxReadQPS(
	dc *dynamicconfig.Collection,
) dynamicconfig.IntPropertyFn {
	return dynamicconfig.VisibilityPersistenceMaxReadQPS.Get(dc)
}

//nolint:revive
func GetVisibilityPersistenceMaxWriteQPS(
	dc *dynamicconfig.Collection,
) dynamicconfig.IntPropertyFn {
	return dynamicconfig.VisibilityPersistenceMaxWriteQPS.Get(dc)
}

//nolint:revive
func GetEnableReadFromSecondaryVisibilityConfig(
	dc *dynamicconfig.Collection,
) dynamicconfig.BoolPropertyFnWithNamespaceFilter {
	return dynamicconfig.EnableReadFromSecondaryVisibility.Get(dc)
}

//nolint:revive
func GetSecondaryVisibilityWritingModeConfig(
	dc *dynamicconfig.Collection,
) dynamicconfig.StringPropertyFn {
	return dynamicconfig.SecondaryVisibilityWritingMode.Get(dc)
}

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
