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
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

const (
	// AdvancedVisibilityWritingModeOff means do not write to advanced visibility store
	AdvancedVisibilityWritingModeOff = "off"
	// AdvancedVisibilityWritingModeOn means only write to advanced visibility store
	AdvancedVisibilityWritingModeOn = "on"
	// AdvancedVisibilityWritingModeDual means write to both normal visibility and advanced visibility store
	AdvancedVisibilityWritingModeDual = "dual"
)

// DefaultAdvancedVisibilityWritingMode returns default advancedVisibilityWritingMode based on whether related config exists in static config file.
func DefaultAdvancedVisibilityWritingMode(advancedVisibilityConfigExist bool) string {
	if advancedVisibilityConfigExist {
		return AdvancedVisibilityWritingModeOn
	}
	return AdvancedVisibilityWritingModeOff
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
