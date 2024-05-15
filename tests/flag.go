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

package tests

import (
	"flag"

	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

// TestFlags contains the feature flags for functional tests
var TestFlags struct {
	FrontendAddr             string
	FrontendHTTPAddr         string
	PersistenceType          string
	PersistenceDriver        string
	TestClusterConfigFile    string
	FaultInjectionConfigFile string
}

func init() {
	flag.StringVar(&TestFlags.FrontendAddr, "frontendAddress", "", "host:port for temporal frontend service")
	flag.StringVar(&TestFlags.FrontendHTTPAddr, "frontendHttpAddress", "", "host:port for temporal frontend HTTP service (only applies when frontendAddress set)")
	flag.StringVar(&TestFlags.PersistenceType, "persistenceType", "sql", "type of persistence - [nosql or sql]")
	flag.StringVar(&TestFlags.PersistenceDriver, "persistenceDriver", "sqlite", "driver of nosql / sql- [cassandra, mysql, postgresql, sqlite]")
	flag.StringVar(&TestFlags.TestClusterConfigFile, "TestClusterConfigFile", "", "test cluster config file location")
	flag.StringVar(&TestFlags.FaultInjectionConfigFile, "FaultInjectionConfigFile", "", "fault injection config file location")
}

func UsingSQLAdvancedVisibility() bool {
	switch TestFlags.PersistenceDriver {
	case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
		return true
	default:
		return false
	}
}
