// Copyright (c) 2019 Uber Technologies, Inc.
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

package host

import "flag"

// TestFlags contains the feature flags for integration tests
var TestFlags struct {
	FrontendAddr          string
	FrontendAddrGRPC      string
	PersistenceType       string
	SQLPluginName         string
	TestClusterConfigFile string
}

func init() {
	flag.StringVar(&TestFlags.FrontendAddr, "frontendAddress", "", "host:port for temporal frontend service")
	flag.StringVar(&TestFlags.FrontendAddrGRPC, "frontendAddressGRPC", "", "host:port for temporal frontend gRPC service")
	flag.StringVar(&TestFlags.PersistenceType, "persistenceType", "cassandra", "type of persistence store - [cassandra or sql]")
	flag.StringVar(&TestFlags.SQLPluginName, "sqlPluginName", "mysql", "type of sql store - [mysql]")
	flag.StringVar(&TestFlags.TestClusterConfigFile, "TestClusterConfigFile", "", "test cluster config file location")
}
