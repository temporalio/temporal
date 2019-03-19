// Copyright (c) 2018 Uber Technologies, Inc.
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

package cluster

import (
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	// TestCurrentClusterInitialFailoverVersion is initial failover version for current cluster
	TestCurrentClusterInitialFailoverVersion = int64(0)
	// TestAlternativeClusterInitialFailoverVersion is initial failover version for alternative cluster
	TestAlternativeClusterInitialFailoverVersion = int64(1)
	// TestFailoverVersionIncrement is failover version increment used for test
	TestFailoverVersionIncrement = int64(10)
	// TestCurrentClusterName is current cluster used for test
	TestCurrentClusterName = "active"
	// TestAlternativeClusterName is alternative cluster used for test
	TestAlternativeClusterName = "standby"
	// TestCurrentClusterFrontendAddress is the ip port address of current cluster
	TestCurrentClusterFrontendAddress = "127.0.0.1:7104"
	// TestAlternativeClusterFrontendAddress is the ip port address of alternative cluster
	TestAlternativeClusterFrontendAddress = "127.0.0.1:8104"
)

var (
	// TestAllClusterNames is the all cluster names used for test
	TestAllClusterNames = []string{TestCurrentClusterName, TestAlternativeClusterName}
	// TestAllClusterFailoverVersions is the same as above, juse convinent for test mocking
	TestAllClusterFailoverVersions = map[string]int64{
		TestCurrentClusterName:     TestCurrentClusterInitialFailoverVersion,
		TestAlternativeClusterName: TestAlternativeClusterInitialFailoverVersion,
	}
	// TestAllClusterAddress is the same as above, juse convinent for test mocking
	TestAllClusterAddress = map[string]config.Address{
		TestCurrentClusterName:     config.Address{RPCName: common.FrontendServiceName, RPCAddress: TestCurrentClusterFrontendAddress},
		TestAlternativeClusterName: config.Address{RPCName: common.FrontendServiceName, RPCAddress: TestAlternativeClusterFrontendAddress},
	}

	// TestSingleDCAllClusterNames is the all cluster names used for test
	TestSingleDCAllClusterNames = []string{TestCurrentClusterName}
	// TestSingleDCAllClusterFailoverVersions is the same as above, juse convinent for test mocking
	TestSingleDCAllClusterFailoverVersions = map[string]int64{
		TestCurrentClusterName: TestCurrentClusterInitialFailoverVersion,
	}
	// TestSingleDCAllClusterAddress is the same as above, juse convinent for test mocking
	TestSingleDCAllClusterAddress = map[string]config.Address{
		TestCurrentClusterName: config.Address{RPCName: common.FrontendServiceName, RPCAddress: TestCurrentClusterFrontendAddress},
	}
)

// GetTestClusterMetadata return an cluster metadata instance, which is initialized
func GetTestClusterMetadata(enableGlobalDomain bool, isMasterCluster bool, enableArchival bool) Metadata {
	masterClusterName := TestCurrentClusterName
	if !isMasterCluster {
		masterClusterName = TestAlternativeClusterName
	}

	archivalStatus := "disabled"
	clusterDefaultBucket := ""
	if enableArchival {
		archivalStatus = "enabled"
		clusterDefaultBucket = "default_bucket"
	}
	if enableGlobalDomain {
		return NewMetadata(
			bark.NewNopLogger(),
			&mocks.Client{},
			dynamicconfig.GetBoolPropertyFn(true),
			TestFailoverVersionIncrement,
			masterClusterName,
			TestCurrentClusterName,
			TestAllClusterFailoverVersions,
			TestAllClusterAddress,
			dynamicconfig.GetStringPropertyFn(archivalStatus),
			clusterDefaultBucket,
		)
	}

	return NewMetadata(
		bark.NewNopLogger(),
		&mocks.Client{},
		dynamicconfig.GetBoolPropertyFn(false),
		TestFailoverVersionIncrement,
		TestCurrentClusterName,
		TestCurrentClusterName,
		TestSingleDCAllClusterFailoverVersions,
		TestSingleDCAllClusterAddress,
		dynamicconfig.GetStringPropertyFn(archivalStatus),
		clusterDefaultBucket,
	)
}
