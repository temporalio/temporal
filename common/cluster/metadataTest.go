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

package cluster

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/service/config"
)

const (
	// TestCurrentClusterInitialFailoverVersion is initial failover version for current cluster
	TestCurrentClusterInitialFailoverVersion = int64(1)
	// TestAlternativeClusterInitialFailoverVersion is initial failover version for alternative cluster
	TestAlternativeClusterInitialFailoverVersion = int64(2)
	// TestFailoverVersionIncrement is failover version increment used for test
	TestFailoverVersionIncrement = int64(10)
	// TestCurrentClusterName is current cluster used for test
	TestCurrentClusterName = "active"
	// TestAlternativeClusterName is alternative cluster used for test
	TestAlternativeClusterName = "standby"
	// TestCurrentClusterFrontendAddress is the ip port address of current cluster
	TestCurrentClusterFrontendAddress = "127.0.0.1:7134"
	// TestAlternativeClusterFrontendAddress is the ip port address of alternative cluster
	TestAlternativeClusterFrontendAddress = "127.0.0.1:8134"
)

var (
	// TestAllClusterNames is the all cluster names used for test
	TestAllClusterNames = []string{TestCurrentClusterName, TestAlternativeClusterName}
	// TestAllClusterInfo is the same as above, just convenient for test mocking
	TestAllClusterInfo = map[string]config.ClusterInformation{
		TestCurrentClusterName: config.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCName:                common.FrontendServiceName,
			RPCAddress:             TestCurrentClusterFrontendAddress,
		},
		TestAlternativeClusterName: config.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: TestAlternativeClusterInitialFailoverVersion,
			RPCName:                common.FrontendServiceName,
			RPCAddress:             TestAlternativeClusterFrontendAddress,
		},
	}

	// TestSingleDCAllClusterNames is the all cluster names used for test
	TestSingleDCAllClusterNames = []string{TestCurrentClusterName}
	// TestSingleDCClusterInfo is the same as above, just convenient for test mocking
	TestSingleDCClusterInfo = map[string]config.ClusterInformation{
		TestCurrentClusterName: config.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCName:                common.FrontendServiceName,
			RPCAddress:             TestCurrentClusterFrontendAddress,
		},
	}
)

// GetTestClusterMetadata return an cluster metadata instance, which is initialized
func GetTestClusterMetadata(enableGlobalNamespace bool, isMasterCluster bool) Metadata {
	masterClusterName := TestCurrentClusterName
	if !isMasterCluster {
		masterClusterName = TestAlternativeClusterName
	}

	if enableGlobalNamespace {
		return NewMetadata(
			log.NewNoopLogger(),
			true,
			TestFailoverVersionIncrement,
			masterClusterName,
			TestCurrentClusterName,
			TestAllClusterInfo,
		)
	}

	return NewMetadata(
		log.NewNoopLogger(),
		false,
		TestFailoverVersionIncrement,
		TestCurrentClusterName,
		TestCurrentClusterName,
		TestSingleDCClusterInfo,
	)
}
