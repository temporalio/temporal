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

const (
	// TestInitialFailoverVersion is initial failover version used for test
	TestInitialFailoverVersion = int64(0)
	// TestFailoverVersionIncrement is failover version increment used for test
	TestFailoverVersionIncrement = int64(10)
	// TestCurrentClusterName is current cluster used for test
	TestCurrentClusterName = "current-cluster"
	// TestAlternativeClusterName is alternative cluster used for test
	TestAlternativeClusterName = "alternative-cluster"
)

var (
	// TestAllClusterNames is the all cluster names used for test
	TestAllClusterNames = []string{TestCurrentClusterName, TestAlternativeClusterName}
	// TestAllClusterNamesMap is the same as above, juse convinent for test mocking
	TestAllClusterNamesMap = map[string]bool{
		TestCurrentClusterName:     true,
		TestAlternativeClusterName: true,
	}
)

// GetTestClusterMetadata return an cluster metadata instance, which is initialized
func GetTestClusterMetadata(enableGlobalDomain bool, isMasterCluster bool) Metadata {
	masterClusterName := TestCurrentClusterName
	if !isMasterCluster {
		masterClusterName = TestAlternativeClusterName
	}
	return NewMetadata(
		enableGlobalDomain,
		TestInitialFailoverVersion,
		TestFailoverVersionIncrement,
		masterClusterName,
		TestCurrentClusterName,
		TestAllClusterNames,
	)
}
