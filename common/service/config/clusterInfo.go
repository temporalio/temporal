// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import "fmt"

// ToClusterMetadata convert deprecated ClustersInfo to ClusterMetadata
// Deprecated: pleasee use ClusterMetadata
func (c ClustersInfo) ToClusterMetadata() ClusterMetadata {
	clusterMetadata := ClusterMetadata{}
	clusterMetadata.EnableGlobalDomain = c.EnableGlobalDomain
	clusterMetadata.FailoverVersionIncrement = c.FailoverVersionIncrement
	clusterMetadata.MasterClusterName = c.MasterClusterName
	clusterMetadata.CurrentClusterName = c.CurrentClusterName
	clusterMetadata.ClusterInformation = map[string]ClusterInformation{}
	for k, v := range c.ClusterInitialFailoverVersions {
		address, ok := c.ClusterAddress[k]
		if !ok {
			panic(fmt.Sprintf("unable to find address for cluster %v", k))
		}

		clusterInfo := ClusterInformation{}
		clusterInfo.Enabled = true
		clusterInfo.InitialFailoverVersion = v
		clusterInfo.RPCName = address.RPCName
		clusterInfo.RPCAddress = address.RPCAddress
		clusterMetadata.ClusterInformation[k] = clusterInfo
	}

	return clusterMetadata
}
