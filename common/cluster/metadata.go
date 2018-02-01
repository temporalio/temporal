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

type (
	// Metadata provides information about clusters
	Metadata interface {
		GetNextFailoverVersion(int64) int64
		// GetCurrentClusterName return the current cluster name
		GetCurrentClusterName() string
		// GetAllClusterNames return the all cluster names, as a set
		GetAllClusterNames() map[string]bool
	}

	MetadataImpl struct {
		// initialFailoverVersion is the initial failover version
		initialFailoverVersion int64
		// failoverVersionIncrement is the increment of each cluster failover version
		failoverVersionIncrement int64
		// currentClusterName is the name of the current cluster
		currentClusterName string
		// clusterNames contains all cluster names, as a set
		clusterNames map[string]bool
	}
)

// NewMetadata create a new instance of Metadata
func NewMetadata(initialFailoverVersion int64, failoverVersionIncrement int64,
	currentClusterName string, clusterNames []string) Metadata {

	if initialFailoverVersion < 0 {
		panic("Bad initial failover version")
	} else if failoverVersionIncrement <= initialFailoverVersion {
		panic("Bad failover version increment")
	} else if len(currentClusterName) == 0 {
		panic("Current cluster name is empty")
	} else if len(clusterNames) == 0 {
		panic("Total number of all cluster names is 0")
	}

	clusters := make(map[string]bool)
	for _, clusterName := range clusterNames {
		if len(clusterName) == 0 {
			panic("Cluster name in all cluster names is empty")
		}
		clusters[clusterName] = true
	}
	if _, ok := clusters[currentClusterName]; !ok {
		panic("Current cluster is not specified in all cluster names")
	}

	return &MetadataImpl{
		initialFailoverVersion:   initialFailoverVersion,
		failoverVersionIncrement: failoverVersionIncrement,
		currentClusterName:       currentClusterName,
		clusterNames:             clusters,
	}
}

// GetNextFailoverVersion return the next failover version based on input
func (metadata *MetadataImpl) GetNextFailoverVersion(currentFailoverVersion int64) int64 {
	failoverVersion := currentFailoverVersion/metadata.failoverVersionIncrement + metadata.initialFailoverVersion
	if failoverVersion < currentFailoverVersion {
		return failoverVersion + metadata.failoverVersionIncrement
	}
	return failoverVersion
}

// GetCurrentClusterName return the current cluster name
func (metadata *MetadataImpl) GetCurrentClusterName() string {
	return metadata.currentClusterName
}

// GetAllClusterNames return the all cluster names
func (metadata *MetadataImpl) GetAllClusterNames() map[string]bool {
	return metadata.clusterNames
}
