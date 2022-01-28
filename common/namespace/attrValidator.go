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

package namespace

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
)

type (
	// AttrValidatorImpl is namespace attr validator
	AttrValidatorImpl struct {
		clusterMetadata cluster.Metadata
	}
)

// newAttrValidator create a new namespace attr validator
func newAttrValidator(
	clusterMetadata cluster.Metadata,
) *AttrValidatorImpl {

	return &AttrValidatorImpl{
		clusterMetadata: clusterMetadata,
	}
}

func (d *AttrValidatorImpl) validateNamespaceConfig(config *persistencespb.NamespaceConfig) error {
	if config.HistoryArchivalState == enumspb.ARCHIVAL_STATE_ENABLED && len(config.HistoryArchivalUri) == 0 {
		return errInvalidArchivalConfig
	}
	if config.VisibilityArchivalState == enumspb.ARCHIVAL_STATE_ENABLED && len(config.VisibilityArchivalUri) == 0 {
		return errInvalidArchivalConfig
	}
	return nil
}

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigForLocalNamespace(
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) error {

	activeCluster := replicationConfig.ActiveClusterName
	clusters := replicationConfig.Clusters

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterName := range clusters {
		if err := d.validateClusterName(clusterName); err != nil {
			return err
		}
	}

	if activeCluster != d.clusterMetadata.GetCurrentClusterName() {
		return serviceerror.NewInvalidArgument("Invalid local namespace active cluster")
	}

	if len(clusters) != 1 || clusters[0] != activeCluster {
		return serviceerror.NewInvalidArgument("Invalid local namespace clusters")
	}

	return nil
}

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigForGlobalNamespace(
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) error {

	activeCluster := replicationConfig.ActiveClusterName
	clusters := replicationConfig.Clusters

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterName := range clusters {
		if err := d.validateClusterName(clusterName); err != nil {
			return err
		}
	}

	activeClusterInClusters := false
	for _, clusterName := range clusters {
		if clusterName == activeCluster {
			activeClusterInClusters = true
			break
		}
	}
	if !activeClusterInClusters {
		return errActiveClusterNotInClusters
	}

	return nil
}

func (d *AttrValidatorImpl) validateClusterName(
	clusterName string,
) error {

	if info, ok := d.clusterMetadata.GetAllClusterInfo()[clusterName]; !ok || !info.Enabled {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid cluster name: %v", clusterName))
	}
	return nil
}
