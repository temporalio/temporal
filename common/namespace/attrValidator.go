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

	enumspb "go.temporal.io/temporal-proto/enums/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs/v1"
	"github.com/temporalio/temporal/common/cluster"
)

type (
	// AttrValidatorImpl is namespace attr validator
	AttrValidatorImpl struct {
		clusterMetadata  cluster.Metadata
		minRetentionDays int32
	}
)

// newAttrValidator create a new namespace attr validator
func newAttrValidator(
	clusterMetadata cluster.Metadata,
	minRetentionDays int32,
) *AttrValidatorImpl {

	return &AttrValidatorImpl{
		clusterMetadata:  clusterMetadata,
		minRetentionDays: minRetentionDays,
	}
}

func (d *AttrValidatorImpl) validateNamespaceConfig(config *persistenceblobs.NamespaceConfig) error {
	if config.RetentionDays < int32(d.minRetentionDays) {
		return errInvalidRetentionPeriod
	}
	if config.HistoryArchivalStatus == enumspb.ARCHIVAL_STATUS_ENABLED && len(config.HistoryArchivalURI) == 0 {
		return errInvalidArchivalConfig
	}
	if config.VisibilityArchivalStatus == enumspb.ARCHIVAL_STATUS_ENABLED && len(config.VisibilityArchivalURI) == 0 {
		return errInvalidArchivalConfig
	}
	return nil
}

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigForLocalNamespace(
	replicationConfig *persistenceblobs.NamespaceReplicationConfig,
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
	replicationConfig *persistenceblobs.NamespaceReplicationConfig,
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

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigClustersDoesNotRemove(
	clustersOld []string,
	clustersNew []string,
) error {

	clusterNamesOld := make(map[string]bool)
	for _, clusterName := range clustersOld {
		clusterNamesOld[clusterName] = true
	}
	clusterNamesNew := make(map[string]bool)
	for _, clusterName := range clustersNew {
		clusterNamesNew[clusterName] = true
	}

	if len(clusterNamesNew) < len(clusterNamesOld) {
		return errCannotRemoveClustersFromNamespace
	}

	for clusterName := range clusterNamesOld {
		if _, ok := clusterNamesNew[clusterName]; !ok {
			return errCannotRemoveClustersFromNamespace
		}
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
