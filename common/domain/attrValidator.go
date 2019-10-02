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

package domain

import (
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

type (
	// AttrValidatorImpl is domain attr validator
	AttrValidatorImpl struct {
		clusterMetadata  cluster.Metadata
		minRetentionDays int32
	}
)

// newAttrValidator create a new domain attr validator
func newAttrValidator(
	clusterMetadata cluster.Metadata,
	minRetentionDays int32,
) *AttrValidatorImpl {

	return &AttrValidatorImpl{
		clusterMetadata:  clusterMetadata,
		minRetentionDays: minRetentionDays,
	}
}

func (d *AttrValidatorImpl) validateDomainConfig(config *persistence.DomainConfig) error {
	if config.Retention < int32(d.minRetentionDays) {
		return errInvalidRetentionPeriod
	}
	if config.HistoryArchivalStatus == shared.ArchivalStatusEnabled && len(config.HistoryArchivalURI) == 0 {
		return errInvalidArchivalConfig
	}
	if config.VisibilityArchivalStatus == shared.ArchivalStatusEnabled && len(config.VisibilityArchivalURI) == 0 {
		return errInvalidArchivalConfig
	}
	return nil
}

func (d *AttrValidatorImpl) validateDomainReplicationConfigForLocalDomain(
	replicationConfig *persistence.DomainReplicationConfig,
) error {

	activeCluster := replicationConfig.ActiveClusterName
	clusters := replicationConfig.Clusters

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterConfig := range clusters {
		if err := d.validateClusterName(clusterConfig.ClusterName); err != nil {
			return err
		}
	}

	if activeCluster != d.clusterMetadata.GetCurrentClusterName() {
		return &shared.BadRequestError{Message: "Invalid local domain active cluster"}
	}

	if len(clusters) != 1 || clusters[0].ClusterName != activeCluster {
		return &shared.BadRequestError{Message: "Invalid local domain clusters"}
	}

	return nil
}

func (d *AttrValidatorImpl) validateDomainReplicationConfigForGlobalDomain(
	replicationConfig *persistence.DomainReplicationConfig,
) error {

	activeCluster := replicationConfig.ActiveClusterName
	clusters := replicationConfig.Clusters

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterConfig := range clusters {
		if err := d.validateClusterName(clusterConfig.ClusterName); err != nil {
			return err
		}
	}

	activeClusterInClusters := false
	for _, clusterConfig := range clusters {
		if clusterConfig.ClusterName == activeCluster {
			activeClusterInClusters = true
			break
		}
	}
	if !activeClusterInClusters {
		return errActiveClusterNotInClusters
	}

	return nil
}

func (d *AttrValidatorImpl) validateDomainReplicationConfigClustersDoesNotChange(
	clustersOld []*persistence.ClusterReplicationConfig,
	clustersNew []*persistence.ClusterReplicationConfig,
) error {

	clusterNamesOld := make(map[string]bool)
	for _, clusterConfig := range clustersOld {
		clusterNamesOld[clusterConfig.ClusterName] = true
	}
	clusterNamesNew := make(map[string]bool)
	for _, clusterConfig := range clustersNew {
		clusterNamesNew[clusterConfig.ClusterName] = true
	}

	if len(clusterNamesNew) != len(clusterNamesOld) {
		return errCannotModifyClustersFromDomain
	}

	for clusterName := range clusterNamesOld {
		if _, ok := clusterNamesNew[clusterName]; !ok {
			return errCannotModifyClustersFromDomain
		}
	}
	return nil
}

func (d *AttrValidatorImpl) validateClusterName(
	clusterName string,
) error {

	if info, ok := d.clusterMetadata.GetAllClusterInfo()[clusterName]; !ok || !info.Enabled {
		return &shared.BadRequestError{Message: fmt.Sprintf(
			"Invalid cluster name: %v",
			clusterName,
		)}
	}
	return nil
}
