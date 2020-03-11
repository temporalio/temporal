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

package domain

import (
	"github.com/gogo/protobuf/types"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/persistence"
)

// NOTE: the counterpart of domain replication receiving logic is in service/worker package

type (
	// Replicator is the interface which can replicate the domain
	Replicator interface {
		HandleTransmissionTask(domainOperation enums.DomainOperation, info *persistence.DomainInfo,
			config *persistence.DomainConfig, replicationConfig *persistence.DomainReplicationConfig,
			configVersion int64, failoverVersion int64, isGlobalDomainEnabled bool) error
	}

	domainReplicatorImpl struct {
		replicationMessageSink messaging.Producer
		logger                 log.Logger
	}
)

// NewDomainReplicator create a new instance of domain replicator
func NewDomainReplicator(replicationMessageSink messaging.Producer, logger log.Logger) Replicator {
	return &domainReplicatorImpl{
		replicationMessageSink: replicationMessageSink,
		logger:                 logger,
	}
}

// HandleTransmissionTask handle transmission of the domain replication task
func (domainReplicator *domainReplicatorImpl) HandleTransmissionTask(domainOperation enums.DomainOperation,
	info *persistence.DomainInfo, config *persistence.DomainConfig, replicationConfig *persistence.DomainReplicationConfig,
	configVersion int64, failoverVersion int64, isGlobalDomainEnabled bool) error {

	if !isGlobalDomainEnabled {
		domainReplicator.logger.Warn("Should not replicate non global domain", tag.WorkflowDomainID(info.ID))
		return nil
	}

	status, err := domainReplicator.convertDomainStatusToProto(info.Status)
	if err != nil {
		return err
	}

	taskType := enums.ReplicationTaskTypeDomain
	task := &replication.DomainTaskAttributes{
		DomainOperation: domainOperation,
		Id:              info.ID,
		Info: &commonproto.DomainInfo{
			Name:        info.Name,
			Status:      status,
			Description: info.Description,
			OwnerEmail:  info.OwnerEmail,
			Data:        info.Data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: config.Retention,
			EmitMetric:                             &types.BoolValue{Value: config.EmitMetric},
			HistoryArchivalStatus:                  config.HistoryArchivalStatus,
			HistoryArchivalURI:                     config.HistoryArchivalURI,
			VisibilityArchivalStatus:               config.VisibilityArchivalStatus,
			VisibilityArchivalURI:                  config.VisibilityArchivalURI,
			BadBinaries:                            &config.BadBinaries,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: replicationConfig.ActiveClusterName,
			Clusters:          domainReplicator.convertClusterReplicationConfigToProto(replicationConfig.Clusters),
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	return domainReplicator.replicationMessageSink.Publish(
		&replication.ReplicationTask{
			TaskType: taskType,
			Attributes: &replication.ReplicationTask_DomainTaskAttributes{
				DomainTaskAttributes: task,
			},
		})
}

func (domainReplicator *domainReplicatorImpl) convertClusterReplicationConfigToProto(
	input []*persistence.ClusterReplicationConfig,
) []*commonproto.ClusterReplicationConfiguration {
	output := []*commonproto.ClusterReplicationConfiguration{}
	for _, cluster := range input {
		clusterName := cluster.ClusterName
		output = append(output, &commonproto.ClusterReplicationConfiguration{ClusterName: clusterName})
	}
	return output
}

func (domainReplicator *domainReplicatorImpl) convertDomainStatusToProto(input int) (enums.DomainStatus, error) {
	switch input {
	case persistence.DomainStatusRegistered:
		output := enums.DomainStatusRegistered
		return output, nil
	case persistence.DomainStatusDeprecated:
		output := enums.DomainStatusDeprecated
		return output, nil
	default:
		return enums.DomainStatusRegistered, ErrInvalidDomainStatus
	}
}
