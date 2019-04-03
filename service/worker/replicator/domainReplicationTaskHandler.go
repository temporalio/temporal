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

package replicator

import (
	"errors"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

var (
	// ErrEmptyDomainReplicationTask is the error to indicate empty replication task
	ErrEmptyDomainReplicationTask = errors.New("empty domain replication task")
	// ErrInvalidDomainOperation is the error to indicate empty domain operation attribute
	ErrInvalidDomainOperation = errors.New("invalid domain operation attribute")
	// ErrInvalidDomainID is the error to indicate empty rID attribute
	ErrInvalidDomainID = errors.New("invalid domain ID attribute")
	// ErrInvalidDomainInfo is the error to indicate empty info attribute
	ErrInvalidDomainInfo = errors.New("invalid domain info attribute")
	// ErrInvalidDomainConfig is the error to indicate empty config attribute
	ErrInvalidDomainConfig = errors.New("invalid domain config attribute")
	// ErrInvalidDomainReplicationConfig is the error to indicate empty replication config attribute
	ErrInvalidDomainReplicationConfig = errors.New("invalid domain replication config attribute")
	// ErrInvalidDomainConfigVersion is the error to indicate empty config version attribute
	ErrInvalidDomainConfigVersion = errors.New("invalid domain config version attribute")
	// ErrInvalidDomainFailoverVersion is the error to indicate empty failover version attribute
	ErrInvalidDomainFailoverVersion = errors.New("invalid domain failover version attribute")
	// ErrInvalidDomainStatus is the error to indicate invalid domain status
	ErrInvalidDomainStatus = errors.New("invalid domain status attribute")
)

// NOTE: the counterpart of domain replication transmission logic is in service/fropntend package

type (
	// DomainReplicator is the interface which can replicate the domain
	DomainReplicator interface {
		HandleReceivingTask(task *replicator.DomainTaskAttributes) error
	}

	domainReplicatorImpl struct {
		metadataManagerV2 persistence.MetadataManager
		logger            bark.Logger
	}
)

// NewDomainReplicator create a new instance odf domain replicator
func NewDomainReplicator(metadataManagerV2 persistence.MetadataManager, logger bark.Logger) DomainReplicator {
	return &domainReplicatorImpl{
		metadataManagerV2: metadataManagerV2,
		logger:            logger,
	}
}

// HandleReceiveTask handle receiving of the domain replication task
func (domainReplicator *domainReplicatorImpl) HandleReceivingTask(task *replicator.DomainTaskAttributes) error {
	if err := domainReplicator.validateDomainReplicationTask(task); err != nil {
		return err
	}

	switch task.GetDomainOperation() {
	case replicator.DomainOperationCreate:
		return domainReplicator.handleDomainCreationReplicationTask(task)
	case replicator.DomainOperationUpdate:
		return domainReplicator.handleDomainUpdateReplicationTask(task)
	default:
		return ErrInvalidDomainOperation
	}
}

// handleDomainCreationReplicationTask handle the domain creation replication task
func (domainReplicator *domainReplicatorImpl) handleDomainCreationReplicationTask(task *replicator.DomainTaskAttributes) error {
	// task already validated
	status, err := domainReplicator.convertDomainStatusFromThrift(task.Info.Status)
	if err != nil {
		return err
	}

	request := &persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          task.GetID(),
			Name:        task.Info.GetName(),
			Status:      status,
			Description: task.Info.GetDescription(),
			OwnerEmail:  task.Info.GetOwnerEmail(),
			Data:        task.Info.Data,
		},
		Config: &persistence.DomainConfig{
			Retention:      task.Config.GetWorkflowExecutionRetentionPeriodInDays(),
			EmitMetric:     task.Config.GetEmitMetric(),
			ArchivalBucket: task.Config.GetArchivalBucketName(),
			ArchivalStatus: task.Config.GetArchivalStatus(),
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: task.ReplicationConfig.GetActiveClusterName(),
			Clusters:          domainReplicator.convertClusterReplicationConfigFromThrift(task.ReplicationConfig.Clusters),
		},
		IsGlobalDomain:  true, // local domain will not be replicated
		ConfigVersion:   task.GetConfigVersion(),
		FailoverVersion: task.GetFailoverVersion(),
	}

	_, err = domainReplicator.metadataManagerV2.CreateDomain(request)
	return err
}

// handleDomainUpdateReplicationTask handle the domain update replication task
func (domainReplicator *domainReplicatorImpl) handleDomainUpdateReplicationTask(task *replicator.DomainTaskAttributes) error {
	// task already validated
	status, err := domainReplicator.convertDomainStatusFromThrift(task.Info.Status)
	if err != nil {
		return err
	}

	// first we need to get the current notification verion since we need to it for conditional update
	metadata, err := domainReplicator.metadataManagerV2.GetMetadata()
	if err != nil {
		return err
	}
	notificationVersion := metadata.NotificationVersion

	// plus, we need to check whether the config version is <= the config version set in the input
	// plus, we need to check whether the failover version is <= the failover version set in the input
	resp, err := domainReplicator.metadataManagerV2.GetDomain(&persistence.GetDomainRequest{
		Name: task.Info.GetName(),
	})
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); ok {
			// this can happen if the create domain replication task is to processed.
			// e.g. new cluster being lanuched
			return domainReplicator.handleDomainCreationReplicationTask(task)
		}
		return err
	}

	recordUpdated := false
	request := &persistence.UpdateDomainRequest{
		Info:                        resp.Info,
		Config:                      resp.Config,
		ReplicationConfig:           resp.ReplicationConfig,
		ConfigVersion:               resp.ConfigVersion,
		FailoverVersion:             resp.FailoverVersion,
		FailoverNotificationVersion: resp.FailoverNotificationVersion,
		NotificationVersion:         notificationVersion,
	}

	if resp.ConfigVersion < task.GetConfigVersion() {
		recordUpdated = true
		request.Info = &persistence.DomainInfo{
			ID:          task.GetID(),
			Name:        task.Info.GetName(),
			Status:      status,
			Description: task.Info.GetDescription(),
			OwnerEmail:  task.Info.GetOwnerEmail(),
			Data:        task.Info.Data,
		}
		request.Config = &persistence.DomainConfig{
			Retention:      task.Config.GetWorkflowExecutionRetentionPeriodInDays(),
			EmitMetric:     task.Config.GetEmitMetric(),
			ArchivalBucket: task.Config.GetArchivalBucketName(),
			ArchivalStatus: task.Config.GetArchivalStatus(),
		}
		request.ReplicationConfig.Clusters = domainReplicator.convertClusterReplicationConfigFromThrift(task.ReplicationConfig.Clusters)
		request.ConfigVersion = task.GetConfigVersion()
	}
	if resp.FailoverVersion < task.GetFailoverVersion() {
		recordUpdated = true
		request.ReplicationConfig.ActiveClusterName = task.ReplicationConfig.GetActiveClusterName()
		request.FailoverVersion = task.GetFailoverVersion()
		request.FailoverNotificationVersion = notificationVersion
	}

	if !recordUpdated {
		return nil
	}

	return domainReplicator.metadataManagerV2.UpdateDomain(request)
}

func (domainReplicator *domainReplicatorImpl) validateDomainReplicationTask(task *replicator.DomainTaskAttributes) error {
	if task == nil {
		return ErrEmptyDomainReplicationTask
	}

	if task.DomainOperation == nil {
		return ErrInvalidDomainOperation
	} else if task.ID == nil {
		return ErrInvalidDomainID
	} else if task.Info == nil {
		return ErrInvalidDomainInfo
	} else if task.Config == nil {
		return ErrInvalidDomainConfig
	} else if task.ReplicationConfig == nil {
		return ErrInvalidDomainReplicationConfig
	} else if task.ConfigVersion == nil {
		return ErrInvalidDomainConfigVersion
	} else if task.FailoverVersion == nil {
		return ErrInvalidDomainFailoverVersion
	}
	return nil
}

func (domainReplicator *domainReplicatorImpl) convertClusterReplicationConfigFromThrift(
	input []*shared.ClusterReplicationConfiguration) []*persistence.ClusterReplicationConfig {
	output := []*persistence.ClusterReplicationConfig{}
	for _, cluster := range input {
		clusterName := cluster.GetClusterName()
		output = append(output, &persistence.ClusterReplicationConfig{ClusterName: clusterName})
	}
	return output
}

func (domainReplicator *domainReplicatorImpl) convertDomainStatusFromThrift(input *shared.DomainStatus) (int, error) {
	if input == nil {
		return 0, ErrInvalidDomainStatus
	}

	switch *input {
	case shared.DomainStatusRegistered:
		return persistence.DomainStatusRegistered, nil
	case shared.DomainStatusDeprecated:
		return persistence.DomainStatusDeprecated, nil
	default:
		return 0, ErrInvalidDomainStatus
	}
}
