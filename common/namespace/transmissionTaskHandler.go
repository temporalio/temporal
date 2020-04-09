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
	"github.com/gogo/protobuf/types"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	replicationpb "go.temporal.io/temporal-proto/replication"

	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/persistence"
)

// NOTE: the counterpart of namespace replication receiving logic is in service/worker package

type (
	// Replicator is the interface which can replicate the namespace
	Replicator interface {
		HandleTransmissionTask(namespaceOperation replicationgenpb.NamespaceOperation, info *persistence.NamespaceInfo,
			config *persistence.NamespaceConfig, replicationConfig *persistence.NamespaceReplicationConfig,
			configVersion int64, failoverVersion int64, isGlobalNamespaceEnabled bool) error
	}

	namespaceReplicatorImpl struct {
		replicationMessageSink messaging.Producer
		logger                 log.Logger
	}
)

// NewNamespaceReplicator create a new instance of namespace replicator
func NewNamespaceReplicator(replicationMessageSink messaging.Producer, logger log.Logger) Replicator {
	return &namespaceReplicatorImpl{
		replicationMessageSink: replicationMessageSink,
		logger:                 logger,
	}
}

// HandleTransmissionTask handle transmission of the namespace replication task
func (namespaceReplicator *namespaceReplicatorImpl) HandleTransmissionTask(namespaceOperation replicationgenpb.NamespaceOperation,
	info *persistence.NamespaceInfo, config *persistence.NamespaceConfig, replicationConfig *persistence.NamespaceReplicationConfig,
	configVersion int64, failoverVersion int64, isGlobalNamespaceEnabled bool) error {

	if !isGlobalNamespaceEnabled {
		namespaceReplicator.logger.Warn("Should not replicate non global namespace", tag.WorkflowNamespaceID(info.ID))
		return nil
	}

	status, err := namespaceReplicator.convertNamespaceStatusToProto(info.Status)
	if err != nil {
		return err
	}

	taskType := replicationgenpb.ReplicationTaskType_NamespaceTask
	task := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: namespaceOperation,
		Id:                 info.ID,
		Info: &namespacepb.NamespaceInfo{
			Name:        info.Name,
			Status:      status,
			Description: info.Description,
			OwnerEmail:  info.OwnerEmail,
			Data:        info.Data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: config.Retention,
			EmitMetric:                             &types.BoolValue{Value: config.EmitMetric},
			HistoryArchivalStatus:                  config.HistoryArchivalStatus,
			HistoryArchivalURI:                     config.HistoryArchivalURI,
			VisibilityArchivalStatus:               config.VisibilityArchivalStatus,
			VisibilityArchivalURI:                  config.VisibilityArchivalURI,
			BadBinaries:                            &config.BadBinaries,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
			ActiveClusterName: replicationConfig.ActiveClusterName,
			Clusters:          namespaceReplicator.convertClusterReplicationConfigToProto(replicationConfig.Clusters),
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	return namespaceReplicator.replicationMessageSink.Publish(
		&replicationgenpb.ReplicationTask{
			TaskType: taskType,
			Attributes: &replicationgenpb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: task,
			},
		})
}

func (namespaceReplicator *namespaceReplicatorImpl) convertClusterReplicationConfigToProto(
	input []*persistence.ClusterReplicationConfig,
) []*replicationpb.ClusterReplicationConfiguration {
	var output []*replicationpb.ClusterReplicationConfiguration
	for _, cluster := range input {
		clusterName := cluster.ClusterName
		output = append(output, &replicationpb.ClusterReplicationConfiguration{ClusterName: clusterName})
	}
	return output
}

func (namespaceReplicator *namespaceReplicatorImpl) convertNamespaceStatusToProto(input int) (namespacepb.NamespaceStatus, error) {
	switch input {
	case persistence.NamespaceStatusRegistered:
		output := namespacepb.NamespaceStatus_Registered
		return output, nil
	case persistence.NamespaceStatusDeprecated:
		output := namespacepb.NamespaceStatus_Deprecated
		return output, nil
	default:
		return namespacepb.NamespaceStatus_Registered, ErrInvalidNamespaceStatus
	}
}
