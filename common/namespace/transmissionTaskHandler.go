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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

// NOTE: the counterpart of namespace replication receiving logic is in service/worker package

type (
	// Replicator is the interface which can replicate the namespace
	Replicator interface {
		HandleTransmissionTask(namespaceOperation enums.NamespaceOperation, info *persistenceblobs.NamespaceInfo,
			config *persistenceblobs.NamespaceConfig, replicationConfig *persistenceblobs.NamespaceReplicationConfig,
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
func (namespaceReplicator *namespaceReplicatorImpl) HandleTransmissionTask(namespaceOperation enums.NamespaceOperation,
	info *persistenceblobs.NamespaceInfo, config *persistenceblobs.NamespaceConfig, replicationConfig *persistenceblobs.NamespaceReplicationConfig,
	configVersion int64, failoverVersion int64, isGlobalNamespaceEnabled bool) error {

	if !isGlobalNamespaceEnabled {
		namespaceReplicator.logger.Warn("Should not replicate non global namespace", tag.WorkflowNamespaceIDBytes(info.Id))
		return nil
	}

	taskType := enums.ReplicationTaskTypeNamespace
	task := &replication.NamespaceTaskAttributes{
		NamespaceOperation: namespaceOperation,
		Id:                 primitives.UUIDString(info.Id),
		Info: &commonproto.NamespaceInfo{
			Name:        info.Name,
			Status:      info.Status,
			Description: info.Description,
			OwnerEmail:  info.Owner,
			Data:        info.Data,
		},
		Config: &commonproto.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: config.RetentionDays,
			EmitMetric:                             &types.BoolValue{Value: config.EmitMetric},
			HistoryArchivalStatus:                  config.HistoryArchivalStatus,
			HistoryArchivalURI:                     config.HistoryArchivalURI,
			VisibilityArchivalStatus:               config.VisibilityArchivalStatus,
			VisibilityArchivalURI:                  config.VisibilityArchivalURI,
			BadBinaries:                            config.BadBinaries,
		},
		ReplicationConfig: &commonproto.NamespaceReplicationConfiguration{
			ActiveClusterName: replicationConfig.ActiveClusterName,
			Clusters:          namespaceReplicator.convertClusterReplicationConfigToProto(replicationConfig.Clusters),
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	return namespaceReplicator.replicationMessageSink.Publish(
		&replication.ReplicationTask{
			TaskType: taskType,
			Attributes: &replication.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: task,
			},
		})
}

func (namespaceReplicator *namespaceReplicatorImpl) convertClusterReplicationConfigToProto(
	input []string,
) []*commonproto.ClusterReplicationConfiguration {
	output := []*commonproto.ClusterReplicationConfiguration{}
	for _, clusterName := range input {
		output = append(output, &commonproto.ClusterReplicationConfiguration{ClusterName: clusterName})
	}
	return output
}

func (namespaceReplicator *namespaceReplicatorImpl) convertNamespaceStatusToProto(input int) (enums.NamespaceStatus, error) {
	switch input {
	case persistence.NamespaceStatusRegistered:
		output := enums.NamespaceStatusRegistered
		return output, nil
	case persistence.NamespaceStatusDeprecated:
		output := enums.NamespaceStatusDeprecated
		return output, nil
	default:
		return enums.NamespaceStatusRegistered, ErrInvalidNamespaceStatus
	}
}
