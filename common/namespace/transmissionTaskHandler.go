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
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

// NOTE: the counterpart of namespace replication receiving logic is in service/worker package

type (
	// Replicator is the interface which can replicate the namespace
	Replicator interface {
		HandleTransmissionTask(
			ctx context.Context,
			namespaceOperation enumsspb.NamespaceOperation,
			info *persistencespb.NamespaceInfo,
			config *persistencespb.NamespaceConfig,
			replicationConfig *persistencespb.NamespaceReplicationConfig,
			replicationClusterListUpdated bool,
			configVersion int64,
			failoverVersion int64,
			isGlobalNamespace bool,
		) error
	}

	namespaceReplicatorImpl struct {
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		logger                    log.Logger
	}
)

// NewNamespaceReplicator create a new instance of namespace replicator
func NewNamespaceReplicator(
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	logger log.Logger,
) Replicator {
	return &namespaceReplicatorImpl{
		namespaceReplicationQueue: namespaceReplicationQueue,
		logger:                    logger,
	}
}

// HandleTransmissionTask handle transmission of the namespace replication task
func (namespaceReplicator *namespaceReplicatorImpl) HandleTransmissionTask(
	ctx context.Context,
	namespaceOperation enumsspb.NamespaceOperation,
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig,
	replicationClusterListUpdated bool,
	configVersion int64,
	failoverVersion int64,
	isGlobalNamespace bool,
) error {

	if !isGlobalNamespace {
		return nil
	}
	if len(replicationConfig.Clusters) <= 1 && !replicationClusterListUpdated {
		return nil
	}
	if info.State == enumspb.NAMESPACE_STATE_DELETED {
		// Don't replicate deleted namespace changes.
		return nil
	}

	taskType := enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK
	task := &replicationspb.ReplicationTask_NamespaceTaskAttributes{
		NamespaceTaskAttributes: &replicationspb.NamespaceTaskAttributes{
			NamespaceOperation: namespaceOperation,
			Id:                 info.Id,
			Info: &namespacepb.NamespaceInfo{
				Name:        info.Name,
				State:       info.State,
				Description: info.Description,
				OwnerEmail:  info.Owner,
				Data:        info.Data,
			},
			Config: &namespacepb.NamespaceConfig{
				WorkflowExecutionRetentionTtl: config.Retention,
				HistoryArchivalState:          config.HistoryArchivalState,
				HistoryArchivalUri:            config.HistoryArchivalUri,
				VisibilityArchivalState:       config.VisibilityArchivalState,
				VisibilityArchivalUri:         config.VisibilityArchivalUri,
				BadBinaries:                   config.BadBinaries,
			},
			ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
				ActiveClusterName: replicationConfig.ActiveClusterName,
				Clusters:          namespaceReplicator.convertClusterReplicationConfigToProto(replicationConfig.Clusters),
			},
			ConfigVersion:   configVersion,
			FailoverVersion: failoverVersion,
		},
	}

	return namespaceReplicator.namespaceReplicationQueue.Publish(
		ctx,
		&replicationspb.ReplicationTask{
			TaskType:   taskType,
			Attributes: task,
		})
}

func (namespaceReplicator *namespaceReplicatorImpl) convertClusterReplicationConfigToProto(
	input []string,
) []*replicationpb.ClusterReplicationConfig {
	output := make([]*replicationpb.ClusterReplicationConfig, 0, len(input))
	for _, clusterName := range input {
		output = append(output, &replicationpb.ClusterReplicationConfig{ClusterName: clusterName})
	}
	return output
}
