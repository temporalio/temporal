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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskHandler_mock.go

package namespace

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

var (
	// ErrEmptyNamespaceReplicationTask is the error to indicate empty replication task
	ErrEmptyNamespaceReplicationTask = serviceerror.NewInvalidArgument("empty namespace replication task")
	// ErrInvalidNamespaceOperation is the error to indicate empty namespace operation attribute
	ErrInvalidNamespaceOperation = serviceerror.NewInvalidArgument("invalid namespace operation attribute")
	// ErrInvalidNamespaceID is the error to indicate empty rID attribute
	ErrInvalidNamespaceID = serviceerror.NewInvalidArgument("invalid namespace ID attribute")
	// ErrInvalidNamespaceInfo is the error to indicate empty info attribute
	ErrInvalidNamespaceInfo = serviceerror.NewInvalidArgument("invalid namespace info attribute")
	// ErrInvalidNamespaceConfig is the error to indicate empty config attribute
	ErrInvalidNamespaceConfig = serviceerror.NewInvalidArgument("invalid namespace config attribute")
	// ErrInvalidNamespaceReplicationConfig is the error to indicate empty replication config attribute
	ErrInvalidNamespaceReplicationConfig = serviceerror.NewInvalidArgument("invalid namespace replication config attribute")
	// ErrInvalidNamespaceConfigVersion is the error to indicate empty config version attribute
	ErrInvalidNamespaceConfigVersion = serviceerror.NewInvalidArgument("invalid namespace config version attribute")
	// ErrInvalidNamespaceFailoverVersion is the error to indicate empty failover version attribute
	ErrInvalidNamespaceFailoverVersion = serviceerror.NewInvalidArgument("invalid namespace failover version attribute")
	// ErrInvalidNamespaceState is the error to indicate invalid namespace state
	ErrInvalidNamespaceState = serviceerror.NewInvalidArgument("invalid namespace state attribute")
	// ErrNameUUIDCollision is the error to indicate namespace name / UUID collision
	ErrNameUUIDCollision = serviceerror.NewInvalidArgument("namespace replication encountered name / UUID collision")
)

// NOTE: the counterpart of namespace replication transmission logic is in service/fropntend package

type (
	// ReplicationTaskExecutor is the interface which is to execute namespace replication task
	ReplicationTaskExecutor interface {
		Execute(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) error
	}

	namespaceReplicationTaskExecutorImpl struct {
		currentCluster  string
		metadataManager persistence.MetadataManager
		logger          log.Logger
	}
)

// NewReplicationTaskExecutor create a new instance of namespace replicator
func NewReplicationTaskExecutor(
	currentCluster string,
	metadataManagerV2 persistence.MetadataManager,
	logger log.Logger,
) ReplicationTaskExecutor {

	return &namespaceReplicationTaskExecutorImpl{
		currentCluster:  currentCluster,
		metadataManager: metadataManagerV2,
		logger:          logger,
	}
}

// Execute handles receiving of the namespace replication task
func (h *namespaceReplicationTaskExecutorImpl) Execute(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	if err := h.validateNamespaceReplicationTask(task); err != nil {
		return err
	}
	if shouldProcess, err := h.shouldProcessTask(ctx, task); !shouldProcess || err != nil {
		return err
	}

	switch task.GetNamespaceOperation() {
	case enumsspb.NAMESPACE_OPERATION_CREATE:
		return h.handleNamespaceCreationReplicationTask(ctx, task)
	case enumsspb.NAMESPACE_OPERATION_UPDATE:
		return h.handleNamespaceUpdateReplicationTask(ctx, task)
	default:
		return ErrInvalidNamespaceOperation
	}
}

func checkClusterIncludedInReplicationConfig(clusterName string, repCfg []*replicationpb.ClusterReplicationConfig) bool {
	for _, cluster := range repCfg {
		if clusterName == cluster.ClusterName {
			return true
		}
	}
	return false
}

func (h *namespaceReplicationTaskExecutorImpl) shouldProcessTask(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (bool, error) {
	resp, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.Info.GetName(),
	})
	switch err.(type) {
	case nil:
		if resp.Namespace.Info.Id != task.GetId() {
			return false, ErrNameUUIDCollision
		}

		return true, nil
	case *serviceerror.NamespaceNotFound:
		return checkClusterIncludedInReplicationConfig(h.currentCluster, task.ReplicationConfig.Clusters), nil
	default:
		// return the original err
		return false, err
	}
}

// handleNamespaceCreationReplicationTask handles the namespace creation replication task
func (h *namespaceReplicationTaskExecutorImpl) handleNamespaceCreationReplicationTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	// task already validated
	err := h.validateNamespaceStatus(task.Info.State)
	if err != nil {
		return err
	}

	request := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          task.GetId(),
				Name:        task.Info.GetName(),
				State:       task.Info.GetState(),
				Description: task.Info.GetDescription(),
				Owner:       task.Info.GetOwnerEmail(),
				Data:        task.Info.Data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               task.Config.GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:    task.Config.GetHistoryArchivalState(),
				HistoryArchivalUri:      task.Config.GetHistoryArchivalUri(),
				VisibilityArchivalState: task.Config.GetVisibilityArchivalState(),
				VisibilityArchivalUri:   task.Config.GetVisibilityArchivalUri(),
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: task.ReplicationConfig.GetActiveClusterName(),
				Clusters:          h.convertClusterReplicationConfigFromProto(task.ReplicationConfig.Clusters),
			},
			ConfigVersion:   task.GetConfigVersion(),
			FailoverVersion: task.GetFailoverVersion(),
		},
		IsGlobalNamespace: true, // local namespace will not be replicated
	}

	_, err = h.metadataManager.CreateNamespace(ctx, request)
	if err != nil {
		// SQL and Cassandra handle namespace UUID collision differently
		// here, whenever seeing a error replicating a namespace
		// do a check if there is a name / UUID collision

		recordExists := true
		resp, getErr := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			Name: task.Info.GetName(),
		})
		switch getErr.(type) {
		case nil:
			if resp.Namespace.Info.Id != task.GetId() {
				return ErrNameUUIDCollision
			}
		case *serviceerror.NamespaceNotFound:
			// no check is necessary
			recordExists = false
		default:
			// return the original err
			return err
		}

		resp, getErr = h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			ID: task.GetId(),
		})
		switch getErr.(type) {
		case nil:
			if resp.Namespace.Info.Name != task.Info.GetName() {
				return ErrNameUUIDCollision
			}
		case *serviceerror.NamespaceNotFound:
			// no check is necessary
			recordExists = false
		default:
			// return the original err
			return err
		}

		if recordExists {
			// name -> id & id -> name check pass, this is duplication request
			return nil
		}
		return err
	}

	return err
}

// handleNamespaceUpdateReplicationTask handles the namespace update replication task
func (h *namespaceReplicationTaskExecutorImpl) handleNamespaceUpdateReplicationTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	// task already validated
	err := h.validateNamespaceStatus(task.Info.State)
	if err != nil {
		return err
	}

	// first we need to get the current notification version since we need to it for conditional update
	metadata, err := h.metadataManager.GetMetadata(ctx)
	if err != nil {
		return err
	}
	notificationVersion := metadata.NotificationVersion

	// plus, we need to check whether the config version is <= the config version set in the input
	// plus, we need to check whether the failover version is <= the failover version set in the input
	resp, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.Info.GetName(),
	})
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
			// this can happen if the create namespace replication task is to processed.
			// e.g. new cluster which does not have anything
			return h.handleNamespaceCreationReplicationTask(ctx, task)
		}
		return err
	}

	recordUpdated := false
	request := &persistence.UpdateNamespaceRequest{
		Namespace:           resp.Namespace,
		NotificationVersion: notificationVersion,
		IsGlobalNamespace:   resp.IsGlobalNamespace,
	}

	if resp.Namespace.ConfigVersion < task.GetConfigVersion() {
		recordUpdated = true
		request.Namespace.Info = &persistencespb.NamespaceInfo{
			Id:          task.GetId(),
			Name:        task.Info.GetName(),
			State:       task.Info.GetState(),
			Description: task.Info.GetDescription(),
			Owner:       task.Info.GetOwnerEmail(),
			Data:        task.Info.Data,
		}
		request.Namespace.Config = &persistencespb.NamespaceConfig{
			Retention:               task.Config.GetWorkflowExecutionRetentionTtl(),
			HistoryArchivalState:    task.Config.GetHistoryArchivalState(),
			HistoryArchivalUri:      task.Config.GetHistoryArchivalUri(),
			VisibilityArchivalState: task.Config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:   task.Config.GetVisibilityArchivalUri(),
		}
		if task.Config.GetBadBinaries() != nil {
			request.Namespace.Config.BadBinaries = task.Config.GetBadBinaries()
		}
		request.Namespace.ReplicationConfig.Clusters = h.convertClusterReplicationConfigFromProto(task.ReplicationConfig.Clusters)
		request.Namespace.ConfigVersion = task.GetConfigVersion()
	}
	if resp.Namespace.FailoverVersion < task.GetFailoverVersion() {
		recordUpdated = true
		request.Namespace.ReplicationConfig.ActiveClusterName = task.ReplicationConfig.GetActiveClusterName()
		request.Namespace.FailoverVersion = task.GetFailoverVersion()
		request.Namespace.FailoverNotificationVersion = notificationVersion
	}

	if !recordUpdated {
		return nil
	}

	return h.metadataManager.UpdateNamespace(ctx, request)
}

func (h *namespaceReplicationTaskExecutorImpl) validateNamespaceReplicationTask(task *replicationspb.NamespaceTaskAttributes) error {
	if task == nil {
		return ErrEmptyNamespaceReplicationTask
	}

	if task.Id == "" {
		return ErrInvalidNamespaceID
	} else if task.Info == nil {
		return ErrInvalidNamespaceInfo
	} else if task.Config == nil {
		return ErrInvalidNamespaceConfig
	} else if task.ReplicationConfig == nil {
		return ErrInvalidNamespaceReplicationConfig
	}
	return nil
}

func (h *namespaceReplicationTaskExecutorImpl) convertClusterReplicationConfigFromProto(
	input []*replicationpb.ClusterReplicationConfig) []string {
	output := []string{}
	for _, cluster := range input {
		clusterName := cluster.GetClusterName()
		output = append(output, clusterName)
	}
	return output
}

func (h *namespaceReplicationTaskExecutorImpl) validateNamespaceStatus(input enumspb.NamespaceState) error {
	switch input {
	case enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED:
		return nil
	default:
		return ErrInvalidNamespaceState
	}
}
