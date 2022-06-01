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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
)

type (
	namespaceReplicationTaskExecutorSuite struct {
		suite.Suite
		persistencetests.TestBase
		namespaceReplicator *namespaceReplicationTaskExecutorImpl
	}
)

func TestNamespaceReplicationTaskExecutorSuite(t *testing.T) {
	s := new(namespaceReplicationTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *namespaceReplicationTaskExecutorSuite) SetupSuite() {
}

func (s *namespaceReplicationTaskExecutorSuite) TearDownSuite() {

}

func (s *namespaceReplicationTaskExecutorSuite) SetupTest() {
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	logger := log.NewTestLogger()
	s.namespaceReplicator = NewReplicationTaskExecutor("some random standby cluster name",
		s.MetadataManager,
		logger,
	).(*namespaceReplicationTaskExecutorImpl)
}

func (s *namespaceReplicationTaskExecutorSuite) TearDownTest() {
	s.TearDownWorkflowStore()
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_NameUUIDCollision() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	s.namespaceReplicator.currentCluster = clusterStandby
	err := s.namespaceReplicator.Execute(context.Background(), task)
	s.Nil(err)

	task.Id = uuid.New()
	task.Info.Name = name
	err = s.namespaceReplicator.Execute(context.Background(), task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	task.Id = id
	task.Info.Name = "other random namespace test name"
	err = s.namespaceReplicator.Execute(context.Background(), task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(context.Background(), task)
	s.Nil(err)

	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{ID: id})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, resp.Namespace.Info.State)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.EqualValues(retention, *resp.Namespace.Config.Retention)
	s.Equal(historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)

	// handle duplicated task
	err = s.namespaceReplicator.Execute(context.Background(), task)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NamespaceNotExist() {
	operation := enumsspb.NAMESPACE_OPERATION_UPDATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(12)
	failoverVersion := int64(59)
	namespaceData := map[string]string{"k1": "v1", "k2": "v2"}
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        namespaceData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)

	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, resp.Namespace.Info.State)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(namespaceData, resp.Namespace.Info.Data)
	s.EqualValues(retention, *resp.Namespace.Config.Retention)
	s.Equal(historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_UpdateActiveCluster() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(context.Background(), createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateState := enumspb.NAMESPACE_STATE_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v1"}
	updateRetention := 122 * time.Hour * 24
	updateHistoryArchivalState := enumspb.ARCHIVAL_STATE_DISABLED
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalState := enumspb.ARCHIVAL_STATE_DISABLED
	updateVisibilityArchivalURI := "some updated visibility archival uri"
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion + 1
	updateFailoverVersion := failoverVersion + 1
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &updateRetention,
			HistoryArchivalState:          updateHistoryArchivalState,
			HistoryArchivalUri:            updateHistoryArchivalURI,
			VisibilityArchivalState:       updateVisibilityArchivalState,
			VisibilityArchivalUri:         updateVisibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	s.namespaceReplicator.currentCluster = updateClusterStandby
	err = s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_DEPRECATED, resp.Namespace.Info.State)
	s.Equal(updateDescription, resp.Namespace.Info.Description)
	s.Equal(updateOwnerEmail, resp.Namespace.Info.Owner)
	s.Equal(updatedData, resp.Namespace.Info.Data)
	s.EqualValues(updateRetention, *resp.Namespace.Config.Retention)
	s.Equal(updateHistoryArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(updateHistoryArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(updateVisibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(updateVisibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(updateClusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(updateConfigVersion, resp.Namespace.ConfigVersion)
	s.Equal(updateFailoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(notificationVersion, resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_NoUpdateActiveCluster() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_DISABLED
	historyArchivalURI := ""
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_DISABLED
	visibilityArchivalURI := ""
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(context.Background(), createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateState := enumspb.NAMESPACE_STATE_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updateData := map[string]string{"k": "v2"}
	updateRetention := 122 * time.Hour * 24
	updateHistoryArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	updateVisibilityArchivalURI := "some updated visibility archival uri"
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion + 1
	updateFailoverVersion := failoverVersion - 1
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updateData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &updateRetention,
			HistoryArchivalState:          updateHistoryArchivalState,
			HistoryArchivalUri:            updateHistoryArchivalURI,
			VisibilityArchivalState:       updateVisibilityArchivalState,
			VisibilityArchivalUri:         updateVisibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	s.namespaceReplicator.currentCluster = updateClusterStandby
	err = s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_DEPRECATED, resp.Namespace.Info.State)
	s.Equal(updateDescription, resp.Namespace.Info.Description)
	s.Equal(updateOwnerEmail, resp.Namespace.Info.Owner)
	s.Equal(updateData, resp.Namespace.Info.Data)
	s.EqualValues(updateRetention, *resp.Namespace.Config.Retention)
	s.Equal(updateHistoryArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(updateHistoryArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(updateVisibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(updateVisibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(updateConfigVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_UpdateActiveCluster() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(context.Background(), createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateState := enumspb.NAMESPACE_STATE_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := 122 * time.Hour * 24
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion - 1
	updateFailoverVersion := failoverVersion + 1
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &updateRetention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	s.namespaceReplicator.currentCluster = updateClusterStandby
	err = s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, resp.Namespace.Info.State)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.EqualValues(retention, *resp.Namespace.Config.Retention)
	s.Equal(historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(updateClusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(updateFailoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(notificationVersion, resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_NoUpdateActiveCluster() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(context.Background(), createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateState := enumspb.NAMESPACE_STATE_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := 122 * time.Hour * 24
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion - 1
	updateFailoverVersion := failoverVersion - 1
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &updateRetention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	s.namespaceReplicator.currentCluster = updateClusterStandby
	err = s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, resp.Namespace.Info.State)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.EqualValues(retention, *resp.Namespace.Config.Retention)
	s.Equal(historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateClusterList() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	state := enumspb.NAMESPACE_STATE_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := 10 * time.Hour * 24
	historyArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalState := enumspb.ARCHIVAL_STATE_ENABLED
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata(context.Background())
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(context.Background(), createTask)
	s.Nil(err)

	// success update case
	updateClusterActive := "other random active cluster name"
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: updateClusterActive,
		},
	}
	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   configVersion + 1,
		FailoverVersion: failoverVersion + 1,
	}
	err = s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, resp.Namespace.Info.State)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.EqualValues(retention, *resp.Namespace.Config.Retention)
	s.Equal(historyArchivalState, resp.Namespace.Config.HistoryArchivalState)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalState, resp.Namespace.Config.VisibilityArchivalState)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(updateClusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion+1, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion+1, resp.Namespace.FailoverVersion)
	s.Equal(int64(1), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion+1, resp.NotificationVersion)
}
