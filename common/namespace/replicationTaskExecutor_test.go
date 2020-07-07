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
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	namespacepb "go.temporal.io/temporal-proto/namespace/v1"
	replicationpb "go.temporal.io/temporal-proto/replication/v1"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	enumsspb "github.com/temporalio/temporal/api/enums/v1"
	replicationspb "github.com/temporalio/temporal/api/replication/v1"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/persistence"
	persistencetests "github.com/temporalio/temporal/common/persistence/persistence-tests"
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
	s.TestBase.Setup()
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	logger := loggerimpl.NewLogger(zapLogger)
	s.namespaceReplicator = NewReplicationTaskExecutor(
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
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(task)
	s.Nil(err)

	task.Id = uuid.New()
	task.Info.Name = name
	err = s.namespaceReplicator.Execute(task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	task.Id = id
	task.Info.Name = "other random namespace test name"
	err = s.namespaceReplicator.Execute(task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.New()
	name := "some random namespace test name"
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(task)
	s.Nil(err)

	resp, err := s.MetadataManager.GetNamespace(&persistence.GetNamespaceRequest{ID: id})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATUS_REGISTERED, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)

	// handle duplicated task
	err = s.namespaceReplicator.Execute(task)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NamespaceNotExist() {
	operation := enumsspb.NAMESPACE_OPERATION_UPDATE
	id := uuid.New()
	name := "some random namespace test name"
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        namespaceData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(updateTask)
	s.Nil(err)

	resp, err := s.MetadataManager.GetNamespace(&persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATUS_REGISTERED, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(namespaceData, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
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
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateStatus := enumspb.NAMESPACE_STATUS_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v1"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateHistoryArchivalStatus := enumspb.ARCHIVAL_STATUS_DISABLED
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_DISABLED
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
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  updateHistoryArchivalStatus,
			HistoryArchivalUri:                     updateHistoryArchivalURI,
			VisibilityArchivalStatus:               updateVisibilityArchivalStatus,
			VisibilityArchivalUri:                  updateVisibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(&persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATUS_DEPRECATED, resp.Namespace.Info.Status)
	s.Equal(updateDescription, resp.Namespace.Info.Description)
	s.Equal(updateOwnerEmail, resp.Namespace.Info.Owner)
	s.Equal(updatedData, resp.Namespace.Info.Data)
	s.Equal(updateRetention, resp.Namespace.Config.RetentionDays)
	s.Equal(updateEmitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(updateHistoryArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(updateHistoryArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(updateVisibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
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
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_DISABLED
	historyArchivalURI := ""
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_DISABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateStatus := enumspb.NAMESPACE_STATUS_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updateData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateHistoryArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updateData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  updateHistoryArchivalStatus,
			HistoryArchivalUri:                     updateHistoryArchivalURI,
			VisibilityArchivalStatus:               updateVisibilityArchivalStatus,
			VisibilityArchivalUri:                  updateVisibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(&persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATUS_DEPRECATED, resp.Namespace.Info.Status)
	s.Equal(updateDescription, resp.Namespace.Info.Description)
	s.Equal(updateOwnerEmail, resp.Namespace.Info.Owner)
	s.Equal(updateData, resp.Namespace.Info.Data)
	s.Equal(updateRetention, resp.Namespace.Config.RetentionDays)
	s.Equal(updateEmitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(updateHistoryArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(updateHistoryArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(updateVisibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
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
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateStatus := enumspb.NAMESPACE_STATUS_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
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
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(&persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATUS_REGISTERED, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
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
	status := enumspb.NAMESPACE_STATUS_REGISTERED
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enumspb.ARCHIVAL_STATUS_ENABLED
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
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateStatus := enumspb.NAMESPACE_STATUS_DEPRECATED
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
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
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalUri:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalUri:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	err = s.namespaceReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetNamespace(&persistence.GetNamespaceRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.EqualValues(id, resp.Namespace.Info.Id)
	s.Equal(name, resp.Namespace.Info.Name)
	s.Equal(enumspb.NAMESPACE_STATUS_REGISTERED, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalUri)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalUri)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}
