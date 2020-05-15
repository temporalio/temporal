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
	namespacepb "go.temporal.io/temporal-proto/namespace"
	replicationpb "go.temporal.io/temporal-proto/replication"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
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
	operation := replicationgenpb.NamespaceOperation_Create
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	task := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	operation := replicationgenpb.NamespaceOperation_Create
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	task := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	s.Equal(namespacepb.NamespaceStatus_Registered, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
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
	operation := replicationgenpb.NamespaceOperation_Update
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(12)
	failoverVersion := int64(59)
	namespaceData := map[string]string{"k1": "v1", "k2": "v2"}
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	updateTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        namespaceData,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	s.Equal(namespacepb.NamespaceStatus_Registered, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(namespaceData, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_UpdateActiveCluster() {
	operation := replicationgenpb.NamespaceOperation_Create
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := replicationgenpb.NamespaceOperation_Update
	updateStatus := namespacepb.NamespaceStatus_Deprecated
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v1"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateHistoryArchivalStatus := namespacepb.ArchivalStatus_Disabled
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalStatus := namespacepb.ArchivalStatus_Disabled
	updateVisibilityArchivalURI := "some updated visibility archival uri"
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion + 1
	updateFailoverVersion := failoverVersion + 1
	updateClusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  updateHistoryArchivalStatus,
			HistoryArchivalURI:                     updateHistoryArchivalURI,
			VisibilityArchivalStatus:               updateVisibilityArchivalStatus,
			VisibilityArchivalURI:                  updateVisibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	s.Equal(namespacepb.NamespaceStatus_Deprecated, resp.Namespace.Info.Status)
	s.Equal(updateDescription, resp.Namespace.Info.Description)
	s.Equal(updateOwnerEmail, resp.Namespace.Info.Owner)
	s.Equal(updatedData, resp.Namespace.Info.Data)
	s.Equal(updateRetention, resp.Namespace.Config.RetentionDays)
	s.Equal(updateEmitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(updateHistoryArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(updateHistoryArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	s.Equal(updateVisibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(updateVisibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
	s.Equal(updateClusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(updateConfigVersion, resp.Namespace.ConfigVersion)
	s.Equal(updateFailoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(notificationVersion, resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_NoUpdateActiveCluster() {
	operation := replicationgenpb.NamespaceOperation_Create
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Disabled
	historyArchivalURI := ""
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Disabled
	visibilityArchivalURI := ""
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := replicationgenpb.NamespaceOperation_Update
	updateStatus := namespacepb.NamespaceStatus_Deprecated
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updateData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateHistoryArchivalStatus := namespacepb.ArchivalStatus_Enabled
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	updateVisibilityArchivalURI := "some updated visibility archival uri"
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion + 1
	updateFailoverVersion := failoverVersion - 1
	updateClusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updateData,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  updateHistoryArchivalStatus,
			HistoryArchivalURI:                     updateHistoryArchivalURI,
			VisibilityArchivalStatus:               updateVisibilityArchivalStatus,
			VisibilityArchivalURI:                  updateVisibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	s.Equal(namespacepb.NamespaceStatus_Deprecated, resp.Namespace.Info.Status)
	s.Equal(updateDescription, resp.Namespace.Info.Description)
	s.Equal(updateOwnerEmail, resp.Namespace.Info.Owner)
	s.Equal(updateData, resp.Namespace.Info.Data)
	s.Equal(updateRetention, resp.Namespace.Config.RetentionDays)
	s.Equal(updateEmitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(updateHistoryArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(updateHistoryArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	s.Equal(updateVisibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(updateVisibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(updateConfigVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_UpdateActiveCluster() {
	operation := replicationgenpb.NamespaceOperation_Create
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.namespaceReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := replicationgenpb.NamespaceOperation_Update
	updateStatus := namespacepb.NamespaceStatus_Deprecated
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion - 1
	updateFailoverVersion := failoverVersion + 1
	updateClusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	s.Equal(namespacepb.NamespaceStatus_Registered, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
	s.Equal(updateClusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(updateFailoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(notificationVersion, resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_NoUpdateActiveCluster() {
	operation := replicationgenpb.NamespaceOperation_Create
	id := uuid.New()
	name := "some random namespace test name"
	status := namespacepb.NamespaceStatus_Registered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := namespacepb.ArchivalStatus_Enabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := namespacepb.ArchivalStatus_Enabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: operation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	updateOperation := replicationgenpb.NamespaceOperation_Update
	updateStatus := namespacepb.NamespaceStatus_Deprecated
	updateDescription := "other random namespace test description"
	updateOwnerEmail := "other random namespace test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion - 1
	updateFailoverVersion := failoverVersion - 1
	updateClusters := []*replicationpb.ClusterReplicationConfiguration{
		{
			ClusterName: updateClusterActive,
		},
		{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
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
	s.Equal(namespacepb.NamespaceStatus_Registered, resp.Namespace.Info.Status)
	s.Equal(description, resp.Namespace.Info.Description)
	s.Equal(ownerEmail, resp.Namespace.Info.Owner)
	s.Equal(data, resp.Namespace.Info.Data)
	s.Equal(retention, resp.Namespace.Config.RetentionDays)
	s.Equal(emitMetric, resp.Namespace.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Namespace.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Namespace.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Namespace.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Namespace.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.Namespace.ReplicationConfig.ActiveClusterName)
	s.Equal(s.namespaceReplicator.convertClusterReplicationConfigFromProto(clusters), resp.Namespace.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.Namespace.ConfigVersion)
	s.Equal(failoverVersion, resp.Namespace.FailoverVersion)
	s.Equal(int64(0), resp.Namespace.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}
