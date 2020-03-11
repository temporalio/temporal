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
// THE SOFTWARE IS PROVIdED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package domain

import (
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/persistence"
	persistencetests "github.com/temporalio/temporal/common/persistence/persistence-tests"
)

type (
	domainReplicationTaskExecutorSuite struct {
		suite.Suite
		persistencetests.TestBase
		domainReplicator *domainReplicationTaskExecutorImpl
	}
)

func TestDomainReplicationTaskExecutorSuite(t *testing.T) {
	s := new(domainReplicationTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *domainReplicationTaskExecutorSuite) SetupSuite() {
}

func (s *domainReplicationTaskExecutorSuite) TearDownSuite() {

}

func (s *domainReplicationTaskExecutorSuite) SetupTest() {
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup()
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	logger := loggerimpl.NewLogger(zapLogger)
	s.domainReplicator = NewReplicationTaskExecutor(
		s.MetadataManager,
		logger,
	).(*domainReplicationTaskExecutorImpl)
}

func (s *domainReplicationTaskExecutorSuite) TearDownTest() {
	s.TearDownWorkflowStore()
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_RegisterDomainTask_NameUUIDCollision() {
	operation := enums.DomainOperationCreate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enums.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	task := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.domainReplicator.Execute(task)
	s.Nil(err)

	task.Id = uuid.New()
	task.Info.Name = name
	err = s.domainReplicator.Execute(task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	task.Id = id
	task.Info.Name = "other random domain test name"
	err = s.domainReplicator.Execute(task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_RegisterDomainTask() {
	operation := enums.DomainOperationCreate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enums.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	task := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.domainReplicator.Execute(task)
	s.Nil(err)

	resp, err := s.MetadataManager.GetDomain(&persistence.GetDomainRequest{ID: id})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(persistence.DomainStatusRegistered, resp.Info.Status)
	s.Equal(description, resp.Info.Description)
	s.Equal(ownerEmail, resp.Info.OwnerEmail)
	s.Equal(data, resp.Info.Data)
	s.Equal(retention, resp.Config.Retention)
	s.Equal(emitMetric, resp.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(s.domainReplicator.convertClusterReplicationConfigFromProto(clusters), resp.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.ConfigVersion)
	s.Equal(failoverVersion, resp.FailoverVersion)
	s.Equal(int64(0), resp.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)

	// handle duplicated task
	err = s.domainReplicator.Execute(task)
	s.Nil(err)
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_UpdateDomainTask_DomainNotExist() {
	operation := enums.DomainOperationUpdate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enums.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(12)
	failoverVersion := int64(59)
	domainData := map[string]string{"k1": "v1", "k2": "v2"}
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	updateTask := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        domainData,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.domainReplicator.Execute(updateTask)
	s.Nil(err)

	resp, err := s.MetadataManager.GetDomain(&persistence.GetDomainRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(persistence.DomainStatusRegistered, resp.Info.Status)
	s.Equal(description, resp.Info.Description)
	s.Equal(ownerEmail, resp.Info.OwnerEmail)
	s.Equal(domainData, resp.Info.Data)
	s.Equal(retention, resp.Config.Retention)
	s.Equal(emitMetric, resp.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(s.domainReplicator.convertClusterReplicationConfigFromProto(clusters), resp.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.ConfigVersion)
	s.Equal(failoverVersion, resp.FailoverVersion)
	s.Equal(int64(0), resp.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_UpdateDomainTask_UpdateConfig_UpdateActiveCluster() {
	operation := enums.DomainOperationCreate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enums.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.domainReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enums.DomainOperationUpdate
	updateStatus := enums.DomainStatusDeprecated
	updateDescription := "other random domain test description"
	updateOwnerEmail := "other random domain test owner"
	updatedData := map[string]string{"k": "v1"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateHistoryArchivalStatus := enums.ArchivalStatusDisabled
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalStatus := enums.ArchivalStatusDisabled
	updateVisibilityArchivalURI := "some updated visibility archival uri"
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion + 1
	updateFailoverVersion := failoverVersion + 1
	updateClusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replication.DomainTaskAttributes{
		DomainOperation: updateOperation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  updateHistoryArchivalStatus,
			HistoryArchivalURI:                     updateHistoryArchivalURI,
			VisibilityArchivalStatus:               updateVisibilityArchivalStatus,
			VisibilityArchivalURI:                  updateVisibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.domainReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetDomain(&persistence.GetDomainRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(persistence.DomainStatusDeprecated, resp.Info.Status)
	s.Equal(updateDescription, resp.Info.Description)
	s.Equal(updateOwnerEmail, resp.Info.OwnerEmail)
	s.Equal(updatedData, resp.Info.Data)
	s.Equal(updateRetention, resp.Config.Retention)
	s.Equal(updateEmitMetric, resp.Config.EmitMetric)
	s.Equal(updateHistoryArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(updateHistoryArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(updateVisibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(updateVisibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(updateClusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(s.domainReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.ReplicationConfig.Clusters)
	s.Equal(updateConfigVersion, resp.ConfigVersion)
	s.Equal(updateFailoverVersion, resp.FailoverVersion)
	s.Equal(notificationVersion, resp.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_UpdateDomainTask_UpdateConfig_NoUpdateActiveCluster() {
	operation := enums.DomainOperationCreate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusDisabled
	historyArchivalURI := ""
	visibilityArchivalStatus := enums.ArchivalStatusDisabled
	visibilityArchivalURI := ""
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.domainReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enums.DomainOperationUpdate
	updateStatus := enums.DomainStatusDeprecated
	updateDescription := "other random domain test description"
	updateOwnerEmail := "other random domain test owner"
	updateData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateHistoryArchivalStatus := enums.ArchivalStatusEnabled
	updateHistoryArchivalURI := "some updated history archival uri"
	updateVisibilityArchivalStatus := enums.ArchivalStatusEnabled
	updateVisibilityArchivalURI := "some updated visibility archival uri"
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion + 1
	updateFailoverVersion := failoverVersion - 1
	updateClusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replication.DomainTaskAttributes{
		DomainOperation: updateOperation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updateData,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  updateHistoryArchivalStatus,
			HistoryArchivalURI:                     updateHistoryArchivalURI,
			VisibilityArchivalStatus:               updateVisibilityArchivalStatus,
			VisibilityArchivalURI:                  updateVisibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.domainReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetDomain(&persistence.GetDomainRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(persistence.DomainStatusDeprecated, resp.Info.Status)
	s.Equal(updateDescription, resp.Info.Description)
	s.Equal(updateOwnerEmail, resp.Info.OwnerEmail)
	s.Equal(updateData, resp.Info.Data)
	s.Equal(updateRetention, resp.Config.Retention)
	s.Equal(updateEmitMetric, resp.Config.EmitMetric)
	s.Equal(updateHistoryArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(updateHistoryArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(updateVisibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(updateVisibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(s.domainReplicator.convertClusterReplicationConfigFromProto(updateClusters), resp.ReplicationConfig.Clusters)
	s.Equal(updateConfigVersion, resp.ConfigVersion)
	s.Equal(failoverVersion, resp.FailoverVersion)
	s.Equal(int64(0), resp.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_UpdateDomainTask_NoUpdateConfig_UpdateActiveCluster() {
	operation := enums.DomainOperationCreate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enums.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	err := s.domainReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enums.DomainOperationUpdate
	updateStatus := enums.DomainStatusDeprecated
	updateDescription := "other random domain test description"
	updateOwnerEmail := "other random domain test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion - 1
	updateFailoverVersion := failoverVersion + 1
	updateClusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replication.DomainTaskAttributes{
		DomainOperation: updateOperation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.domainReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetDomain(&persistence.GetDomainRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(persistence.DomainStatusRegistered, resp.Info.Status)
	s.Equal(description, resp.Info.Description)
	s.Equal(ownerEmail, resp.Info.OwnerEmail)
	s.Equal(data, resp.Info.Data)
	s.Equal(retention, resp.Config.Retention)
	s.Equal(emitMetric, resp.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(updateClusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(s.domainReplicator.convertClusterReplicationConfigFromProto(clusters), resp.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.ConfigVersion)
	s.Equal(updateFailoverVersion, resp.FailoverVersion)
	s.Equal(notificationVersion, resp.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}

func (s *domainReplicationTaskExecutorSuite) TestExecute_UpdateDomainTask_NoUpdateConfig_NoUpdateActiveCluster() {
	operation := enums.DomainOperationCreate
	id := uuid.New()
	name := "some random domain test name"
	status := enums.DomainStatusRegistered
	description := "some random test description"
	ownerEmail := "some random test owner"
	data := map[string]string{"k": "v"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := enums.ArchivalStatusEnabled
	historyArchivalURI := "some random history archival uri"
	visibilityArchivalStatus := enums.ArchivalStatusEnabled
	visibilityArchivalURI := "some random visibility archival uri"
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	configVersion := int64(0)
	failoverVersion := int64(59)
	clusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStandby,
		},
	}

	createTask := &replication.DomainTaskAttributes{
		DomainOperation: operation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retention,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}
	metadata, err := s.MetadataManager.GetMetadata()
	s.Nil(err)
	notificationVersion := metadata.NotificationVersion
	err = s.domainReplicator.Execute(createTask)
	s.Nil(err)

	// success update case
	updateOperation := enums.DomainOperationUpdate
	updateStatus := enums.DomainStatusDeprecated
	updateDescription := "other random domain test description"
	updateOwnerEmail := "other random domain test owner"
	updatedData := map[string]string{"k": "v2"}
	updateRetention := int32(122)
	updateEmitMetric := true
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	updateConfigVersion := configVersion - 1
	updateFailoverVersion := failoverVersion - 1
	updateClusters := []*commonproto.ClusterReplicationConfiguration{
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterActive,
		},
		&commonproto.ClusterReplicationConfiguration{
			ClusterName: updateClusterStandby,
		},
	}
	updateTask := &replication.DomainTaskAttributes{
		DomainOperation: updateOperation,
		Id:              id,
		Info: &commonproto.DomainInfo{
			Name:        name,
			Status:      updateStatus,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		},
		Config: &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: updateRetention,
			EmitMetric:                             &types.BoolValue{Value: updateEmitMetric},
			HistoryArchivalStatus:                  historyArchivalStatus,
			HistoryArchivalURI:                     historyArchivalURI,
			VisibilityArchivalStatus:               visibilityArchivalStatus,
			VisibilityArchivalURI:                  visibilityArchivalURI,
		},
		ReplicationConfig: &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		},
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}
	err = s.domainReplicator.Execute(updateTask)
	s.Nil(err)
	resp, err := s.MetadataManager.GetDomain(&persistence.GetDomainRequest{Name: name})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(persistence.DomainStatusRegistered, resp.Info.Status)
	s.Equal(description, resp.Info.Description)
	s.Equal(ownerEmail, resp.Info.OwnerEmail)
	s.Equal(data, resp.Info.Data)
	s.Equal(retention, resp.Config.Retention)
	s.Equal(emitMetric, resp.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(s.domainReplicator.convertClusterReplicationConfigFromProto(clusters), resp.ReplicationConfig.Clusters)
	s.Equal(configVersion, resp.ConfigVersion)
	s.Equal(failoverVersion, resp.FailoverVersion)
	s.Equal(int64(0), resp.FailoverNotificationVersion)
	s.Equal(notificationVersion, resp.NotificationVersion)
}
