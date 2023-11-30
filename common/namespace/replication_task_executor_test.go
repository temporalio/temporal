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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	namespaceReplicationTaskExecutorSuite struct {
		suite.Suite
		controller *gomock.Controller

		mockMetadataMgr     *persistence.MockMetadataManager
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
	s.controller = gomock.NewController(s.T())
	s.mockMetadataMgr = persistence.NewMockMetadataManager(s.controller)
	logger := log.NewTestLogger()
	s.namespaceReplicator = NewReplicationTaskExecutor(
		"some random standby cluster name",
		s.mockMetadataMgr,
		logger,
	).(*namespaceReplicationTaskExecutorImpl)
}

func (s *namespaceReplicationTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
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
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
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
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: uuid.New(),
		},
	}}, nil)
	task.Id = uuid.New()
	task.Info.Name = name
	err := s.namespaceReplicator.Execute(context.Background(), task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	task.Id = id
	task.Info.Name = "other random namespace test name"
	var count int
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: task.Info.Name,
	}).DoAndReturn(func(_ context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		nsID := id
		if count != 0 {
			nsID = uuid.New()
		}
		count++
		return &persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id: nsID,
			},
		}}, nil
	}).Times(2)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	err = s.namespaceReplicator.Execute(context.Background(), task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_Success() {
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
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
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

	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: name}).Return(
		nil, &serviceerror.NamespaceNotFound{}).Times(1)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          id,
				State:       task.Info.State,
				Name:        task.Info.Name,
				Description: task.Info.Description,
				Owner:       task.Info.OwnerEmail,
				Data:        task.Info.Data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               task.Config.WorkflowExecutionRetentionTtl,
				HistoryArchivalState:    task.Config.HistoryArchivalState,
				HistoryArchivalUri:      task.Config.HistoryArchivalUri,
				VisibilityArchivalState: task.Config.VisibilityArchivalState,
				VisibilityArchivalUri:   task.Config.VisibilityArchivalUri,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: task.ReplicationConfig.ActiveClusterName,
				Clusters:          []string{clusterActive, clusterStandby},
			},
			ConfigVersion:               configVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             failoverVersion,
		},
		IsGlobalNamespace: true,
	})
	err := s.namespaceReplicator.Execute(context.Background(), task)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_Duplicate() {
	name := uuid.New()
	id := uuid.New()
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}
	task := &replicationspb.NamespaceTaskAttributes{
		Id:                 id,
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Info: &namespacepb.NamespaceInfo{
			Name:  name,
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
		Config: &namespacepb.NamespaceConfig{},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
	}
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: id,
		},
	}}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: id,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Name: name,
		},
	}}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	err := s.namespaceReplicator.Execute(context.Background(), task)
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
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
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

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 0}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: name}).Return(
		nil, &serviceerror.NamespaceNotFound{}).Times(2)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          id,
				State:       updateTask.Info.State,
				Name:        updateTask.Info.Name,
				Description: updateTask.Info.Description,
				Owner:       updateTask.Info.OwnerEmail,
				Data:        updateTask.Info.Data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               updateTask.Config.WorkflowExecutionRetentionTtl,
				HistoryArchivalState:    updateTask.Config.HistoryArchivalState,
				HistoryArchivalUri:      updateTask.Config.HistoryArchivalUri,
				VisibilityArchivalState: updateTask.Config.VisibilityArchivalState,
				VisibilityArchivalUri:   updateTask.Config.VisibilityArchivalUri,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: updateTask.ReplicationConfig.ActiveClusterName,
				Clusters:          []string{clusterActive, clusterStandby},
			},
			ConfigVersion:               configVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             failoverVersion,
		},
		IsGlobalNamespace: true,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_UpdateActiveCluster() {
	id := uuid.New()
	name := "some random namespace test name"
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
	updateConfigVersion := int64(1)
	updateFailoverVersion := int64(59)
	failoverTime := time.Now()
	failoverHistory := []*replicationpb.FailoverStatus{
		{
			FailoverTime:    timestamppb.New(failoverTime),
			FailoverVersion: 999,
		},
	}
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
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
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
		FailoverHistory: failoverHistory,
	}

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: id,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
	}}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          id,
				State:       updateTask.Info.State,
				Name:        updateTask.Info.Name,
				Description: updateTask.Info.Description,
				Owner:       updateTask.Info.OwnerEmail,
				Data:        updateTask.Info.Data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               updateTask.Config.WorkflowExecutionRetentionTtl,
				HistoryArchivalState:    updateTask.Config.HistoryArchivalState,
				HistoryArchivalUri:      updateTask.Config.HistoryArchivalUri,
				VisibilityArchivalState: updateTask.Config.VisibilityArchivalState,
				VisibilityArchivalUri:   updateTask.Config.VisibilityArchivalUri,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: updateTask.ReplicationConfig.ActiveClusterName,
				Clusters:          []string{updateClusterActive, updateClusterStandby},
				FailoverHistory:   convertFailoverHistoryToPersistenceProto(failoverHistory),
			},
			ConfigVersion:               updateConfigVersion,
			FailoverNotificationVersion: updateFailoverVersion,
			FailoverVersion:             updateFailoverVersion,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_NoUpdateActiveCluster() {
	id := uuid.New()
	name := "some random namespace test name"
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
	updateConfigVersion := int64(1)
	updateFailoverVersion := int64(59)
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
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
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

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: id,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		FailoverVersion:   updateFailoverVersion + 1,
	}}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          id,
				State:       updateTask.Info.State,
				Name:        updateTask.Info.Name,
				Description: updateTask.Info.Description,
				Owner:       updateTask.Info.OwnerEmail,
				Data:        updateTask.Info.Data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               updateTask.Config.WorkflowExecutionRetentionTtl,
				HistoryArchivalState:    updateTask.Config.HistoryArchivalState,
				HistoryArchivalUri:      updateTask.Config.HistoryArchivalUri,
				VisibilityArchivalState: updateTask.Config.VisibilityArchivalState,
				VisibilityArchivalUri:   updateTask.Config.VisibilityArchivalUri,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				Clusters: []string{updateClusterActive, updateClusterStandby},
			},
			ConfigVersion:               updateConfigVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             updateFailoverVersion + 1,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_UpdateActiveCluster() {
	id := uuid.New()
	name := "some random namespace test name"
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
	updateConfigVersion := int64(1)
	updateFailoverVersion := int64(59)
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
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
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

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: id,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		ConfigVersion:     updateConfigVersion + 1,
	}}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id: id,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: updateClusterActive,
			},
			ConfigVersion:               updateConfigVersion + 1,
			FailoverNotificationVersion: updateFailoverVersion,
			FailoverVersion:             updateFailoverVersion,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_NoUpdateActiveCluster() {
	id := uuid.New()
	name := "some random namespace test name"
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
	updateConfigVersion := int64(1)
	updateFailoverVersion := int64(59)
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
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
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

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: id,
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		ConfigVersion:     updateConfigVersion + 1,
		FailoverVersion:   updateFailoverVersion + 1,
	}}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)

	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}
