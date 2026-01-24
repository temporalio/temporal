package nsreplication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	namespaceReplicationTaskExecutorSuite struct {
		suite.Suite
		controller *gomock.Controller

		mockMetadataMgr     *persistence.MockMetadataManager
		namespaceReplicator *taskExecutorImpl
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
	s.namespaceReplicator = NewTaskExecutor(
		"some random standby cluster name",
		s.mockMetadataMgr,
		logger,
	).(*taskExecutorImpl)
}

func (s *namespaceReplicationTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_NameUUIDCollision() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.NewString()
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
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterStandby,
		}.Build(),
	}

	task := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: operation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		}.Build(),
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}.Build()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id: uuid.NewString(),
		}.Build(),
	}.Build()}, nil)
	task.SetId(uuid.NewString())
	task.GetInfo().SetName(name)
	err := s.namespaceReplicator.Execute(context.Background(), task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	task.SetId(id)
	task.GetInfo().SetName("other random namespace test name")
	var count int
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: task.GetInfo().GetName(),
	}).DoAndReturn(func(_ context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		nsID := id
		if count != 0 {
			nsID = uuid.NewString()
		}
		count++
		return &persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id: nsID,
			}.Build(),
		}.Build()}, nil
	}).Times(2)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	err = s.namespaceReplicator.Execute(context.Background(), task)
	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_Success() {
	operation := enumsspb.NAMESPACE_OPERATION_CREATE
	id := uuid.NewString()
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
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterStandby,
		}.Build(),
	}
	failoverHistory := []*replicationpb.FailoverStatus{
		replicationpb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(time.Date(2025, 9, 15, 14, 30, 0, 0, time.UTC)),
			FailoverVersion: 2,
		}.Build(),
		replicationpb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(time.Date(2025, 10, 1, 16, 45, 30, 0, time.UTC)),
			FailoverVersion: 11,
		}.Build(),
	}

	task := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: operation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        data,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		}.Build(),
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
		FailoverHistory: failoverHistory,
	}.Build()

	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: name}).Return(
		nil, &serviceerror.NamespaceNotFound{}).Times(1)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), &persistence.CreateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          id,
				State:       task.GetInfo().GetState(),
				Name:        task.GetInfo().GetName(),
				Description: task.GetInfo().GetDescription(),
				Owner:       task.GetInfo().GetOwnerEmail(),
				Data:        task.GetInfo().GetData(),
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:               task.GetConfig().GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:    task.GetConfig().GetHistoryArchivalState(),
				HistoryArchivalUri:      task.GetConfig().GetHistoryArchivalUri(),
				VisibilityArchivalState: task.GetConfig().GetVisibilityArchivalState(),
				VisibilityArchivalUri:   task.GetConfig().GetVisibilityArchivalUri(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: task.GetReplicationConfig().GetActiveClusterName(),
				Clusters:          []string{clusterActive, clusterStandby},
				FailoverHistory: []*persistencespb.FailoverStatus{
					persistencespb.FailoverStatus_builder{
						FailoverTime:    timestamppb.New(time.Date(2025, 9, 15, 14, 30, 0, 0, time.UTC)),
						FailoverVersion: 2,
					}.Build(),
					persistencespb.FailoverStatus_builder{
						FailoverTime:    timestamppb.New(time.Date(2025, 10, 1, 16, 45, 30, 0, time.UTC)),
						FailoverVersion: 11,
					}.Build(),
				},
			}.Build(),
			ConfigVersion:               configVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             failoverVersion,
		}.Build(),
		IsGlobalNamespace: true,
	})
	err := s.namespaceReplicator.Execute(context.Background(), task)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_Duplicate() {
	name := uuid.NewString()
	id := uuid.NewString()
	clusterActive := "some random active cluster name"
	clusterStandby := "some random standby cluster name"
	clusters := []*replicationpb.ClusterReplicationConfig{
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterStandby,
		}.Build(),
	}
	task := replicationspb.NamespaceTaskAttributes_builder{
		Id:                 id,
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Info: namespacepb.NamespaceInfo_builder{
			Name:  name,
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		}.Build(),
		Config: &namespacepb.NamespaceConfig{},
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		}.Build(),
	}.Build()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id: id,
		}.Build(),
	}.Build()}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: id,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Name: name,
		}.Build(),
	}.Build()}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	err := s.namespaceReplicator.Execute(context.Background(), task)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NamespaceNotExist() {
	operation := enumsspb.NAMESPACE_OPERATION_UPDATE
	id := uuid.NewString()
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
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: clusterStandby,
		}.Build(),
	}

	updateTask := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: operation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       state,
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        namespaceData,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
			HistoryArchivalState:          historyArchivalState,
			HistoryArchivalUri:            historyArchivalURI,
			VisibilityArchivalState:       visibilityArchivalState,
			VisibilityArchivalUri:         visibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		}.Build(),
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}.Build()

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 0}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: name}).Return(
		nil, &serviceerror.NamespaceNotFound{}).Times(2)
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), &persistence.CreateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          id,
				State:       updateTask.GetInfo().GetState(),
				Name:        updateTask.GetInfo().GetName(),
				Description: updateTask.GetInfo().GetDescription(),
				Owner:       updateTask.GetInfo().GetOwnerEmail(),
				Data:        updateTask.GetInfo().GetData(),
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:               updateTask.GetConfig().GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:    updateTask.GetConfig().GetHistoryArchivalState(),
				HistoryArchivalUri:      updateTask.GetConfig().GetHistoryArchivalUri(),
				VisibilityArchivalState: updateTask.GetConfig().GetVisibilityArchivalState(),
				VisibilityArchivalUri:   updateTask.GetConfig().GetVisibilityArchivalUri(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: updateTask.GetReplicationConfig().GetActiveClusterName(),
				Clusters:          []string{clusterActive, clusterStandby},
			}.Build(),
			ConfigVersion:               configVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             failoverVersion,
		}.Build(),
		IsGlobalNamespace: true,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_UpdateActiveCluster() {
	id := uuid.NewString()
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
		replicationpb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(failoverTime),
			FailoverVersion: 999,
		}.Build(),
	}
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterStandby,
		}.Build(),
	}
	updateTask := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
			HistoryArchivalState:          updateHistoryArchivalState,
			HistoryArchivalUri:            updateHistoryArchivalURI,
			VisibilityArchivalState:       updateVisibilityArchivalState,
			VisibilityArchivalUri:         updateVisibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		}.Build(),
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
		FailoverHistory: failoverHistory,
	}.Build()

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id: id,
		}.Build(),
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
	}.Build()}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          id,
				State:       updateTask.GetInfo().GetState(),
				Name:        updateTask.GetInfo().GetName(),
				Description: updateTask.GetInfo().GetDescription(),
				Owner:       updateTask.GetInfo().GetOwnerEmail(),
				Data:        updateTask.GetInfo().GetData(),
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:               updateTask.GetConfig().GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:    updateTask.GetConfig().GetHistoryArchivalState(),
				HistoryArchivalUri:      updateTask.GetConfig().GetHistoryArchivalUri(),
				VisibilityArchivalState: updateTask.GetConfig().GetVisibilityArchivalState(),
				VisibilityArchivalUri:   updateTask.GetConfig().GetVisibilityArchivalUri(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: updateTask.GetReplicationConfig().GetActiveClusterName(),
				Clusters:          []string{updateClusterActive, updateClusterStandby},
				FailoverHistory:   ConvertFailoverHistoryToPersistenceProto(failoverHistory),
			}.Build(),
			ConfigVersion:               updateConfigVersion,
			FailoverNotificationVersion: updateFailoverVersion,
			FailoverVersion:             updateFailoverVersion,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_UpdateConfig_NoUpdateActiveCluster() {
	id := uuid.NewString()
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
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterStandby,
		}.Build(),
	}
	updateTask := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
			HistoryArchivalState:          updateHistoryArchivalState,
			HistoryArchivalUri:            updateHistoryArchivalURI,
			VisibilityArchivalState:       updateVisibilityArchivalState,
			VisibilityArchivalUri:         updateVisibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		}.Build(),
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}.Build()

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id: id,
		}.Build(),
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		FailoverVersion:   updateFailoverVersion + 1,
	}.Build()}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          id,
				State:       updateTask.GetInfo().GetState(),
				Name:        updateTask.GetInfo().GetName(),
				Description: updateTask.GetInfo().GetDescription(),
				Owner:       updateTask.GetInfo().GetOwnerEmail(),
				Data:        updateTask.GetInfo().GetData(),
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:               updateTask.GetConfig().GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:    updateTask.GetConfig().GetHistoryArchivalState(),
				HistoryArchivalUri:      updateTask.GetConfig().GetHistoryArchivalUri(),
				VisibilityArchivalState: updateTask.GetConfig().GetVisibilityArchivalState(),
				VisibilityArchivalUri:   updateTask.GetConfig().GetVisibilityArchivalUri(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				Clusters: []string{updateClusterActive, updateClusterStandby},
			}.Build(),
			ConfigVersion:               updateConfigVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             updateFailoverVersion + 1,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_UpdateActiveCluster() {
	id := uuid.NewString()
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
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterStandby,
		}.Build(),
	}
	updateTask := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
			HistoryArchivalState:          updateHistoryArchivalState,
			HistoryArchivalUri:            updateHistoryArchivalURI,
			VisibilityArchivalState:       updateVisibilityArchivalState,
			VisibilityArchivalUri:         updateVisibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		}.Build(),
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}.Build()

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id: id,
		}.Build(),
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		ConfigVersion:     updateConfigVersion + 1,
	}.Build()}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id: id,
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: updateClusterActive,
			}.Build(),
			ConfigVersion:               updateConfigVersion + 1,
			FailoverNotificationVersion: updateFailoverVersion,
			FailoverVersion:             updateFailoverVersion,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	})
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_NoUpdateConfig_NoUpdateActiveCluster() {
	id := uuid.NewString()
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
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterActive,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: updateClusterStandby,
		}.Build(),
	}
	updateTask := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: namespacepb.NamespaceInfo_builder{
			Name:        name,
			State:       updateState,
			Description: updateDescription,
			OwnerEmail:  updateOwnerEmail,
			Data:        updatedData,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: durationpb.New(updateRetention),
			HistoryArchivalState:          updateHistoryArchivalState,
			HistoryArchivalUri:            updateHistoryArchivalURI,
			VisibilityArchivalState:       updateVisibilityArchivalState,
			VisibilityArchivalUri:         updateVisibilityArchivalURI,
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
		}.Build(),
		ConfigVersion:   updateConfigVersion,
		FailoverVersion: updateFailoverVersion,
	}.Build()

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id: id,
		}.Build(),
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		ConfigVersion:     updateConfigVersion + 1,
		FailoverVersion:   updateFailoverVersion + 1,
	}.Build()}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)

	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.Nil(err)
}
