package nsreplication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	replicationEventCaptureLogger struct {
		embedded.Logger
		records []otellog.Record
	}

	namespaceReplicationTaskExecutorSuite struct {
		suite.Suite
		controller *gomock.Controller

		mockMetadataMgr     *persistence.MockMetadataManager
		namespaceReplicator *taskExecutorImpl
		eventLogger         *replicationEventCaptureLogger
	}
)

func namespaceUpdateRequestMatcher(expected *persistence.UpdateNamespaceRequest) gomock.Matcher {
	return gomock.Cond(func(actual *persistence.UpdateNamespaceRequest) bool {
		return actual != nil &&
			actual.IsGlobalNamespace == expected.IsGlobalNamespace &&
			actual.NotificationVersion == expected.NotificationVersion &&
			proto.Equal(actual.Namespace, expected.Namespace)
	})
}

func (l *replicationEventCaptureLogger) Emit(_ context.Context, record otellog.Record) {
	l.records = append(l.records, record)
}

func (l *replicationEventCaptureLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

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
	s.eventLogger = &replicationEventCaptureLogger{}
	s.namespaceReplicator = NewTaskExecutor(
		"some random standby cluster name",
		s.mockMetadataMgr,
		NewNoopDataMerger(),
		NewDefaultAdmitter(),
		logger,
		testhooks.TestHooks{},
		WithNamespaceReplicationLifecycleEvents(
			s.eventLogger,
			dynamicconfig.GetBoolPropertyFn(true),
		),
	).(*taskExecutorImpl)
}

func (s *namespaceReplicationTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *namespaceReplicationTaskExecutorSuite) replicationEventContext(
	task *replicationspb.NamespaceTaskAttributes,
) context.Context {
	eventData, ok := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: task,
			},
		},
	)
	s.Require().True(ok)
	return wideevents.SetNamespaceReplicationTaskContext(context.Background(), wideevents.NamespaceReplicationTaskContext{
		SourceCluster: "source-cluster",
		TargetCluster: "target-cluster",
		SourceTaskID:  42,
		AttemptCount:  2,
		EventData:     eventData,
	})
}

func (s *namespaceReplicationTaskExecutorSuite) processedPersistenceRequest(
	outcome wideevents.NamespaceReplicationOutcome,
) map[string]any {
	s.Require().Len(s.eventLogger.records, 1)
	values := make(map[string]otellog.Value)
	s.eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		values[kv.Key] = kv.Value
		return true
	})
	s.Equal("processed", values["phase"].AsString())
	var details map[string]any
	s.Require().NoError(json.Unmarshal([]byte(values["details"].AsString()), &details))
	s.Equal(string(outcome), details["outcome"])
	s.Equal("source-cluster", details["source_cluster"])
	s.Equal("target-cluster", details["target_cluster"])
	s.InDelta(float64(42), details["source_task_id"], 0)
	s.InDelta(float64(2), details["attempt_count"], 0)

	request, ok := details["persistence_request"]
	if !ok {
		return nil
	}
	return request.(map[string]any)
}

func (s *namespaceReplicationTaskExecutorSuite) processedLocalNamespacePreMutation() map[string]any {
	s.Require().Len(s.eventLogger.records, 1)
	values := make(map[string]otellog.Value)
	s.eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		values[kv.Key] = kv.Value
		return true
	})
	var details map[string]any
	s.Require().NoError(json.Unmarshal([]byte(values["details"].AsString()), &details))
	value, ok := details["local_namespace_pre_mutation"]
	s.Require().True(ok)
	return value.(map[string]any)
}

func (s *namespaceReplicationTaskExecutorSuite) TestEmitProcessedRequiresExplicitConfigAndContext() {
	task := &replicationspb.NamespaceTaskAttributes{
		Id:   "namespace-id",
		Info: &namespacepb.NamespaceInfo{Name: "namespace-name"},
	}

	s.namespaceReplicator.emitNamespaceLifecycleEvents = dynamicconfig.GetBoolPropertyFn(false)
	s.namespaceReplicator.emitNamespaceReplicationProcessed(
		s.replicationEventContext(task),
		wideevents.NamespaceReplicationOutcomeNoChange,
		nil,
		nil,
		nil,
	)
	s.Empty(s.eventLogger.records)

	s.namespaceReplicator.emitNamespaceLifecycleEvents = dynamicconfig.GetBoolPropertyFn(true)
	s.namespaceReplicator.emitNamespaceReplicationProcessed(
		context.Background(),
		wideevents.NamespaceReplicationOutcomeNoChange,
		nil,
		nil,
		nil,
	)
	s.Empty(s.eventLogger.records)

	s.namespaceReplicator.emitNamespaceReplicationProcessed(
		s.replicationEventContext(task),
		wideevents.NamespaceReplicationOutcomeNoChange,
		nil,
		nil,
		nil,
	)
	s.Len(s.eventLogger.records, 1)
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
			Id: uuid.NewString(),
		},
	}}, nil)
	task.Id = uuid.NewString()
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
			nsID = uuid.NewString()
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
	replicationState := enumspb.REPLICATION_STATE_NORMAL
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}
	failoverHistory := []*replicationpb.FailoverStatus{
		{
			FailoverTime:    timestamppb.New(time.Date(2025, 9, 15, 14, 30, 0, 0, time.UTC)),
			FailoverVersion: 2,
		},
		{
			FailoverTime:    timestamppb.New(time.Date(2025, 10, 1, 16, 45, 30, 0, time.UTC)),
			FailoverVersion: 11,
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
			State:             replicationState,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
		FailoverHistory: failoverHistory,
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
				State:             replicationState,
				FailoverHistory: []*persistencespb.FailoverStatus{
					{
						FailoverTime:    timestamppb.New(time.Date(2025, 9, 15, 14, 30, 0, 0, time.UTC)),
						FailoverVersion: 2,
					},
					{
						FailoverTime:    timestamppb.New(time.Date(2025, 10, 1, 16, 45, 30, 0, time.UTC)),
						FailoverVersion: 11,
					},
				},
			},
			ConfigVersion:               configVersion,
			FailoverNotificationVersion: 0,
			FailoverVersion:             failoverVersion,
		},
		IsGlobalNamespace: true,
	})
	err := s.namespaceReplicator.Execute(s.replicationEventContext(task), task)
	s.Nil(err)
	request := s.processedPersistenceRequest(wideevents.NamespaceReplicationOutcomeCreated)
	s.Equal("CreateNamespaceRequest", request["request_type"])
	s.Equal(true, request["is_global_namespace"])
	s.Equal(id, request["namespace"].(map[string]any)["info"].(map[string]any)["id"])
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_Duplicate() {
	name := uuid.NewString()
	id := uuid.NewString()
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
	err := s.namespaceReplicator.Execute(s.replicationEventContext(task), task)
	s.Nil(err)
	s.Nil(s.processedPersistenceRequest(wideevents.NamespaceReplicationOutcomeDuplicate))
}

func (s *namespaceReplicationTaskExecutorSuite) TestExecute_RegisterNamespaceTask_NotAdmitted() {
	task := &replicationspb.NamespaceTaskAttributes{
		Id:                 uuid.NewString(),
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Info: &namespacepb.NamespaceInfo{
			Name:  uuid.NewString(),
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
		Config: &namespacepb.NamespaceConfig{},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: "another-cluster"},
			},
		},
	}
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: task.GetInfo().GetName(),
	}).Return(nil, &serviceerror.NamespaceNotFound{})

	err := s.namespaceReplicator.Execute(s.replicationEventContext(task), task)
	s.Require().NoError(err)
	s.Nil(s.processedPersistenceRequest(wideevents.NamespaceReplicationOutcomeNotAdmitted))
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
	err := s.namespaceReplicator.Execute(s.replicationEventContext(updateTask), updateTask)
	s.Nil(err)
	request := s.processedPersistenceRequest(wideevents.NamespaceReplicationOutcomeCreated)
	s.Equal("CreateNamespaceRequest", request["request_type"])
	s.Equal("12", request["namespace"].(map[string]any)["config_version"])
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
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), namespaceUpdateRequestMatcher(&persistence.UpdateNamespaceRequest{
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
				FailoverHistory:   ConvertFailoverHistoryToPersistenceProto(failoverHistory),
			},
			ConfigVersion:               updateConfigVersion,
			FailoverNotificationVersion: updateFailoverVersion,
			FailoverVersion:             updateFailoverVersion,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	}))
	err := s.namespaceReplicator.Execute(s.replicationEventContext(updateTask), updateTask)
	s.Nil(err)
	request := s.processedPersistenceRequest(wideevents.NamespaceReplicationOutcomeUpdated)
	s.Equal("UpdateNamespaceRequest", request["request_type"])
	s.Equal("1", request["namespace"].(map[string]any)["config_version"])
	s.InDelta(float64(updateFailoverVersion), request["notification_version"].(float64), 0)
	localPreMutation := s.processedLocalNamespacePreMutation()
	s.Equal("0", localPreMutation["config_version"])
	s.Equal(id, localPreMutation["info"].(map[string]any)["id"])
	s.Empty(localPreMutation["info"].(map[string]any)["name"])
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
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), namespaceUpdateRequestMatcher(&persistence.UpdateNamespaceRequest{
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
	}))
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
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), namespaceUpdateRequestMatcher(&persistence.UpdateNamespaceRequest{
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
	}))
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
	err := s.namespaceReplicator.Execute(s.replicationEventContext(updateTask), updateTask)
	s.Nil(err)
	s.Nil(s.processedPersistenceRequest(wideevents.NamespaceReplicationOutcomeNoChange))
	localPreMutation := s.processedLocalNamespacePreMutation()
	s.Equal("2", localPreMutation["config_version"])
	s.Equal("60", localPreMutation["failover_version"])
	s.Equal(id, localPreMutation["info"].(map[string]any)["id"])
}

// TestExecute_UpdateNamespaceTask_FailoverPropagatesNormalState verifies the
// self-heal path: a standby stuck at UNSPECIFIED gets State=NORMAL written from
// the next failover task.
func (s *namespaceReplicationTaskExecutorSuite) TestExecute_UpdateNamespaceTask_FailoverPropagatesNormalState() {
	id := uuid.NewString()
	name := "some random namespace test name"
	updateOperation := enumsspb.NAMESPACE_OPERATION_UPDATE
	updateState := enumspb.NAMESPACE_STATE_REGISTERED
	updateClusterActive := "other random active cluster name"
	updateClusterStandby := "other random standby cluster name"
	existingConfigVersion := int64(1)
	existingFailoverVersion := int64(10)
	updateFailoverVersion := int64(59)
	updateClusters := []*replicationpb.ClusterReplicationConfig{
		{ClusterName: updateClusterActive},
		{ClusterName: updateClusterStandby},
	}
	updateTask := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: updateOperation,
		Id:                 id,
		Info: &namespacepb.NamespaceInfo{
			Name:  name,
			State: updateState,
		},
		Config: &namespacepb.NamespaceConfig{},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: updateClusterActive,
			Clusters:          updateClusters,
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
		ConfigVersion:   existingConfigVersion, // no config bump
		FailoverVersion: updateFailoverVersion, // failover bump
	}

	s.namespaceReplicator.currentCluster = updateClusterStandby
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: name,
	}).Return(&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{Id: id},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			State: enumspb.REPLICATION_STATE_UNSPECIFIED, // pre-fix stuck state
		},
		ConfigVersion:   existingConfigVersion,
		FailoverVersion: existingFailoverVersion,
	}}, nil).Times(2)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: updateFailoverVersion,
	}, nil).Times(1)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), namespaceUpdateRequestMatcher(&persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: id},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: updateClusterActive,
				State:             enumspb.REPLICATION_STATE_NORMAL,
			},
			ConfigVersion:               existingConfigVersion,
			FailoverNotificationVersion: updateFailoverVersion,
			FailoverVersion:             updateFailoverVersion,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: updateFailoverVersion,
	}))
	err := s.namespaceReplicator.Execute(context.Background(), updateTask)
	s.NoError(err)
}
