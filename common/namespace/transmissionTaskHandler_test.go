package namespace

import (
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	replicationpb "go.temporal.io/temporal-proto/replication"

	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	p "github.com/temporalio/temporal/common/persistence"
)

type (
	transmissionTaskSuite struct {
		suite.Suite
		namespaceReplicator *namespaceReplicatorImpl
		kafkaProducer       *mocks.KafkaProducer
	}
)

func TestTransmissionTaskSuite(t *testing.T) {
	s := new(transmissionTaskSuite)
	suite.Run(t, s)
}

func (s *transmissionTaskSuite) SetupSuite() {
}

func (s *transmissionTaskSuite) TearDownSuite() {

}

func (s *transmissionTaskSuite) SetupTest() {
	s.kafkaProducer = &mocks.KafkaProducer{}
	s.namespaceReplicator = NewNamespaceReplicator(
		s.kafkaProducer,
		loggerimpl.NewDevelopmentForTest(s.Suite),
	).(*namespaceReplicatorImpl)
}

func (s *transmissionTaskSuite) TearDownTest() {
	s.kafkaProducer.AssertExpectations(s.T())
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_RegisterNamespaceTask_IsGlobalNamespace() {
	taskType := replicationgenpb.ReplicationTaskType_NamespaceTask
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
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	namespaceOperation := replicationgenpb.NamespaceOperation_Create
	info := &p.NamespaceInfo{
		ID:          id,
		Name:        name,
		Status:      p.NamespaceStatusRegistered,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.NamespaceConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		BadBinaries:              namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}
	replicationConfig := &p.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := true

	s.kafkaProducer.On("Publish", &replicationgenpb.ReplicationTask{
		TaskType: taskType,
		Attributes: &replicationgenpb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: &replicationgenpb.NamespaceTaskAttributes{
				NamespaceOperation: namespaceOperation,
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
					BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
				},
				ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
					ActiveClusterName: clusterActive,
					Clusters:          s.namespaceReplicator.convertClusterReplicationConfigToProto(clusters),
				},
				ConfigVersion:   configVersion,
				FailoverVersion: failoverVersion,
			},
		},
	}).Return(nil).Once()

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_RegisterNamespaceTask_NotGlobalNamespace() {
	id := uuid.New()
	name := "some random namespace test name"
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
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	namespaceOperation := replicationgenpb.NamespaceOperation_Create
	info := &p.NamespaceInfo{
		ID:          id,
		Name:        name,
		Status:      p.NamespaceStatusRegistered,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.NamespaceConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		BadBinaries:              namespacepb.BadBinaries{},
	}
	replicationConfig := &p.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := false

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_UpdateNamespaceTask_IsGlobalNamespace() {
	taskType := replicationgenpb.ReplicationTaskType_NamespaceTask
	id := uuid.New()
	name := "some random namespace test name"
	status, _ := s.namespaceReplicator.convertNamespaceStatusToProto(int(namespacepb.NamespaceStatus_Deprecated))
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
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	namespaceOperation := replicationgenpb.NamespaceOperation_Update
	info := &p.NamespaceInfo{
		ID:          id,
		Name:        name,
		Status:      p.NamespaceStatusDeprecated,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.NamespaceConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		BadBinaries:              namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}
	replicationConfig := &p.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := true

	s.kafkaProducer.On("Publish", &replicationgenpb.ReplicationTask{
		TaskType: taskType,
		Attributes: &replicationgenpb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: &replicationgenpb.NamespaceTaskAttributes{
				NamespaceOperation: namespaceOperation,
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
					BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
				},
				ReplicationConfig: &replicationpb.NamespaceReplicationConfiguration{
					ActiveClusterName: clusterActive,
					Clusters:          s.namespaceReplicator.convertClusterReplicationConfigToProto(clusters),
				},
				ConfigVersion:   configVersion,
				FailoverVersion: failoverVersion},
		},
	}).Return(nil).Once()

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}

func (s *transmissionTaskSuite) TestHandleTransmissionTask_UpdateNamespaceTask_NotGlobalNamespace() {
	id := uuid.New()
	name := "some random namespace test name"
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
	clusters := []*p.ClusterReplicationConfig{
		{
			ClusterName: clusterActive,
		},
		{
			ClusterName: clusterStandby,
		},
	}

	namespaceOperation := replicationgenpb.NamespaceOperation_Update
	info := &p.NamespaceInfo{
		ID:          id,
		Name:        name,
		Status:      p.NamespaceStatusDeprecated,
		Description: description,
		OwnerEmail:  ownerEmail,
		Data:        data,
	}
	config := &p.NamespaceConfig{
		Retention:                retention,
		EmitMetric:               emitMetric,
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
	}
	replicationConfig := &p.NamespaceReplicationConfig{
		ActiveClusterName: clusterActive,
		Clusters:          clusters,
	}
	isGlobalNamespace := false

	err := s.namespaceReplicator.HandleTransmissionTask(namespaceOperation, info, config, replicationConfig, configVersion, failoverVersion, isGlobalNamespace)
	s.Nil(err)
}
