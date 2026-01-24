package frontend

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	namespaceHandlerCommonSuite struct {
		suite.Suite

		controller *gomock.Controller

		maxBadBinaryCount       int
		mockMetadataMgr         *persistence.MockMetadataManager
		mockClusterMetadata     *cluster.MockMetadata
		mockProducer            *persistence.MockNamespaceReplicationQueue
		mockNamespaceReplicator nsreplication.Replicator
		archivalMetadata        archiver.ArchivalMetadata
		mockArchiverProvider    *provider.MockArchiverProvider
		fakeClock               *clock.EventTimeSource
		config                  *Config

		handler *namespaceHandler
	}
)

var now = time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC)

func TestNamespaceHandlerCommonSuite(t *testing.T) {
	s := new(namespaceHandlerCommonSuite)
	suite.Run(t, s)
}

func (s *namespaceHandlerCommonSuite) SetupSuite() {
}

func (s *namespaceHandlerCommonSuite) TearDownSuite() {
}

func (s *namespaceHandlerCommonSuite) SetupTest() {
	logger := log.NewNoopLogger()
	dcCollection := dc.NewNoopCollection()
	s.controller = gomock.NewController(s.T())
	s.maxBadBinaryCount = 10
	s.mockMetadataMgr = persistence.NewMockMetadataManager(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockNamespaceReplicator = nsreplication.NewReplicator(s.mockProducer, logger)
	s.archivalMetadata = archiver.NewArchivalMetadata(
		dcCollection,
		"",
		false,
		"",
		false,
		&config.ArchivalNamespaceDefaults{},
	)
	s.mockArchiverProvider = provider.NewMockArchiverProvider(s.controller)
	s.fakeClock = clock.NewEventTimeSource()
	s.config = NewConfig(dc.NewNoopCollection(), 1024)
	s.handler = newNamespaceHandler(
		logger,
		s.mockMetadataMgr,
		s.mockClusterMetadata,
		s.mockNamespaceReplicator,
		s.archivalMetadata,
		s.mockArchiverProvider,
		s.fakeClock,
		s.config,
	)
}

func (s *namespaceHandlerCommonSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *namespaceHandlerCommonSuite) TestMergeNamespaceData_Overriding() {
	out := s.handler.mergeNamespaceData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v2",
	}, out)
}

func (s *namespaceHandlerCommonSuite) TestMergeNamespaceData_Adding() {
	out := s.handler.mergeNamespaceData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v0",
		"k1": "v2",
	}, out)
}

func (s *namespaceHandlerCommonSuite) TestMergeNamespaceData_Merging() {
	out := s.handler.mergeNamespaceData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

func (s *namespaceHandlerCommonSuite) TestMergeNamespaceData_Nil() {
	out := s.handler.mergeNamespaceData(
		nil,
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

// test merging bad binaries
func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Overriding() {
	out := s.handler.mergeBadBinaries(
		map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason0"}.Build(),
		},
		map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason2"}.Build(),
		}, now,
	)

	protoassert.ProtoEqual(s.T(), &out, namespacepb.BadBinaries_builder{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason2", CreateTime: timestamppb.New(now)}.Build(),
		},
	}.Build())
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Adding() {
	out := s.handler.mergeBadBinaries(
		map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason0"}.Build(),
		},
		map[string]*namespacepb.BadBinaryInfo{
			"k1": namespacepb.BadBinaryInfo_builder{Reason: "reason2"}.Build(),
		}, now,
	)

	expected := namespacepb.BadBinaries_builder{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason0"}.Build(),
			"k1": namespacepb.BadBinaryInfo_builder{Reason: "reason2", CreateTime: timestamppb.New(now)}.Build(),
		},
	}.Build()
	assert.Equal(s.T(), out.String(), expected.String())
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Merging() {
	out := s.handler.mergeBadBinaries(
		map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason0"}.Build(),
		},
		map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason1"}.Build(),
			"k1": namespacepb.BadBinaryInfo_builder{Reason: "reason2"}.Build(),
		}, now,
	)

	protoassert.ProtoEqual(s.T(), &out, namespacepb.BadBinaries_builder{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason1", CreateTime: timestamppb.New(now)}.Build(),
			"k1": namespacepb.BadBinaryInfo_builder{Reason: "reason2", CreateTime: timestamppb.New(now)}.Build(),
		},
	}.Build())
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Nil() {
	out := s.handler.mergeBadBinaries(
		nil,
		map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason1"}.Build(),
			"k1": namespacepb.BadBinaryInfo_builder{Reason: "reason2"}.Build(),
		}, now,
	)

	protoassert.ProtoEqual(s.T(), &out, namespacepb.BadBinaries_builder{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": namespacepb.BadBinaryInfo_builder{Reason: "reason1", CreateTime: timestamppb.New(now)}.Build(),
			"k1": namespacepb.BadBinaryInfo_builder{Reason: "reason2", CreateTime: timestamppb.New(now)}.Build(),
		},
	}.Build())
}

func (s *namespaceHandlerCommonSuite) TestListNamespace() {
	description1 := "some random description 1"
	email1 := "some random email 1"
	retention1 := 1 * time.Hour * 24
	data1 := map[string]string{"some random key 1": "some random value 1"}
	isGlobalNamespace1 := false
	cluster1 := "cluster1"
	cluster2 := "cluster2"
	description2 := "some random description 2"
	email2 := "some random email 2"
	retention2 := 2 * time.Hour * 24
	data2 := map[string]string{"some random key 2": "some random value 2"}
	isGlobalNamespace2 := true
	namespace1 := persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id:          uuid.NewString(),
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Name:        s.getRandomNamespace(),
			Description: description1,
			Owner:       email1,
			Data:        data1,
		}.Build(),
		Config: persistencespb.NamespaceConfig_builder{
			Retention: durationpb.New(retention1),
		}.Build(),
		ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster1,
			Clusters:          []string{cluster1},
		}.Build(),
		ConfigVersion:               0,
		FailoverNotificationVersion: 0,
		FailoverVersion:             0,
		FailoverEndTime:             nil,
	}.Build()
	namespace2 := persistencespb.NamespaceDetail_builder{
		Info: persistencespb.NamespaceInfo_builder{
			Id:          uuid.NewString(),
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Name:        s.getRandomNamespace(),
			Description: description2,
			Owner:       email2,
			Data:        data2,
		}.Build(),
		Config: persistencespb.NamespaceConfig_builder{
			Retention: durationpb.New(retention2),
		}.Build(),
		ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster2,
			Clusters:          []string{cluster1, cluster2},
		}.Build(),
		ConfigVersion:               0,
		FailoverNotificationVersion: 0,
		FailoverVersion:             0,
		FailoverEndTime:             nil,
	}.Build()
	s.mockMetadataMgr.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:      1,
		NextPageToken: nil,
	}).Return(
		&persistence.ListNamespacesResponse{
			Namespaces: []*persistence.GetNamespaceResponse{
				{
					Namespace:         namespace1,
					IsGlobalNamespace: isGlobalNamespace1,
				},
			},
			NextPageToken: []byte{1},
		}, nil,
	)
	s.mockMetadataMgr.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:      1,
		NextPageToken: []byte{1},
	}).Return(
		&persistence.ListNamespacesResponse{
			Namespaces: []*persistence.GetNamespaceResponse{
				{
					Namespace:         namespace2,
					IsGlobalNamespace: isGlobalNamespace2,
				},
			},
			NextPageToken: nil,
		}, nil,
	)
	namespaces := map[string]*workflowservice.DescribeNamespaceResponse{}
	pagesize := int32(1)
	var token []byte
	for doPaging := true; doPaging; doPaging = len(token) > 0 {
		resp, err := s.handler.ListNamespaces(context.Background(), workflowservice.ListNamespacesRequest_builder{
			PageSize:      pagesize,
			NextPageToken: token,
		}.Build())
		s.NoError(err)
		token = resp.GetNextPageToken()
		s.True(len(resp.GetNamespaces()) <= int(pagesize))
		if len(resp.GetNamespaces()) > 0 {
			namespaces[resp.GetNamespaces()[0].GetNamespaceInfo().GetName()] = resp.GetNamespaces()[0]
		}
	}
	expectedResult := map[string]*persistencespb.NamespaceDetail{
		namespace1.GetInfo().GetName(): namespace1,
		namespace2.GetInfo().GetName(): namespace2,
	}
	for name, ns := range namespaces {
		s.Equal(expectedResult[name].GetInfo().GetName(), ns.GetNamespaceInfo().GetName())
		s.Equal(expectedResult[name].GetInfo().GetState(), ns.GetNamespaceInfo().GetState())
		s.Equal(expectedResult[name].GetInfo().GetDescription(), ns.GetNamespaceInfo().GetDescription())
		s.Equal(expectedResult[name].GetInfo().GetOwner(), ns.GetNamespaceInfo().GetOwnerEmail())
		s.Equal(expectedResult[name].GetInfo().GetData(), ns.GetNamespaceInfo().GetData())
		s.Equal(expectedResult[name].GetInfo().GetId(), ns.GetNamespaceInfo().GetId())
		s.Equal(expectedResult[name].GetConfig().GetRetention(), ns.GetConfig().GetWorkflowExecutionRetentionTtl())
		s.Equal(expectedResult[name].GetConfig().GetHistoryArchivalState(), ns.GetConfig().GetHistoryArchivalState())
		s.Equal(expectedResult[name].GetConfig().GetHistoryArchivalUri(), ns.GetConfig().GetHistoryArchivalUri())
		s.Equal(expectedResult[name].GetConfig().GetVisibilityArchivalState(), ns.GetConfig().GetVisibilityArchivalState())
		s.Equal(expectedResult[name].GetConfig().GetVisibilityArchivalUri(), ns.GetConfig().GetVisibilityArchivalUri())
		s.Equal(expectedResult[name].GetConfig().GetBadBinaries(), ns.GetConfig().GetBadBinaries())
		s.Equal(expectedResult[name].GetReplicationConfig().GetActiveClusterName(), ns.GetReplicationConfig().GetActiveClusterName())
		s.Equal(expectedResult[name].GetReplicationConfig().GetClusters(), nsreplication.ConvertClusterReplicationConfigFromProto(ns.GetReplicationConfig().GetClusters()))
		s.Equal(expectedResult[name].GetReplicationConfig().GetState(), ns.GetReplicationConfig().GetState())
		s.Equal(expectedResult[name].GetFailoverVersion(), ns.GetFailoverVersion())
	}
}

func (s *namespaceHandlerCommonSuite) TestCapabilitiesAndLimits() {
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "ns",
	}).Return(
		&persistence.GetNamespaceResponse{
			Namespace: persistencespb.NamespaceDetail_builder{
				Info: persistencespb.NamespaceInfo_builder{
					Id: "id",
				}.Build(),
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			}.Build(),
		}, nil,
	).AnyTimes()

	// First call: Use default value of dynamic configs.
	resp, err := s.handler.DescribeNamespace(context.Background(), workflowservice.DescribeNamespaceRequest_builder{
		Namespace: "ns",
	}.Build())
	s.NoError(err)

	s.True(resp.GetNamespaceInfo().GetCapabilities().GetEagerWorkflowStart())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetSyncUpdate())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetAsyncUpdate())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetReportedProblemsSearchAttribute())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetWorkerHeartbeats())
	s.False(resp.GetNamespaceInfo().GetCapabilities().GetWorkflowPause())
	s.False(resp.GetNamespaceInfo().GetCapabilities().GetStandaloneActivities())
	s.Equal(int64(2*1024*1024), resp.GetNamespaceInfo().GetLimits().GetBlobSizeLimitError())
	s.Equal(int64(2*1024*1024), resp.GetNamespaceInfo().GetLimits().GetMemoSizeLimitError())

	// Second call: Override the default value of dynamic configs.
	s.config.EnableEagerWorkflowStart = dc.GetBoolPropertyFnFilteredByNamespace(false)
	s.config.EnableUpdateWorkflowExecution = dc.GetBoolPropertyFnFilteredByNamespace(false)
	s.config.EnableUpdateWorkflowExecutionAsyncAccepted = dc.GetBoolPropertyFnFilteredByNamespace(false)
	s.config.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute = dc.GetIntPropertyFnFilteredByNamespace(5)
	s.config.WorkerHeartbeatsEnabled = dc.GetBoolPropertyFnFilteredByNamespace(false)
	s.config.WorkflowPauseEnabled = dc.GetBoolPropertyFnFilteredByNamespace(true)
	s.config.Activity.Enabled = dc.GetBoolPropertyFnFilteredByNamespace(true)
	s.config.BlobSizeLimitError = dc.GetIntPropertyFnFilteredByNamespace(1024)
	s.config.MemoSizeLimitError = dc.GetIntPropertyFnFilteredByNamespace(512)

	resp, err = s.handler.DescribeNamespace(context.Background(), workflowservice.DescribeNamespaceRequest_builder{
		Namespace: "ns",
	}.Build())
	s.NoError(err)
	s.False(resp.GetNamespaceInfo().GetCapabilities().GetEagerWorkflowStart())
	s.False(resp.GetNamespaceInfo().GetCapabilities().GetSyncUpdate())
	s.False(resp.GetNamespaceInfo().GetCapabilities().GetAsyncUpdate())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetReportedProblemsSearchAttribute())
	s.False(resp.GetNamespaceInfo().GetCapabilities().GetWorkerHeartbeats())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetWorkflowPause())
	s.True(resp.GetNamespaceInfo().GetCapabilities().GetStandaloneActivities())
	s.Equal(int64(1024), resp.GetNamespaceInfo().GetLimits().GetBlobSizeLimitError())
	s.Equal(int64(512), resp.GetNamespaceInfo().GetLimits().GetMemoSizeLimitError())
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_WithOneCluster() {
	const namespace = "namespace-to-register"
	clusterName := "cluster1"
	retention := durationpb.New(10 * 24 * time.Hour)
	registerRequest := workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	}.Build()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName, int64(0)).Return(int64(1))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(namespace, request.Namespace.GetInfo().GetDescription())
			s.Equal(registerRequest.GetIsGlobalNamespace(), request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(clusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal(int64(1), request.Namespace.GetFailoverVersion())
			return &persistence.CreateNamespaceResponse{}, nil
		})
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil).Times(0)
	_, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_WithTwoCluster() {
	const namespace = "namespace-to-register"
	clusterName := "cluster1"
	clusterName2 := "cluster2"
	retention := durationpb.New(10 * 24 * time.Hour)
	registerRequest := workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: retention,
		ActiveClusterName:                clusterName,
		Clusters: []*replicationpb.ClusterReplicationConfig{
			replicationpb.ClusterReplicationConfig_builder{
				ClusterName: clusterName,
			}.Build(),
			replicationpb.ClusterReplicationConfig_builder{
				ClusterName: clusterName2,
			}.Build(),
		},
		IsGlobalNamespace: true,
	}.Build()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		clusterName2: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName, int64(0)).Return(int64(1))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(namespace, request.Namespace.GetInfo().GetDescription())
			s.Equal(registerRequest.GetIsGlobalNamespace(), request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(clusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal(int64(1), request.Namespace.GetFailoverVersion())
			return &persistence.CreateNamespaceResponse{}, nil
		})
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	_, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_InvalidRetentionPeriod() {
	clusterName := "cluster1"
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName).AnyTimes()

	// local
	for _, invalidDuration := range []time.Duration{
		0,
		-1 * time.Hour,
		1 * time.Millisecond,
		30 * time.Minute,
	} {
		registerRequest := workflowservice.RegisterNamespaceRequest_builder{
			Namespace:                        "random namespace name",
			Description:                      "random namespace name",
			WorkflowExecutionRetentionPeriod: durationpb.New(invalidDuration),
			IsGlobalNamespace:                false,
		}.Build()
		resp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}

	// global
	for _, invalidDuration := range []time.Duration{0, -1 * time.Hour, 1 * time.Millisecond, 10 * time.Hour} {
		registerRequest := workflowservice.RegisterNamespaceRequest_builder{
			Namespace:                        "random namespace name",
			Description:                      "random namespace name",
			WorkflowExecutionRetentionPeriod: durationpb.New(invalidDuration),
			IsGlobalNamespace:                true,
		}.Build()
		resp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_InvalidRetentionPeriod() {
	namespace := uuid.NewString()
	version := int64(1)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   uuid.NewString(),
				Name: namespace,
			}.Build(),
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		}.Build(),
	}, nil).AnyTimes()
	for _, invalidDuration := range []time.Duration{
		0,
		-1 * time.Hour,
		1 * time.Millisecond,
		30 * time.Minute,
	} {
		updateRequest := workflowservice.UpdateNamespaceRequest_builder{
			Namespace: namespace,
			Config: namespacepb.NamespaceConfig_builder{
				WorkflowExecutionRetentionTtl: durationpb.New(invalidDuration),
			}.Build(),
		}.Build()
		resp, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_PromoteLocalNamespace() {
	namespace := "local-ns-to-be-promoted"
	clusterName := "cluster1"
	version := int64(1)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName,
				Clusters:          []string{clusterName},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName,
				Clusters:          []string{clusterName},
			}.Build(),
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             2,
		}.Build(),
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	})
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName, int64(0)).Return(int64(2))

	updateRequest := workflowservice.UpdateNamespaceRequest_builder{
		Namespace:        namespace,
		PromoteNamespace: true,
	}.Build()
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_UpdateActiveClusterWithHandoverState() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.NewString()
	version := int64(100)
	clusterName1 := "cluster1"
	clusterName2 := "cluster2"
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName1: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		clusterName2: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName1).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName2, int64(0)).Return(int64(2))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:    nid,
				Name:  namespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_HANDOVER,
			}.Build(),
		}.Build(),
		IsGlobalNamespace: true,
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Eq(&persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:    nid,
				Name:  namespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_NORMAL,
				FailoverHistory: []*persistencespb.FailoverStatus{
					persistencespb.FailoverStatus_builder{
						FailoverTime:    timestamppb.New(update1Time),
						FailoverVersion: 2,
					}.Build(),
				},
			}.Build(),
			ConfigVersion:               int64(0),
			FailoverNotificationVersion: version,
			FailoverVersion:             int64(2),
		}.Build(),
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	}))
	s.fakeClock.Update(update1Time)
	updateRequest := workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterName2,
		}.Build(),
		PromoteNamespace: true,
	}.Build()
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_ChangeActiveClusterWithoutUpdatingReplicationState() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.NewString()
	version := int64(100)
	clusterName1 := "cluster1"
	clusterName2 := "cluster2"
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName1: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		clusterName2: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName1).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName2, int64(0)).Return(int64(2))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_NORMAL,
				FailoverHistory: []*persistencespb.FailoverStatus{
					persistencespb.FailoverStatus_builder{
						FailoverTime:    timestamppb.New(update1Time),
						FailoverVersion: 2,
					}.Build(),
				},
			}.Build(),
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             2,
		}.Build(),
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	})
	s.fakeClock.Update(update1Time)
	updateRequest := workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterName2,
		}.Build(),
		PromoteNamespace: true,
	}.Build()
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

// Test that the number of replication statuses is limited
func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_UpdateActiveCluster_LimitRecordHistory() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.NewString()
	version := int64(100)
	clusterName1 := "cluster1"
	clusterName2 := "cluster2"
	failoverHistory := []*persistencespb.FailoverStatus{
		persistencespb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(2),
		}.Build(),
		persistencespb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(11),
		}.Build(),
		persistencespb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(12),
		}.Build(),
		persistencespb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(21),
		}.Build(),
		persistencespb.FailoverStatus_builder{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(22),
		}.Build(),
	}
	updateRequest := workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: "cluster2",
		}.Build(),
		PromoteNamespace: true,
	}.Build()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName1: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		clusterName2: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName1).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName2, int64(0)).Return(int64(32))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
				FailoverHistory:   failoverHistory,
			}.Build(),
		}.Build(),
	}, nil)
	sizeLimitedFailoverHistory := slices.Clone(failoverHistory)
	sizeLimitedFailoverHistory = append(sizeLimitedFailoverHistory, persistencespb.FailoverStatus_builder{
		FailoverTime:    timestamppb.New(update1Time),
		FailoverVersion: 32,
	}.Build())
	sizeLimitedFailoverHistory = sizeLimitedFailoverHistory[0:]
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_NORMAL,
				FailoverHistory:   sizeLimitedFailoverHistory,
			}.Build(),
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             32,
		}.Build(),
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	})
	s.fakeClock.Update(update1Time)
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterLocalNamespace_InvalidGlobalNamespace() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: activeClusterName,
		}.Build(),
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := true
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		activeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(activeClusterName).AnyTimes()

	resp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	}.Build())
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(resp)
}

func (s *namespaceHandlerCommonSuite) TestRegisterLocalNamespace_InvalidCluster() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	activeClusterName := cluster.TestAlternativeClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: activeClusterName,
		}.Build(),
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := false
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})

	resp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	}.Build())
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(resp)
}

func (s *namespaceHandlerCommonSuite) TestRegisterLocalNamespace_AllDefault() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(time.Hour)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.NotEmpty(request.Namespace.GetInfo().GetId())
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(false, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal([]string{cluster.TestCurrentClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
	}.Build())
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)
}

func (s *namespaceHandlerCommonSuite) TestRegisterLocalNamespace_NoDefault() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := durationpb.New(7 * time.Hour * 24)
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: activeClusterName,
		}.Build(),
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := false

	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.NotEmpty(request.Namespace.GetInfo().GetId())
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(description, request.Namespace.GetInfo().GetDescription())
			s.Equal(email, request.Namespace.GetInfo().GetOwner())
			s.Equal(data, request.Namespace.GetInfo().GetData())
			s.Equal(false, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(activeClusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal([]string{activeClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: retention,
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	}.Build())
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)
}

func (s *namespaceHandlerCommonSuite) TestUpdateLocalNamespace_NoAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention: durationpb.New(retention),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
	}.Build())
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateLocalNamespace_AllAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := durationpb.New(7 * time.Hour * 24)
	activeClusterName := cluster.TestCurrentClusterName
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		activeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(activeClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: activeClusterName,
				Clusters:          []string{activeClusterName},
				State:             enumspb.REPLICATION_STATE_NORMAL,
			}.Build(),
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		UpdateInfo: namespacepb.UpdateNamespaceInfo_builder{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: activeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				replicationpb.ClusterReplicationConfig_builder{ClusterName: activeClusterName}.Build(),
			},
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterGlobalNamespace_AllDefault() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(24 * time.Hour)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil).Times(0)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(cluster.TestCurrentClusterName, gomock.Any()).Return(int64(1))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.NotEmpty(request.Namespace.GetInfo().GetId())
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(true, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal([]string{cluster.TestCurrentClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	}.Build())
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)
}

func (s *namespaceHandlerCommonSuite) TestRegisterGlobalNamespace_NoDefault() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(24 * time.Hour)
	description := "description"
	email := "email"
	clusters := []*replicationpb.ClusterReplicationConfig{
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: cluster.TestCurrentClusterName,
		}.Build(),
		replicationpb.ClusterReplicationConfig_builder{
			ClusterName: cluster.TestAlternativeClusterName,
		}.Build(),
	}
	data := map[string]string{"some random key": "some random value"}
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(cluster.TestCurrentClusterName, gomock.Any()).Return(int64(1))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.NotEmpty(request.Namespace.GetInfo().GetId())
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(true, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal([]string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: retention,
		Clusters:                         clusters,
		ActiveClusterName:                cluster.TestCurrentClusterName,
		Data:                             data,
		IsGlobalNamespace:                true,
	}.Build())
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)
}

func (s *namespaceHandlerCommonSuite) TestUpdateGlobalNamespace_NoAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention: durationpb.New(retention),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
		}.Build(),
		IsGlobalNamespace: true,
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
	}.Build())
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateGlobalNamespace_AllAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := durationpb.New(7 * time.Hour * 24)
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		UpdateInfo: namespacepb.UpdateNamespaceInfo_builder{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterLocalNamespace_NotMaster() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(time.Hour)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			s.NotEmpty(request.Namespace.GetInfo().GetId())
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.GetInfo().GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(false, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().GetActiveClusterName())
			s.Equal([]string{cluster.TestCurrentClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
	}.Build())
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)
}

func (s *namespaceHandlerCommonSuite) TestUpdateLocalNamespace_NotMaster() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := durationpb.New(7 * time.Hour * 24)
	activeClusterName := cluster.TestCurrentClusterName
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		activeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(activeClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: activeClusterName,
				Clusters:          []string{activeClusterName},
			}.Build(),
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		UpdateInfo: namespacepb.UpdateNamespaceInfo_builder{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterGlobalNamespace_NotMaster() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(24 * time.Hour)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(false).AnyTimes()

	_, err := s.handler.RegisterNamespace(context.Background(), workflowservice.RegisterNamespaceRequest_builder{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	}.Build())
	s.Error(err)
	s.Equal(errNotMasterCluster, err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateGlobalNamespace_NotMaster() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := durationpb.New(7 * time.Hour * 24)
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.NewString()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:   retention,
				BadBinaries: namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			}.Build(),
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		}.Build(),
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		UpdateInfo: namespacepb.UpdateNamespaceInfo_builder{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   namespacepb.BadBinaries_builder{Binaries: map[string]*namespacepb.BadBinaryInfo{}}.Build(),
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestFailoverGlobalNamespace_NotMaster() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.NewString()
	version := int64(100)
	clusterName1 := "cluster1"
	clusterName2 := "cluster2"
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		clusterName1: {
			Enabled:                true,
			InitialFailoverVersion: 1,
		},
		clusterName2: {
			Enabled:                true,
			InitialFailoverVersion: 2,
		},
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName1).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(clusterName2, int64(0)).Return(int64(2))
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
			}.Build(),
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   nid,
				Name: namespace,
			}.Build(),
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_NORMAL,
				FailoverHistory: []*persistencespb.FailoverStatus{
					persistencespb.FailoverStatus_builder{
						FailoverTime:    timestamppb.New(update1Time),
						FailoverVersion: 2,
					}.Build(),
				},
			}.Build(),
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             2,
		}.Build(),
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	})
	s.fakeClock.Update(update1Time)
	updateRequest := workflowservice.UpdateNamespaceRequest_builder{
		Namespace: namespace,
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: clusterName2,
		}.Build(),
		PromoteNamespace: true,
	}.Build()
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestCreateWorkflowRule_Acceptance() {
	namespaceName := "test-namespace"
	identity := "identity"
	description := "description"
	spec := rulespb.WorkflowRuleSpec_builder{
		Id: "",
	}.Build()
	version := int64(100)

	// first call returns error, because ID is not set
	_, err := s.handler.CreateWorkflowRule(context.Background(), spec, identity, description, namespaceName)
	s.Error(err)

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   "1",
				Name: namespaceName,
			}.Build(),
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		}.Build(),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)

	spec.SetId("test-id")
	rule, err := s.handler.CreateWorkflowRule(context.Background(), spec, identity, description, namespaceName)
	s.NoError(err)
	s.NotNil(rule)
	s.NotNil(rule.GetSpec())
	s.NotNil(rule.GetCreateTime())
	s.Equal(identity, rule.GetCreatedByIdentity())
	s.Equal(description, rule.GetDescription())
}

func (s *namespaceHandlerCommonSuite) TestCreateWorkflowRule_Duplicate() {
	namespaceName := "test-namespace"
	identity := "identity"
	description := "description"
	ruleId := "test-id"
	spec := rulespb.WorkflowRuleSpec_builder{
		Id: ruleId,
	}.Build()
	version := int64(100)

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   "1",
				Name: namespaceName,
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				WorkflowRules: map[string]*rulespb.WorkflowRule{
					ruleId: rulespb.WorkflowRule_builder{
						Spec: rulespb.WorkflowRuleSpec_builder{Id: ruleId}.Build(),
					}.Build(),
				},
			}.Build(),
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		}.Build(),
	}, nil)

	_, err := s.handler.CreateWorkflowRule(context.Background(), spec, identity, description, namespaceName)
	s.Error(err)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *namespaceHandlerCommonSuite) TestDeleteWorkflowRule() {
	namespaceName := "test-namespace"
	ruleId := "test-id"
	nsConfig := persistencespb.NamespaceConfig_builder{
		WorkflowRules: map[string]*rulespb.WorkflowRule{
			ruleId: rulespb.WorkflowRule_builder{
				Spec: rulespb.WorkflowRuleSpec_builder{Id: ruleId}.Build(),
			}.Build(),
		},
	}.Build()

	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(1),
	}, nil).AnyTimes()

	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   "1",
				Name: namespaceName,
			}.Build(),
			Config:            nsConfig,
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		}.Build(),
	}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// happy path
	err := s.handler.DeleteWorkflowRule(context.Background(), ruleId, namespaceName)
	s.NoError(err)

	var invalidArgument *serviceerror.InvalidArgument

	// rule with such id doesn't exist
	err = s.handler.DeleteWorkflowRule(context.Background(), "not existing rule id", namespaceName)
	s.Error(err)
	s.ErrorAs(err, &invalidArgument)

	// config is nil
	nsConfig.SetWorkflowRules(nil)
	err = s.handler.DeleteWorkflowRule(context.Background(), "not existing rule id", namespaceName)
	s.Error(err)
	s.ErrorAs(err, &invalidArgument)
}

func (s *namespaceHandlerCommonSuite) TestDescribeWorkflowRule() {
	namespaceName := "test-namespace"
	ruleId := "test-id"
	nsConfig := persistencespb.NamespaceConfig_builder{
		WorkflowRules: map[string]*rulespb.WorkflowRule{
			ruleId: rulespb.WorkflowRule_builder{
				Spec: rulespb.WorkflowRuleSpec_builder{Id: ruleId}.Build(),
			}.Build(),
		},
	}.Build()

	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   "1",
				Name: namespaceName,
			}.Build(),
			Config:            nsConfig,
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		}.Build(),
	}, nil).AnyTimes()

	// happy path
	rule, err := s.handler.DescribeWorkflowRule(context.Background(), ruleId, namespaceName)
	s.NoError(err)
	s.NotNil(rule)

	var invalidArgument *serviceerror.InvalidArgument

	// rule with such id doesn't exist
	rule, err = s.handler.DescribeWorkflowRule(context.Background(), "not existing rule id", namespaceName)
	s.Error(err)
	s.ErrorAs(err, &invalidArgument)
	s.Nil(rule)

	// config is nil
	nsConfig.SetWorkflowRules(nil)
	rule, err = s.handler.DescribeWorkflowRule(context.Background(), "not existing rule id", namespaceName)
	s.Error(err)
	s.ErrorAs(err, &invalidArgument)
	s.Nil(rule)
}

func (s *namespaceHandlerCommonSuite) TestListWorkflowRules() {
	namespaceName := "test-namespace"
	nsConfig := persistencespb.NamespaceConfig_builder{
		WorkflowRules: map[string]*rulespb.WorkflowRule{
			"rule 1": rulespb.WorkflowRule_builder{Spec: rulespb.WorkflowRuleSpec_builder{Id: "rule 1"}.Build()}.Build(),
			"rule 2": rulespb.WorkflowRule_builder{Spec: rulespb.WorkflowRuleSpec_builder{Id: "rule 2"}.Build()}.Build(),
		},
	}.Build()

	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:   "1",
				Name: namespaceName,
			}.Build(),
			Config:            nsConfig,
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		}.Build(),
	}, nil).AnyTimes()

	// happy path
	rules, err := s.handler.ListWorkflowRules(context.Background(), namespaceName)
	s.NoError(err)
	s.NotNil(rules)
	s.Equal(2, len(rules))

	// config is nil
	nsConfig.SetWorkflowRules(nil)
	rules, err = s.handler.ListWorkflowRules(context.Background(), namespaceName)
	s.NoError(err)
	s.NotNil(rules)
	s.Equal(0, len(rules))
}

func (s *namespaceHandlerCommonSuite) TestWorkflowRuleEviction() {
	s.fakeClock.Update(time.Now())
	expiredTime1 := s.fakeClock.Now().Add(-1 * time.Hour)
	expiredTime2 := s.fakeClock.Now().Add(-2 * time.Hour)

	tests := []struct {
		name        string
		deletedRule string
		rules       map[string]*rulespb.WorkflowRule
	}{
		{
			name: "empty map", deletedRule: "", rules: map[string]*rulespb.WorkflowRule{},
		},
		{
			name: "no rule to delete", deletedRule: "", rules: map[string]*rulespb.WorkflowRule{
				"rule 1": rulespb.WorkflowRule_builder{Spec: rulespb.WorkflowRuleSpec_builder{Id: "rule 1"}.Build()}.Build(),
			},
		},
		{
			name: "single rule to delete", deletedRule: "rule 1", rules: map[string]*rulespb.WorkflowRule{
				"rule 1": rulespb.WorkflowRule_builder{Spec: rulespb.WorkflowRuleSpec_builder{Id: "rule 1", ExpirationTime: timestamppb.New(expiredTime1)}.Build()}.Build(),
			},
		},
		{
			name: "two candidates to delete", deletedRule: "rule 2", rules: map[string]*rulespb.WorkflowRule{
				"rule 1": rulespb.WorkflowRule_builder{Spec: rulespb.WorkflowRuleSpec_builder{Id: "rule 1", ExpirationTime: timestamppb.New(expiredTime1)}.Build()}.Build(),
				"rule 2": rulespb.WorkflowRule_builder{Spec: rulespb.WorkflowRuleSpec_builder{Id: "rule 2", ExpirationTime: timestamppb.New(expiredTime2)}.Build()}.Build(),
			},
		},
	}

	for _, tt := range tests {
		oldLens := len(tt.rules)
		s.handler.removeOldestExpiredWorkflowRule("", tt.rules)
		if len(tt.deletedRule) == 0 {
			s.Equal(oldLens, len(tt.rules))
		} else {
			if _, exists := tt.rules[tt.deletedRule]; exists {
				s.True(false, "Rule was not deleted")
			}
		}
	}
}

func (s *namespaceHandlerCommonSuite) getRandomNamespace() string {
	return "namespace" + uuid.NewString()
}
