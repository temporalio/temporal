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

	"go.temporal.io/server/common/clock"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/config"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite struct {
		suite.Suite
		persistencetests.TestBase

		controller *gomock.Controller

		maxBadBinaryCount       int
		metadataMgr             persistence.MetadataManager
		mockProducer            *persistence.MockNamespaceReplicationQueue
		mockNamespaceReplicator Replicator
		archivalMetadata        archiver.ArchivalMetadata
		mockArchiverProvider    *provider.MockArchiverProvider

		handler *HandlerImpl
	}
)

func TestNamespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite(t *testing.T) {
	s := new(namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite)
	suite.Run(t, s)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) SetupSuite() {
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(cluster.NewTestClusterMetadataConfig(true, false))
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) SetupTest() {
	logger := log.NewNoopLogger()
	dcCollection := dc.NewCollection(dc.NewNoopClient(), logger)
	s.maxBadBinaryCount = 10
	s.metadataMgr = s.TestBase.MetadataManager
	s.controller = gomock.NewController(s.T())
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockNamespaceReplicator = NewNamespaceReplicator(s.mockProducer, logger)
	s.archivalMetadata = archiver.NewArchivalMetadata(
		dcCollection,
		"",
		false,
		"",
		false,
		&config.ArchivalNamespaceDefaults{},
	)
	s.mockArchiverProvider = provider.NewMockArchiverProvider(s.controller)
	s.handler = NewHandler(
		dc.GetIntPropertyFilteredByNamespace(s.maxBadBinaryCount),
		logger,
		s.metadataMgr,
		s.ClusterMetadata,
		s.mockNamespaceReplicator,
		s.archivalMetadata,
		s.mockArchiverProvider,
		dc.GetBoolPropertyFnFilteredByNamespace(false),
		clock.NewRealTimeSource(),
	)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestRegisterGetNamespace_LocalNamespace_AllDefault() {
	namespace := s.getRandomNamespace()
	isGlobalNamespace := false
	var clusters []*replicationpb.ClusterReplicationConfig
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: name,
		})
	}

	retention := 1 * time.Hour * 24
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                isGlobalNamespace,
		WorkflowExecutionRetentionPeriod: &retention,
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	resp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)

	s.NotEmpty(resp.NamespaceInfo.GetId())
	resp.NamespaceInfo.Id = ""
	s.Equal(&namespacepb.NamespaceInfo{
		Name:        namespace,
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Description: "",
		OwnerEmail:  "",
		Data:        map[string]string{},
		Id:          "",
	}, resp.NamespaceInfo)
	s.Equal(&namespacepb.NamespaceConfig{
		WorkflowExecutionRetentionTtl: &retention,
		HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
		HistoryArchivalUri:            "",
		VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalUri:         "",
		BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}, resp.Config)
	s.Equal(&replicationpb.NamespaceReplicationConfig{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters:          clusters,
	}, resp.ReplicationConfig)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.Equal(isGlobalNamespace, resp.GetIsGlobalNamespace())
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestRegisterGetNamespace_LocalNamespace_NoDefault() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		&replicationpb.ClusterReplicationConfig{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := false

	var expectedClusters []*replicationpb.ClusterReplicationConfig
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		expectedClusters = append(expectedClusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: name,
		})
	}

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: &retention,
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	resp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)

	s.NotEmpty(resp.NamespaceInfo.GetId())
	resp.NamespaceInfo.Id = ""
	s.Equal(&namespacepb.NamespaceInfo{
		Name:        namespace,
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Description: description,
		OwnerEmail:  email,
		Data:        data,
		Id:          "",
	}, resp.NamespaceInfo)
	s.Equal(&namespacepb.NamespaceConfig{
		WorkflowExecutionRetentionTtl: &retention,
		HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
		HistoryArchivalUri:            "",
		VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalUri:         "",
		BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}, resp.Config)
	s.Equal(&replicationpb.NamespaceReplicationConfig{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters:          expectedClusters,
	}, resp.ReplicationConfig)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.Equal(isGlobalNamespace, resp.GetIsGlobalNamespace())
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestUpdateGetNamespace_LocalNamespace_NoAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	data := map[string]string{"some random key": "some random value"}
	var clusters []*replicationpb.ClusterReplicationConfig
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: name,
		})
	}

	isGlobalNamespace := false

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: &retention,
		Clusters:                         clusters,
		ActiveClusterName:                s.ClusterMetadata.GetCurrentClusterName(),
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	fnTest := func(info *namespacepb.NamespaceInfo, config *namespacepb.NamespaceConfig,
		replicationConfig *replicationpb.NamespaceReplicationConfig, isGlobalNamespace bool, failoverVersion int64) {
		s.NotEmpty(info.GetId())
		info.Id = ""
		s.Equal(&namespacepb.NamespaceInfo{
			Name:        namespace,
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
			Id:          "",
		}, info)
		s.Equal(&namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		}, config)
		s.Equal(&replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.Equal(isGlobalNamespace, isGlobalNamespace)
	}

	updateResp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	fnTest(
		updateResp.NamespaceInfo,
		updateResp.Config,
		updateResp.ReplicationConfig,
		updateResp.GetIsGlobalNamespace(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	fnTest(
		getResp.NamespaceInfo,
		getResp.Config,
		getResp.ReplicationConfig,
		getResp.GetIsGlobalNamespace(),
		getResp.GetFailoverVersion(),
	)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestUpdateGetNamespace_LocalNamespace_AllAttrSet() {
	namespace := s.getRandomNamespace()
	isGlobalNamespace := false
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                isGlobalNamespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(1 * time.Hour * 24),
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	data := map[string]string{"some random key": "some random value"}
	var clusters []*replicationpb.ClusterReplicationConfig
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: name,
		})
	}

	fnTest := func(info *namespacepb.NamespaceInfo, config *namespacepb.NamespaceConfig,
		replicationConfig *replicationpb.NamespaceReplicationConfig, isGlobalNamespace bool, failoverVersion int64) {
		s.NotEmpty(info.GetId())
		info.Id = ""
		s.Equal(&namespacepb.NamespaceInfo{
			Name:        namespace,
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
			Id:          "",
		}, info)
		s.Equal(&namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		}, config)
		s.Equal(&replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.Equal(isGlobalNamespace, isGlobalNamespace)
	}

	updateResp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          clusters,
		},
	})
	s.NoError(err)
	fnTest(
		updateResp.NamespaceInfo,
		updateResp.Config,
		updateResp.ReplicationConfig,
		updateResp.GetIsGlobalNamespace(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	fnTest(
		getResp.NamespaceInfo,
		getResp.Config,
		getResp.ReplicationConfig,
		getResp.GetIsGlobalNamespace(),
		getResp.GetFailoverVersion(),
	)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestRegisterGetNamespace_GlobalNamespace_AllDefault() {
	namespace := s.getRandomNamespace()
	isGlobalNamespace := true
	var clusters []*replicationpb.ClusterReplicationConfig
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: name,
		})
	}
	s.Equal(1, len(clusters))

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:         namespace,
		IsGlobalNamespace: isGlobalNamespace,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(registerResp)

	resp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.Error(err)
	s.IsType(&serviceerror.NamespaceNotFound{}, err)
	s.Nil(resp)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestRegisterGetNamespace_GlobalNamespace_NoDefault() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	activeClusterName := ""
	clusters := []*replicationpb.ClusterReplicationConfig{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
	}
	s.True(len(activeClusterName) > 0)
	s.True(len(clusters) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := true

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: &retention,
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(registerResp)

	resp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.Error(err)
	s.IsType(&serviceerror.NamespaceNotFound{}, err)
	s.Nil(resp)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestUpdateGetNamespace_GlobalNamespace_NoAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	activeClusterName := ""
	clusters := []string{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
		clusters = append(clusters, clusterName)
	}
	s.True(len(activeClusterName) > 0)
	s.True(len(clusters) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := true

	_, err := s.MetadataManager.CreateNamespace(context.Background(), &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          uuid.New(),
				Name:        namespace,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               &retention,
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
				HistoryArchivalUri:      "",
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalUri:   "",
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: activeClusterName,
				Clusters:          clusters,
			},
			ConfigVersion:   0,
			FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(activeClusterName, 0),
		},
		IsGlobalNamespace: isGlobalNamespace,
	})
	s.NoError(err)

	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	resp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestUpdateGetNamespace_GlobalNamespace_AllAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	activeClusterName := ""
	clusters := []*replicationpb.ClusterReplicationConfig{}
	clustersDB := []string{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName = clusterName
		}
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
		clustersDB = append(clustersDB, clusterName)
	}
	s.True(len(activeClusterName) > 0)
	s.True(len(clusters) > 1)
	s.True(len(clustersDB) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := true

	_, err := s.MetadataManager.CreateNamespace(context.Background(), &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          uuid.New(),
				Name:        namespace,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "",
				Owner:       "",
				Data:        map[string]string{},
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               timestamp.DurationFromDays(0),
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
				HistoryArchivalUri:      "",
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalUri:   "",
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: activeClusterName,
				Clusters:          clustersDB,
			},
			ConfigVersion:   0,
			FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(activeClusterName, 0),
		},
		IsGlobalNamespace: isGlobalNamespace,
	})
	s.NoError(err)

	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	updateResp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: "",
			Clusters:          clusters,
		},
	})
	s.NoError(err)
	s.NotNil(updateResp)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) TestUpdateGetNamespace_GlobalNamespace_Failover() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := 7 * time.Hour * 24
	prevActiveClusterName := ""
	nextActiveClusterName := s.ClusterMetadata.GetCurrentClusterName()
	clusters := []*replicationpb.ClusterReplicationConfig{}
	clustersDB := []string{}
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			prevActiveClusterName = clusterName
		}
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
		clustersDB = append(clustersDB, clusterName)
	}
	s.True(len(prevActiveClusterName) > 0)
	s.True(len(clusters) > 1)
	s.True(len(clustersDB) > 1)
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := true

	_, err := s.MetadataManager.CreateNamespace(context.Background(), &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          uuid.New(),
				Name:        namespace,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               &retention,
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
				HistoryArchivalUri:      "",
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalUri:   "",
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: prevActiveClusterName,
				Clusters:          clustersDB,
			},
			ConfigVersion:   0,
			FailoverVersion: s.ClusterMetadata.GetNextFailoverVersion(prevActiveClusterName, 0),
		},
		IsGlobalNamespace: isGlobalNamespace,
	})
	s.NoError(err)

	fnTest := func(info *namespacepb.NamespaceInfo, config *namespacepb.NamespaceConfig,
		replicationConfig *replicationpb.NamespaceReplicationConfig, isGlobalNamespace bool, failoverVersion int64) {
		s.NotEmpty(info.GetId())
		info.Id = ""
		s.Equal(&namespacepb.NamespaceInfo{
			Name:        namespace,
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: description,
			OwnerEmail:  email,
			Data:        data,
			Id:          "",
		}, info)
		s.Equal(&namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: &retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		}, config)
		s.Equal(&replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: nextActiveClusterName,
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(s.ClusterMetadata.GetNextFailoverVersion(
			nextActiveClusterName,
			s.ClusterMetadata.GetNextFailoverVersion(prevActiveClusterName, 0),
		), failoverVersion)
		s.Equal(isGlobalNamespace, isGlobalNamespace)
	}

	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	updateResp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		},
	})
	s.NoError(err)
	fnTest(
		updateResp.NamespaceInfo,
		updateResp.Config,
		updateResp.ReplicationConfig,
		updateResp.GetIsGlobalNamespace(),
		updateResp.GetFailoverVersion(),
	)

	getResp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	fnTest(
		getResp.NamespaceInfo,
		getResp.Config,
		getResp.ReplicationConfig,
		getResp.GetIsGlobalNamespace(),
		getResp.GetFailoverVersion(),
	)
}

func (s *namespaceHandlerGlobalNamespaceEnabledNotMasterClusterSuite) getRandomNamespace() string {
	return "namespace" + uuid.New()
}
