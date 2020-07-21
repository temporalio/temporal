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
	"log"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/service/config"
	dc "go.temporal.io/server/common/service/dynamicconfig"
)

type (
	namespaceHandlerGlobalNamespaceDisabledSuite struct {
		suite.Suite
		persistencetests.TestBase

		minRetentionDays        int
		maxBadBinaryCount       int
		metadataMgr             persistence.MetadataManager
		mockProducer            *mocks.KafkaProducer
		mockNamespaceReplicator Replicator
		archivalMetadata        archiver.ArchivalMetadata
		mockArchiverProvider    *provider.MockArchiverProvider

		handler *HandlerImpl
	}
)

func TestNamespaceHandlerGlobalNamespaceDisabledSuite(t *testing.T) {
	s := new(namespaceHandlerGlobalNamespaceDisabledSuite)
	suite.Run(t, s)
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{
		ClusterMetadata: cluster.GetTestClusterMetadata(false, false),
	})
	s.TestBase.Setup()
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) SetupTest() {
	logger := loggerimpl.NewNopLogger()
	dcCollection := dc.NewCollection(dc.NewNopClient(), logger)
	s.minRetentionDays = 1
	s.maxBadBinaryCount = 10
	s.metadataMgr = s.TestBase.MetadataManager
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockNamespaceReplicator = NewNamespaceReplicator(s.mockProducer, logger)
	s.archivalMetadata = archiver.NewArchivalMetadata(
		dcCollection,
		"",
		false,
		"",
		false,
		&config.ArchivalNamespaceDefaults{},
	)
	s.mockArchiverProvider = &provider.MockArchiverProvider{}
	s.handler = NewHandler(
		s.minRetentionDays,
		dc.GetIntPropertyFilteredByNamespace(s.maxBadBinaryCount),
		logger,
		s.metadataMgr,
		s.ClusterMetadata,
		s.mockNamespaceReplicator,
		s.archivalMetadata,
		s.mockArchiverProvider,
	)
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TestRegisterGetNamespace_InvalidGlobalNamespace() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := true

	resp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		Description:                          description,
		OwnerEmail:                           email,
		WorkflowExecutionRetentionPeriodDays: retention,
		Clusters:                             clusters,
		ActiveClusterName:                    activeClusterName,
		Data:                                 data,
		IsGlobalNamespace:                    isGlobalNamespace,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(resp)
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TestRegisterGetNamespace_InvalidCluster() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	activeClusterName := cluster.TestAlternativeClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := false

	resp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		Description:                          description,
		OwnerEmail:                           email,
		WorkflowExecutionRetentionPeriodDays: retention,
		Clusters:                             clusters,
		ActiveClusterName:                    activeClusterName,
		Data:                                 data,
		IsGlobalNamespace:                    isGlobalNamespace,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Nil(resp)
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TestRegisterGetNamespace_AllDefault() {
	namespace := s.getRandomNamespace()
	var clusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster,
		})
	}

	retention := int32(1)
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		WorkflowExecutionRetentionPeriodDays: retention,
	})
	s.NoError(err)
	s.Nil(registerResp)

	resp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
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
		WorkflowExecutionRetentionPeriodInDays: retention,
		HistoryArchivalState:                   enumspb.ARCHIVAL_STATE_DISABLED,
		HistoryArchivalUri:                     "",
		VisibilityArchivalState:                enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalUri:                  "",
		BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}, resp.Config)
	s.Equal(&replicationpb.NamespaceReplicationConfig{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters:          clusters,
	}, resp.ReplicationConfig)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.False(resp.GetIsGlobalNamespace())
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TestRegisterGetNamespace_NoDefault() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		&replicationpb.ClusterReplicationConfig{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}
	isGlobalNamespace := false

	var expectedClusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		expectedClusters = append(expectedClusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster,
		})
	}

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		Description:                          description,
		OwnerEmail:                           email,
		WorkflowExecutionRetentionPeriodDays: retention,
		Clusters:                             clusters,
		ActiveClusterName:                    activeClusterName,
		Data:                                 data,
		IsGlobalNamespace:                    isGlobalNamespace,
	})
	s.NoError(err)
	s.Nil(registerResp)

	resp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
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
		WorkflowExecutionRetentionPeriodInDays: retention,
		HistoryArchivalState:                   enumspb.ARCHIVAL_STATE_DISABLED,
		HistoryArchivalUri:                     "",
		VisibilityArchivalState:                enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalUri:                  "",
		BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
	}, resp.Config)
	s.Equal(&replicationpb.NamespaceReplicationConfig{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters:          expectedClusters,
	}, resp.ReplicationConfig)
	s.Equal(common.EmptyVersion, resp.GetFailoverVersion())
	s.Equal(isGlobalNamespace, resp.GetIsGlobalNamespace())
}

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TestUpdateGetNamespace_NoAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := int32(7)
	data := map[string]string{"some random key": "some random value"}
	var clusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster,
		})
	}

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		Description:                          description,
		OwnerEmail:                           email,
		WorkflowExecutionRetentionPeriodDays: retention,
		Clusters:                             clusters,
		ActiveClusterName:                    s.ClusterMetadata.GetCurrentClusterName(),
		Data:                                 data,
	})
	s.NoError(err)
	s.Nil(registerResp)

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
			WorkflowExecutionRetentionPeriodInDays: retention,
			HistoryArchivalState:                   enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:                     "",
			VisibilityArchivalState:                enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:                  "",
			BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		}, config)
		s.Equal(&replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          clusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.False(isGlobalNamespace)
	}

	updateResp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
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
		Name: namespace,
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

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) TestUpdateGetNamespace_AllAttrSet() {
	namespace := s.getRandomNamespace()
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		WorkflowExecutionRetentionPeriodDays: 1,
	})
	s.NoError(err)
	s.Nil(registerResp)

	description := "some random description"
	email := "some random email"
	retention := int32(7)
	activeClusterName := cluster.TestCurrentClusterName
	clusters := []*replicationpb.ClusterReplicationConfig{
		&replicationpb.ClusterReplicationConfig{
			ClusterName: activeClusterName,
		},
	}
	data := map[string]string{"some random key": "some random value"}

	var expectedClusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		expectedClusters = append(expectedClusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster,
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
			WorkflowExecutionRetentionPeriodInDays: retention,
			HistoryArchivalState:                   enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:                     "",
			VisibilityArchivalState:                enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:                  "",
			BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		}, config)
		s.Equal(&replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
			Clusters:          expectedClusters,
		}, replicationConfig)
		s.Equal(common.EmptyVersion, failoverVersion)
		s.False(isGlobalNamespace)
	}

	updateResp, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retention,
			HistoryArchivalState:                   enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:                     "",
			VisibilityArchivalState:                enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:                  "",
			BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters:          clusters,
		},
	})
	s.NoError(err)
	fnTest(updateResp.NamespaceInfo, updateResp.Config, updateResp.ReplicationConfig, updateResp.GetIsGlobalNamespace(), updateResp.GetFailoverVersion())

	getResp, err := s.handler.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
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

func (s *namespaceHandlerGlobalNamespaceDisabledSuite) getRandomNamespace() string {
	return "namespace" + uuid.New()
}
