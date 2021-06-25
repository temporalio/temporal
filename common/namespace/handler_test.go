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

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/config"

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
	namespaceHandlerCommonSuite struct {
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

var now = time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC)

func TestNamespaceHandlerCommonSuite(t *testing.T) {
	s := new(namespaceHandlerCommonSuite)
	suite.Run(t, s)
}

func (s *namespaceHandlerCommonSuite) SetupSuite() {
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(cluster.NewTestClusterMetadataConfig(true, true))
}

func (s *namespaceHandlerCommonSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *namespaceHandlerCommonSuite) SetupTest() {
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
			"k0": {Reason: "reason0"},
		},
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason2"},
		}, now,
	)

	assert.True(s.T(), proto.Equal(&out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason2", CreateTime: &now},
		},
	}))
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Adding() {
	out := s.handler.mergeBadBinaries(
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason0"},
		},
		map[string]*namespacepb.BadBinaryInfo{
			"k1": {Reason: "reason2"},
		}, now,
	)

	expected := namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason0"},
			"k1": {Reason: "reason2", CreateTime: &now},
		},
	}
	assert.Equal(s.T(), out.String(), expected.String())
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Merging() {
	out := s.handler.mergeBadBinaries(
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason0"},
		},
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1"},
			"k1": {Reason: "reason2"},
		}, now,
	)

	assert.True(s.T(), proto.Equal(&out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1", CreateTime: &now},
			"k1": {Reason: "reason2", CreateTime: &now},
		},
	}))
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Nil() {
	out := s.handler.mergeBadBinaries(
		nil,
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1"},
			"k1": {Reason: "reason2"},
		}, now,
	)

	assert.True(s.T(), proto.Equal(&out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1", CreateTime: &now},
			"k1": {Reason: "reason2", CreateTime: &now},
		},
	}))
}

func (s *namespaceHandlerCommonSuite) TestListNamespace() {
	namespace1 := s.getRandomNamespace()
	description1 := "some random description 1"
	email1 := "some random email 1"
	retention1 := 1 * time.Hour * 24
	data1 := map[string]string{"some random key 1": "some random value 1"}
	isGlobalNamespace1 := false
	activeClusterName1 := s.ClusterMetadata.GetCurrentClusterName()
	var cluster1 []*replicationpb.ClusterReplicationConfig
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		cluster1 = append(cluster1, &replicationpb.ClusterReplicationConfig{
			ClusterName: name,
		})
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace1,
		Description:                      description1,
		OwnerEmail:                       email1,
		WorkflowExecutionRetentionPeriod: &retention1,
		Data:                             data1,
		IsGlobalNamespace:                isGlobalNamespace1,
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	namespace2 := s.getRandomNamespace()
	description2 := "some random description 2"
	email2 := "some random email 2"
	retention2 := 2 * time.Hour * 24
	data2 := map[string]string{"some random key 2": "some random value 2"}
	isGlobalNamespace2 := true
	activeClusterName2 := ""
	var cluster2 []*replicationpb.ClusterReplicationConfig
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName2 = clusterName
		}
		cluster2 = append(cluster2, &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterName,
		})
	}
	s.mockProducer.EXPECT().Publish(gomock.Any()).Return(nil)
	registerResp, err = s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace2,
		Description:                      description2,
		OwnerEmail:                       email2,
		WorkflowExecutionRetentionPeriod: &retention2,
		Clusters:                         cluster2,
		ActiveClusterName:                activeClusterName2,
		Data:                             data2,
		IsGlobalNamespace:                isGlobalNamespace2,
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	namespaces := map[string]*workflowservice.DescribeNamespaceResponse{}
	pagesize := int32(1)
	var token []byte
	for doPaging := true; doPaging; doPaging = len(token) > 0 {
		resp, err := s.handler.ListNamespaces(context.Background(), &workflowservice.ListNamespacesRequest{
			PageSize:      pagesize,
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		s.True(len(resp.Namespaces) <= int(pagesize))
		if len(resp.Namespaces) > 0 {
			s.NotEmpty(resp.Namespaces[0].NamespaceInfo.GetId())
			resp.Namespaces[0].NamespaceInfo.Id = ""
			namespaces[resp.Namespaces[0].NamespaceInfo.GetName()] = resp.Namespaces[0]
		}
	}
	delete(namespaces, common.SystemLocalNamespace)
	s.Equal(map[string]*workflowservice.DescribeNamespaceResponse{
		namespace1: &workflowservice.DescribeNamespaceResponse{
			NamespaceInfo: &namespacepb.NamespaceInfo{
				Name:        namespace1,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: description1,
				OwnerEmail:  email1,
				Data:        data1,
				Id:          "",
			},
			Config: &namespacepb.NamespaceConfig{
				WorkflowExecutionRetentionTtl: &retention1,
				HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
				HistoryArchivalUri:            "",
				VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalUri:         "",
				BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
				ActiveClusterName: activeClusterName1,
				Clusters:          cluster1,
			},
			FailoverVersion:   common.EmptyVersion,
			IsGlobalNamespace: isGlobalNamespace1,
		},
		namespace2: &workflowservice.DescribeNamespaceResponse{
			NamespaceInfo: &namespacepb.NamespaceInfo{
				Name:        namespace2,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: description2,
				OwnerEmail:  email2,
				Data:        data2,
				Id:          "",
			},
			Config: &namespacepb.NamespaceConfig{
				WorkflowExecutionRetentionTtl: &retention2,
				HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
				HistoryArchivalUri:            "",
				VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalUri:         "",
				BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
				ActiveClusterName: activeClusterName2,
				Clusters:          cluster2,
			},
			FailoverVersion:   s.ClusterMetadata.GetNextFailoverVersion(activeClusterName2, 0),
			IsGlobalNamespace: isGlobalNamespace2,
		},
	}, namespaces)
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_InvalidRetentionPeriod() {
	// local
	for _, invalidDuration := range []time.Duration{0, -1 * time.Hour, 1 * time.Millisecond, 10 * 365 * 24 * time.Hour} {
		registerRequest := &workflowservice.RegisterNamespaceRequest{
			Namespace:                        "random namespace name",
			Description:                      "random namespace name",
			WorkflowExecutionRetentionPeriod: &invalidDuration,
			IsGlobalNamespace:                false,
		}
		resp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}

	// global
	for _, invalidDuration := range []time.Duration{0, -1 * time.Hour, 1 * time.Millisecond, 10 * time.Hour} {
		registerRequest := &workflowservice.RegisterNamespaceRequest{
			Namespace:                        "random namespace name",
			Description:                      "random namespace name",
			WorkflowExecutionRetentionPeriod: &invalidDuration,
			IsGlobalNamespace:                true,
		}
		resp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_InvalidRetentionPeriod() {
	namespace := "random namespace name"
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(10 * 24 * time.Hour),
		IsGlobalNamespace:                false,
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	for _, invalidDuration := range []time.Duration{0, -1 * time.Hour, 1 * time.Millisecond, 10 * 365 * 24 * time.Hour} {
		updateRequest := &workflowservice.UpdateNamespaceRequest{
			Namespace: namespace,
			Config: &namespacepb.NamespaceConfig{
				WorkflowExecutionRetentionTtl: timestamp.DurationPtr(invalidDuration),
			},
		}
		resp, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}
}

func (s *namespaceHandlerCommonSuite) getRandomNamespace() string {
	return "namespace" + uuid.New()
}
