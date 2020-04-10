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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	replicationpb "go.temporal.io/temporal-proto/replication"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	persistencetests "github.com/temporalio/temporal/common/persistence/persistence-tests"
	"github.com/temporalio/temporal/common/service/config"
	dc "github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	namespaceHandlerCommonSuite struct {
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

var nowInt64 = time.Now().UnixNano()

func TestNamespaceHandlerCommonSuite(t *testing.T) {
	s := new(namespaceHandlerCommonSuite)
	suite.Run(t, s)
}

func (s *namespaceHandlerCommonSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{
		ClusterMetadata: cluster.GetTestClusterMetadata(true, true),
	})
	s.TestBase.Setup()
}

func (s *namespaceHandlerCommonSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *namespaceHandlerCommonSuite) SetupTest() {
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

func (s *namespaceHandlerCommonSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
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
		}, nowInt64,
	)

	assert.True(s.T(), proto.Equal(&out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason2", CreatedTimeNano: nowInt64},
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
		}, nowInt64,
	)

	expected := namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason0"},
			"k1": {Reason: "reason2", CreatedTimeNano: nowInt64},
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
		}, nowInt64,
	)

	assert.True(s.T(), proto.Equal(&out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1", CreatedTimeNano: nowInt64},
			"k1": {Reason: "reason2", CreatedTimeNano: nowInt64},
		},
	}))
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Nil() {
	out := s.handler.mergeBadBinaries(
		nil,
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1"},
			"k1": {Reason: "reason2"},
		}, nowInt64,
	)

	assert.True(s.T(), proto.Equal(&out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1", CreatedTimeNano: nowInt64},
			"k1": {Reason: "reason2", CreatedTimeNano: nowInt64},
		},
	}))
}

func (s *namespaceHandlerCommonSuite) TestListNamespace() {
	namespace1 := s.getRandomNamespace()
	description1 := "some random description 1"
	email1 := "some random email 1"
	retention1 := int32(1)
	emitMetric1 := true
	data1 := map[string]string{"some random key 1": "some random value 1"}
	isGlobalNamespace1 := false
	activeClusterName1 := s.ClusterMetadata.GetCurrentClusterName()
	var cluster1 []*replicationpb.ClusterReplicationConfiguration
	for _, name := range persistence.GetOrUseDefaultClusters(s.ClusterMetadata.GetCurrentClusterName(), nil) {
		cluster1 = append(cluster1, &replicationpb.ClusterReplicationConfiguration{
			ClusterName: name,
		})
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                   namespace1,
		Description:                            description1,
		OwnerEmail:                             email1,
		WorkflowExecutionRetentionPeriodInDays: retention1,
		EmitMetric:                             emitMetric1,
		Data:                                   data1,
		IsGlobalNamespace:                      isGlobalNamespace1,
	})
	s.NoError(err)
	s.Nil(registerResp)

	namespace2 := s.getRandomNamespace()
	description2 := "some random description 2"
	email2 := "some random email 2"
	retention2 := int32(2)
	emitMetric2 := false
	data2 := map[string]string{"some random key 2": "some random value 2"}
	isGlobalNamespace2 := true
	activeClusterName2 := ""
	var cluster2 []*replicationpb.ClusterReplicationConfiguration
	for clusterName := range s.ClusterMetadata.GetAllClusterInfo() {
		if clusterName != s.ClusterMetadata.GetCurrentClusterName() {
			activeClusterName2 = clusterName
		}
		cluster2 = append(cluster2, &replicationpb.ClusterReplicationConfiguration{
			ClusterName: clusterName,
		})
	}
	s.mockProducer.On("Publish", mock.Anything).Return(nil).Once()
	registerResp, err = s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Name:                                   namespace2,
		Description:                            description2,
		OwnerEmail:                             email2,
		WorkflowExecutionRetentionPeriodInDays: retention2,
		EmitMetric:                             emitMetric2,
		Clusters:                               cluster2,
		ActiveClusterName:                      activeClusterName2,
		Data:                                   data2,
		IsGlobalNamespace:                      isGlobalNamespace2,
	})
	s.NoError(err)
	s.Nil(registerResp)

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
				Status:      namespacepb.NamespaceStatus_Registered,
				Description: description1,
				OwnerEmail:  email1,
				Data:        data1,
				Id:          "",
			},
			Configuration: &namespacepb.NamespaceConfiguration{
				WorkflowExecutionRetentionPeriodInDays: retention1,
				EmitMetric:                             &types.BoolValue{Value: emitMetric1},
				HistoryArchivalStatus:                  namespacepb.ArchivalStatus_Disabled,
				HistoryArchivalURI:                     "",
				VisibilityArchivalStatus:               namespacepb.ArchivalStatus_Disabled,
				VisibilityArchivalURI:                  "",
				BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfiguration: &replicationpb.NamespaceReplicationConfiguration{
				ActiveClusterName: activeClusterName1,
				Clusters:          cluster1,
			},
			FailoverVersion:   common.EmptyVersion,
			IsGlobalNamespace: isGlobalNamespace1,
		},
		namespace2: &workflowservice.DescribeNamespaceResponse{
			NamespaceInfo: &namespacepb.NamespaceInfo{
				Name:        namespace2,
				Status:      namespacepb.NamespaceStatus_Registered,
				Description: description2,
				OwnerEmail:  email2,
				Data:        data2,
				Id:          "",
			},
			Configuration: &namespacepb.NamespaceConfiguration{
				WorkflowExecutionRetentionPeriodInDays: retention2,
				EmitMetric:                             &types.BoolValue{Value: emitMetric2},
				HistoryArchivalStatus:                  namespacepb.ArchivalStatus_Disabled,
				HistoryArchivalURI:                     "",
				VisibilityArchivalStatus:               namespacepb.ArchivalStatus_Disabled,
				VisibilityArchivalURI:                  "",
				BadBinaries:                            &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfiguration: &replicationpb.NamespaceReplicationConfiguration{
				ActiveClusterName: activeClusterName2,
				Clusters:          cluster2,
			},
			FailoverVersion:   s.ClusterMetadata.GetNextFailoverVersion(activeClusterName2, 0),
			IsGlobalNamespace: isGlobalNamespace2,
		},
	}, namespaces)
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_InvalidRetentionPeriod() {
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Name:                                   "random namespace name",
		Description:                            "random namespace name",
		WorkflowExecutionRetentionPeriodInDays: int32(0),
		IsGlobalNamespace:                      false,
	}
	resp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.Equal(errInvalidRetentionPeriod, err)
	s.Nil(resp)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_InvalidRetentionPeriod() {
	namespace := "random namespace name"
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Name:                                   namespace,
		Description:                            namespace,
		WorkflowExecutionRetentionPeriodInDays: int32(10),
		IsGlobalNamespace:                      false,
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.NoError(err)
	s.Nil(registerResp)

	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		Configuration: &namespacepb.NamespaceConfiguration{
			WorkflowExecutionRetentionPeriodInDays: int32(-1),
		},
	}
	resp, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.Equal(errInvalidRetentionPeriod, err)
	s.Nil(resp)
}

func (s *namespaceHandlerCommonSuite) getRandomNamespace() string {
	return "namespace" + uuid.New()
}
