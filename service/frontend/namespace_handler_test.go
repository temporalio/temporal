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

package frontend

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"golang.org/x/exp/slices"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
)

type (
	namespaceHandlerCommonSuite struct {
		suite.Suite

		controller *gomock.Controller

		maxBadBinaryCount       int
		mockMetadataMgr         *persistence.MockMetadataManager
		mockClusterMetadata     *cluster.MockMetadata
		mockProducer            *persistence.MockNamespaceReplicationQueue
		mockNamespaceReplicator namespace.Replicator
		archivalMetadata        archiver.ArchivalMetadata
		mockArchiverProvider    *provider.MockArchiverProvider
		fakeClock               *clock.EventTimeSource

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
	s.mockNamespaceReplicator = namespace.NewNamespaceReplicator(s.mockProducer, logger)
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
	s.handler = newNamespaceHandler(
		dc.GetIntPropertyFnFilteredByNamespace(s.maxBadBinaryCount),
		logger,
		s.mockMetadataMgr,
		s.mockClusterMetadata,
		s.mockNamespaceReplicator,
		s.archivalMetadata,
		s.mockArchiverProvider,
		func(s string) bool { return strings.HasSuffix(s, "sched") },
		s.fakeClock,
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

	protoassert.ProtoEqual(s.T(), &out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason2", CreateTime: timestamppb.New(now)},
		},
	})
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
			"k1": {Reason: "reason2", CreateTime: timestamppb.New(now)},
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

	protoassert.ProtoEqual(s.T(), &out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1", CreateTime: timestamppb.New(now)},
			"k1": {Reason: "reason2", CreateTime: timestamppb.New(now)},
		},
	})
}

func (s *namespaceHandlerCommonSuite) TestMergeBadBinaries_Nil() {
	out := s.handler.mergeBadBinaries(
		nil,
		map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1"},
			"k1": {Reason: "reason2"},
		}, now,
	)

	protoassert.ProtoEqual(s.T(), &out, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"k0": {Reason: "reason1", CreateTime: timestamppb.New(now)},
			"k1": {Reason: "reason2", CreateTime: timestamppb.New(now)},
		},
	})
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
	namespace1 := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:          uuid.New(),
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Name:        s.getRandomNamespace(),
			Description: description1,
			Owner:       email1,
			Data:        data1,
		},
		Config: &persistencespb.NamespaceConfig{
			Retention: durationpb.New(retention1),
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster1,
			Clusters:          []string{cluster1},
		},
		ConfigVersion:               0,
		FailoverNotificationVersion: 0,
		FailoverVersion:             0,
		FailoverEndTime:             nil,
	}
	namespace2 := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:          uuid.New(),
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Name:        s.getRandomNamespace(),
			Description: description2,
			Owner:       email2,
			Data:        data2,
		},
		Config: &persistencespb.NamespaceConfig{
			Retention: durationpb.New(retention2),
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster2,
			Clusters:          []string{cluster1, cluster2},
		},
		ConfigVersion:               0,
		FailoverNotificationVersion: 0,
		FailoverVersion:             0,
		FailoverEndTime:             nil,
	}
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
		resp, err := s.handler.ListNamespaces(context.Background(), &workflowservice.ListNamespacesRequest{
			PageSize:      pagesize,
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		s.True(len(resp.Namespaces) <= int(pagesize))
		if len(resp.Namespaces) > 0 {
			namespaces[resp.Namespaces[0].NamespaceInfo.GetName()] = resp.Namespaces[0]
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
		s.Equal(expectedResult[name].GetReplicationConfig().GetClusters(), namespace.ConvertClusterReplicationConfigFromProto(ns.GetReplicationConfig().GetClusters()))
		s.Equal(expectedResult[name].GetReplicationConfig().GetState(), ns.GetReplicationConfig().GetState())
		s.Equal(expectedResult[name].GetFailoverVersion(), ns.GetFailoverVersion())
	}
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_WithOneCluster() {
	const namespace = "namespace-to-register"
	clusterName := "cluster1"
	retention := durationpb.New(10 * 24 * time.Hour)
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	}
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(namespace, request.Namespace.GetInfo().GetDescription())
			s.Equal(registerRequest.IsGlobalNamespace, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(clusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
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
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: retention,
		ActiveClusterName:                clusterName,
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{
				ClusterName: clusterName,
			},
			{
				ClusterName: clusterName2,
			},
		},
		IsGlobalNamespace: true,
	}
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(namespace, request.Namespace.GetInfo().GetDescription())
			s.Equal(registerRequest.IsGlobalNamespace, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(clusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
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
		registerRequest := &workflowservice.RegisterNamespaceRequest{
			Namespace:                        "random namespace name",
			Description:                      "random namespace name",
			WorkflowExecutionRetentionPeriod: durationpb.New(invalidDuration),
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
			WorkflowExecutionRetentionPeriod: durationpb.New(invalidDuration),
			IsGlobalNamespace:                true,
		}
		resp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_InvalidRetentionPeriod() {
	namespace := uuid.New()
	version := int64(1)
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   uuid.New(),
				Name: namespace,
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		},
	}, nil).AnyTimes()
	for _, invalidDuration := range []time.Duration{
		0,
		-1 * time.Hour,
		1 * time.Millisecond,
		30 * time.Minute,
	} {
		updateRequest := &workflowservice.UpdateNamespaceRequest{
			Namespace: namespace,
			Config: &namespacepb.NamespaceConfig{
				WorkflowExecutionRetentionTtl: durationpb.New(invalidDuration),
			},
		}
		resp, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
		s.Equal(errInvalidRetentionPeriod, err)
		s.Nil(resp)
	}
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_PromoteLocalNamespace() {
	namespace := "local-ns-to-be-promoted"
	clusterName := "cluster1"
	version := int64(1)
	nid := uuid.New()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
		NotificationVersion: version,
	}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName,
				Clusters:          []string{clusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName,
				Clusters:          []string{clusterName},
			},
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             2,
		},
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

	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Namespace:        namespace,
		PromoteNamespace: true,
	}
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_UpdateActiveClusterWithHandoverState() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    nid,
				Name:  namespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_HANDOVER,
			},
		},
		IsGlobalNamespace: true,
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Eq(&persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    nid,
				Name:  namespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				State:             enumspb.REPLICATION_STATE_HANDOVER,
				FailoverHistory: []*persistencespb.FailoverStatus{
					{
						FailoverTime:    timestamppb.New(update1Time),
						FailoverVersion: 2,
					},
				},
			},
			ConfigVersion:               int64(0),
			FailoverNotificationVersion: version,
			FailoverVersion:             int64(2),
		},
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	}))
	s.fakeClock.Update(update1Time)
	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName2,
		},
		PromoteNamespace: true,
	}
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_ChangeActiveClusterWithoutUpdatingReplicationState() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				FailoverHistory: []*persistencespb.FailoverStatus{
					{
						FailoverTime:    timestamppb.New(update1Time),
						FailoverVersion: 2,
					},
				},
			},
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             2,
		},
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	})
	s.fakeClock.Update(update1Time)
	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName2,
		},
		PromoteNamespace: true,
	}
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

// Test that the number of replication statuses is limited
func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_UpdateActiveCluster_LimitRecordHistory() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.New()
	version := int64(100)
	clusterName1 := "cluster1"
	clusterName2 := "cluster2"
	failoverHistory := []*persistencespb.FailoverStatus{
		{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(2),
		},
		{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(11),
		},
		{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(12),
		},
		{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(21),
		},
		{
			FailoverTime:    timestamppb.New(update1Time),
			FailoverVersion: int64(22),
		},
	}
	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: "cluster2",
		},
		PromoteNamespace: true,
	}
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
				FailoverHistory:   failoverHistory,
			},
		},
	}, nil)
	sizeLimitedFailoverHistory := slices.Clone(failoverHistory)
	sizeLimitedFailoverHistory = append(sizeLimitedFailoverHistory, &persistencespb.FailoverStatus{
		FailoverTime:    timestamppb.New(update1Time),
		FailoverVersion: 32,
	})
	sizeLimitedFailoverHistory = sizeLimitedFailoverHistory[0:]
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				FailoverHistory:   sizeLimitedFailoverHistory,
			},
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             32,
		},
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
		{
			ClusterName: activeClusterName,
		},
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

	resp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	})
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
		{
			ClusterName: activeClusterName,
		},
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

	resp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	})
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(false, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
			s.Equal([]string{cluster.TestCurrentClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
	})
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
		{
			ClusterName: activeClusterName,
		},
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(description, request.Namespace.GetInfo().GetDescription())
			s.Equal(email, request.Namespace.GetInfo().GetOwner())
			s.Equal(data, request.Namespace.GetInfo().GetData())
			s.Equal(false, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(activeClusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
			s.Equal([]string{activeClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: retention,
		Clusters:                         clusters,
		ActiveClusterName:                activeClusterName,
		Data:                             data,
		IsGlobalNamespace:                isGlobalNamespace,
	})
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
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: durationpb.New(retention),
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
	})
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
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: activeClusterName,
				Clusters:          []string{activeClusterName},
			},
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: activeClusterName},
			},
		},
	})
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(true, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
			s.Equal([]string{cluster.TestCurrentClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	})
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)
}

func (s *namespaceHandlerCommonSuite) TestRegisterGlobalNamespace_NoDefault() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(24 * time.Hour)
	description := "description"
	email := "email"
	clusters := []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: cluster.TestCurrentClusterName,
		},
		{
			ClusterName: cluster.TestAlternativeClusterName,
		},
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(true, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
			s.Equal([]string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      description,
		OwnerEmail:                       email,
		WorkflowExecutionRetentionPeriod: retention,
		Clusters:                         clusters,
		ActiveClusterName:                cluster.TestCurrentClusterName,
		Data:                             data,
		IsGlobalNamespace:                true,
	})
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
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: durationpb.New(retention),
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
		IsGlobalNamespace: true,
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestUpdateGlobalNamespace_AllAttrSet() {
	namespace := s.getRandomNamespace()
	description := "some random description"
	email := "some random email"
	retention := durationpb.New(7 * time.Hour * 24)
	data := map[string]string{"some random key": "some random value"}
	version := int64(100)
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
	})
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
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.GetState())
			s.Equal(namespace, request.Namespace.GetInfo().GetName())
			s.Equal(false, request.IsGlobalNamespace)
			s.Equal(retention, request.Namespace.GetConfig().GetRetention())
			s.Equal(cluster.TestCurrentClusterName, request.Namespace.GetReplicationConfig().ActiveClusterName)
			s.Equal([]string{cluster.TestCurrentClusterName}, request.Namespace.GetReplicationConfig().GetClusters())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetHistoryArchivalState())
			s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, request.Namespace.GetConfig().GetVisibilityArchivalState())
			return &persistence.CreateNamespaceResponse{}, nil
		},
	)

	registerResp, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
	})
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
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: activeClusterName,
				Clusters:          []string{activeClusterName},
			},
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
	})
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestRegisterGlobalNamespace_NotMaster() {
	namespace := s.getRandomNamespace()
	retention := durationpb.New(24 * time.Hour)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(false).AnyTimes()

	_, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	})
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
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nid,
				Name:        namespace,
				Description: description,
				Owner:       email,
				Data:        data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   retention,
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []string{cluster.TestCurrentClusterName},
			},
			ConfigVersion:               1,
			FailoverNotificationVersion: 0,
			FailoverVersion:             0,
		},
		IsGlobalNamespace:   false,
		NotificationVersion: version,
	})
	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  email,
			Data:        data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: retention,
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:            "",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "",
			BadBinaries:                   &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
	})
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestFailoverGlobalNamespace_NotMaster() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	namespace := "global-ns-to-be-migrated"
	nid := uuid.New()
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
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName1,
				Clusters:          []string{clusterName1, clusterName2},
			},
		},
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   nid,
				Name: namespace,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterName2,
				Clusters:          []string{clusterName1, clusterName2},
				FailoverHistory: []*persistencespb.FailoverStatus{
					{
						FailoverTime:    timestamppb.New(update1Time),
						FailoverVersion: 2,
					},
				},
			},
			ConfigVersion:               0,
			FailoverNotificationVersion: version,
			FailoverVersion:             2,
		},
		IsGlobalNamespace:   true,
		NotificationVersion: version,
	})
	s.fakeClock.Update(update1Time)
	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName2,
		},
		PromoteNamespace: true,
	}
	_, err := s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) getRandomNamespace() string {
	return "namespace" + uuid.New()
}
