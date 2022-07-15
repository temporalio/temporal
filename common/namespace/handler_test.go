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

	"go.temporal.io/server/common/log"

	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistence2 "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log/tag"

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	dc "go.temporal.io/server/common/dynamicconfig"
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
		fakeClock               *clock.EventTimeSource

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
	s.fakeClock = clock.NewEventTimeSource()
	s.handler = NewHandler(
		dc.GetIntPropertyFilteredByNamespace(s.maxBadBinaryCount),
		logger,
		s.metadataMgr,
		s.ClusterMetadata,
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

	namespace2 := s.getRandomNamespace() + "sched"
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
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
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
				Name:              namespace2,
				State:             enumspb.NAMESPACE_STATE_REGISTERED,
				Description:       description2,
				OwnerEmail:        email2,
				Data:              data2,
				Id:                "",
				SupportsSchedules: true,
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
	}, namespaces,
	)
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace() {
	const namespace = "namespace-to-register"
	retention := timestamp.DurationPtr(10 * 24 * time.Hour)
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: retention,
		IsGlobalNamespace:                true,
	}
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	_, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.NoError(err)

	nsResp, err := s.MetadataManager.GetNamespace(
		context.Background(),
		&persistence.GetNamespaceRequest{Name: namespace},
	)
	s.NoError(err)

	wantNsDetail := persistence2.NamespaceDetail{
		Info: &persistence2.NamespaceInfo{
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Name:        namespace,
			Description: namespace,
		},
		Config: &persistence2.NamespaceConfig{Retention: retention},
		ReplicationConfig: &persistence2.NamespaceReplicationConfig{
			ActiveClusterName: s.ClusterMetadata.GetMasterClusterName(),
			Clusters:          []string{s.ClusterMetadata.GetMasterClusterName()},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
	}

	s.Equal(registerRequest.IsGlobalNamespace, nsResp.IsGlobalNamespace)
	gotNsDetail := nsResp.Namespace
	s.Equal(wantNsDetail.Info.Name, gotNsDetail.Info.Name)
	s.Equal(wantNsDetail.Info.Description, gotNsDetail.Info.Description)
	s.Equal(wantNsDetail.Info.State, gotNsDetail.Info.State)
	s.Equal(wantNsDetail.Config.Retention, gotNsDetail.Config.Retention)
	s.Equal(wantNsDetail.ReplicationConfig, gotNsDetail.ReplicationConfig)
}

func (s *namespaceHandlerCommonSuite) TestRegisterNamespace_InvalidRetentionPeriod() {
	// local
	for _, invalidDuration := range []time.Duration{
		0,
		-1 * time.Hour,
		1 * time.Millisecond,
		10 * 365 * 24 * time.Hour,
	} {
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

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_PromoteLocalNamespace() {
	namespace := "local-ns-to-be-promoted"
	registerRequest := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(24 * time.Hour),
		IsGlobalNamespace:                false,
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), registerRequest)
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	updateRequest := &workflowservice.UpdateNamespaceRequest{
		Namespace:        namespace,
		PromoteNamespace: true,
	}
	_, err = s.handler.UpdateNamespace(context.Background(), updateRequest)
	s.NoError(err)

	descResp, err := s.handler.DescribeNamespace(
		context.Background(), &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		},
	)
	s.NoError(err)
	s.True(descResp.IsGlobalNamespace)
	s.Equal(cluster.TestCurrentClusterInitialFailoverVersion, descResp.FailoverVersion)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_UpdateActiveCluster() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()

	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	update2Time := update1Time.Add(17 * time.Minute)

	namespace := "global-ns-to-be-migrated"
	registerReq := workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(24 * time.Hour),
		IsGlobalNamespace:                true,
		ActiveClusterName:                s.ClusterMetadata.GetCurrentClusterName(),
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &registerReq)
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	s.checkActiveClusterName(namespace, cluster.TestCurrentClusterName)

	s.fakeClock.Update(update1Time)
	s.migrateNamespace(namespace, cluster.TestAlternativeClusterName)
	handover1Time := s.fakeClock.Now()

	// Migrate back to the source cluster
	s.fakeClock.Update(update2Time)
	s.migrateNamespace(namespace, cluster.TestCurrentClusterName)
	handover2Time := s.fakeClock.Now()

	// Verify that the replication history was written
	getNsResp, err := s.MetadataManager.GetNamespace(
		context.Background(),
		&persistence.GetNamespaceRequest{Name: namespace},
	)
	s.NoError(err)
	s.True(getNsResp.IsGlobalNamespace)

	wantHistory := []*persistence2.FailoverStatus{
		{FailoverTime: &handover1Time, FailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion},
		{
			FailoverTime:    &handover2Time,
			FailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion + cluster.TestFailoverVersionIncrement,
		},
	}

	s.Equal(wantHistory, getNsResp.Namespace.ReplicationConfig.FailoverHistory)
}

// Test that the number of replication statuses is limited
func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_UpdateActiveCluster_LimitRecordHistory() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()

	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)

	namespace := "global-ns-to-be-migrated-many-times"
	registerReq := workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(24 * time.Hour),
		IsGlobalNamespace:                true,
		ActiveClusterName:                s.ClusterMetadata.GetCurrentClusterName(),
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &registerReq)
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	s.checkActiveClusterName(namespace, cluster.TestCurrentClusterName)

	s.fakeClock.Update(update1Time)

	for i := 0; i < 10; i++ {
		s.migrateNamespace(namespace, cluster.TestAlternativeClusterName)
		s.migrateNamespace(namespace, cluster.TestCurrentClusterName)
	}

	// Verify that the replication history was written
	getNsResp, err := s.MetadataManager.GetNamespace(
		context.Background(),
		&persistence.GetNamespaceRequest{Name: namespace},
	)
	s.NoError(err)
	s.True(getNsResp.IsGlobalNamespace)

	var wantClusters []int64
	for i := 0; i < 5; i++ {
		wantClusters = append(
			wantClusters,
			cluster.TestAlternativeClusterInitialFailoverVersion,
			cluster.TestCurrentClusterInitialFailoverVersion,
		)
	}

	var gotClusters []int64
	for _, s := range getNsResp.Namespace.ReplicationConfig.FailoverHistory {
		gotClusters = append(gotClusters, s.FailoverVersion%cluster.TestFailoverVersionIncrement)
	}

	s.Equal(wantClusters, gotClusters)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_HandoverFails() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()

	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)
	update2Time := update1Time.Add(17 * time.Minute)

	namespace := "global-ns-failed-handover"
	registerReq := workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(24 * time.Hour),
		IsGlobalNamespace:                true,
		ActiveClusterName:                s.ClusterMetadata.GetCurrentClusterName(),
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &registerReq)
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	s.checkActiveClusterName(namespace, cluster.TestCurrentClusterName)

	s.fakeClock.Update(update1Time)
	s.migrateNamespace(namespace, cluster.TestAlternativeClusterName)
	handover1Time := s.fakeClock.Now()

	s.fakeClock.Update(update2Time)
	s.setReplicationState(namespace, enumspb.REPLICATION_STATE_HANDOVER)
	s.clockTick()

	// Handover fails for unspecified reasons so replication state goes back to NORMAL without updating Active Cluster

	s.setReplicationState(namespace, enumspb.REPLICATION_STATE_NORMAL)

	// Verify that the replication history was written
	getNsResp, err := s.MetadataManager.GetNamespace(
		context.Background(),
		&persistence.GetNamespaceRequest{Name: namespace},
	)
	s.NoError(err)
	s.True(getNsResp.IsGlobalNamespace)

	wantHistory := []*persistence2.FailoverStatus{
		{FailoverTime: &handover1Time, FailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion},
	}

	s.Equal(wantHistory, getNsResp.Namespace.ReplicationConfig.FailoverHistory)
}

func (s *namespaceHandlerCommonSuite) TestUpdateNamespace_ChangeActiveClusterWithoutUpdatingReplicationState() {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()

	update1Time := time.Date(2011, 12, 27, 23, 44, 55, 999999, time.UTC)

	namespace := "global-ns-update-active-cluster"
	registerReq := workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(24 * time.Hour),
		IsGlobalNamespace:                true,
		ActiveClusterName:                s.ClusterMetadata.GetCurrentClusterName(),
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	}
	registerResp, err := s.handler.RegisterNamespace(context.Background(), &registerReq)
	s.NoError(err)
	s.Equal(&workflowservice.RegisterNamespaceResponse{}, registerResp)

	descResp, err2 := s.handler.DescribeNamespace(
		context.Background(),
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace},
	)
	s.NoError(err2)
	s.Logger.Debug("DescribeNamespace", tag.NewAnyTag("ns", descResp.NamespaceInfo))

	s.checkActiveClusterName(namespace, cluster.TestCurrentClusterName)

	s.fakeClock.Update(update1Time)
	s.setActiveClusterName(namespace, cluster.TestAlternativeClusterName)

	// Verify that the replication history was written
	getNsResp, err := s.MetadataManager.GetNamespace(
		context.Background(),
		&persistence.GetNamespaceRequest{Name: namespace},
	)
	s.NoError(err)
	s.True(getNsResp.IsGlobalNamespace)

	wantHistory := []*persistence2.FailoverStatus{
		{FailoverTime: &update1Time, FailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion},
	}

	s.Equal(wantHistory, getNsResp.Namespace.ReplicationConfig.FailoverHistory)
}

func (s *namespaceHandlerCommonSuite) migrateNamespace(namespace string, targetCluster string) {
	s.setReplicationState(namespace, enumspb.REPLICATION_STATE_HANDOVER)
	s.clockTick()

	s.setActiveClusterName(namespace, targetCluster)
	s.clockTick()

	s.setReplicationState(namespace, enumspb.REPLICATION_STATE_NORMAL)
}

func (s *namespaceHandlerCommonSuite) setActiveClusterName(namespace string, newActiveCluster string) {
	setActiveClusterReq := workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: newActiveCluster,
		},
	}
	_, err := s.handler.UpdateNamespace(context.Background(), &setActiveClusterReq)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) setReplicationState(namespace string, replicationState enumspb.ReplicationState) {
	setReplStateReq := workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			State: replicationState,
		},
	}
	_, err := s.handler.UpdateNamespace(context.Background(), &setReplStateReq)
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) checkActiveClusterName(namespace string, wantClusterName string) {
	descResp, err := s.handler.DescribeNamespace(
		context.Background(),
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace},
	)
	s.NoError(err)
	s.Equal(wantClusterName, descResp.ReplicationConfig.ActiveClusterName)
}

func (s *namespaceHandlerCommonSuite) getRandomNamespace() string {
	return "namespace" + uuid.New()
}

func (s *namespaceHandlerCommonSuite) clockTick() {
	s.fakeClock.Update(s.fakeClock.Now().Add(time.Second))
}
