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

package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"

	"github.com/stretchr/testify/suite"
)

const mockCurrentCuster = "current_cluster_1"

type (
	EagerNamespaceRefresherSuite struct {
		suite.Suite
		*require.Assertions

		controller                  *gomock.Controller
		mockShard                   *shard.ContextTest
		mockMetadataManager         *persistence.MockMetadataManager
		mockNamespaceRegistry       *namespace.MockRegistry
		eagerNamespaceRefresher     EagerNamespaceRefresher
		logger                      log.Logger
		clientBean                  *client.MockBean
		mockReplicationTaskExecutor *namespace.MockReplicationTaskExecutor
		currentCluster              string
		mockMetricsHandler          metrics.Handler
		remoteAdminClient           *adminservicemock.MockAdminServiceClient
	}
)

func (s *EagerNamespaceRefresherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.mockMetadataManager = persistence.NewMockMetadataManager(s.controller)
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.remoteAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(s.remoteAdminClient, nil).AnyTimes()
	scope := tally.NewTestScope("test", nil)
	s.mockReplicationTaskExecutor = namespace.NewMockReplicationTaskExecutor(s.controller)
	s.mockMetricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(
		metrics.ServiceNameTag("serviceName"))
	s.eagerNamespaceRefresher = NewEagerNamespaceRefresher(
		s.mockMetadataManager,
		s.mockNamespaceRegistry,
		s.logger,
		s.clientBean,
		s.mockReplicationTaskExecutor,
		mockCurrentCuster,
		s.mockMetricsHandler,
	)
}

func TestEagerNamespaceRefresherSuite(t *testing.T) {
	suite.Run(t, new(EagerNamespaceRefresherSuite))
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)
	currentFailoverVersion := targetFailoverVersion - 1

	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: currentFailoverVersion,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED, // Still must be included.
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	ns := namespace.FromPersistentState(nsResponse)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(ns, nil).Times(1)

	s.mockMetadataManager.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 123}, nil).Times(1)
	s.mockMetadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: namespaceID,
	}).Return(nsResponse, nil).Times(1)
	s.mockMetadataManager.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)

	s.Nil(err)
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion_TargetVersionSmallerThanVersionInCache() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)
	currentFailoverVersion := targetFailoverVersion + 1

	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: currentFailoverVersion,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED, // Still must be included.
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	ns := namespace.FromPersistentState(nsResponse)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(ns, nil).Times(1)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)

	s.Nil(err)
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion_TargetVersionSmallerThanVersionInPersistent() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)
	currentFailoverVersion := targetFailoverVersion - 1

	nsFromCache := namespace.FromPersistentState(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: currentFailoverVersion,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED,
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	})

	nsFromPersistent := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: targetFailoverVersion,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED,
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}

	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nsFromCache, nil).Times(1)

	s.mockMetadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: namespaceID,
	}).Return(nsFromPersistent, nil).Times(1)

	s.mockMetadataManager.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).Times(0)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)

	s.Nil(err)
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion_NamespaceNotFoundFromRegistry() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)

	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)
	s.Nil(err)
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion_GetNamespaceErrorFromRegistry() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)

	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, errors.New("some error")).Times(1)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)
	s.Error(err)
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion_GetNamespaceErrorFromPersistent() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)
	currentFailoverVersion := targetFailoverVersion - 1

	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: currentFailoverVersion,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED, // Still must be included.
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	ns := namespace.FromPersistentState(nsResponse)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(ns, nil).Times(1)

	s.mockMetadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: namespaceID,
	}).Return(nil, errors.New("some error")).Times(1)
	// No more interaction with metadata manager
	s.mockMetadataManager.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 123}, nil).Times(0)
	s.mockMetadataManager.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).Times(0)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)

	s.Error(err)
}

func (s *EagerNamespaceRefresherSuite) TestUpdateNamespaceFailoverVersion_GetMetadataErrorFrom() {
	namespaceID := "test-namespace-id"
	targetFailoverVersion := int64(100)
	currentFailoverVersion := targetFailoverVersion - 1

	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: currentFailoverVersion,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED, // Still must be included.
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	ns := namespace.FromPersistentState(nsResponse)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(ns, nil).Times(1)

	s.mockMetadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: namespaceID,
	}).Return(nsResponse, nil).Times(1)
	s.mockMetadataManager.EXPECT().GetMetadata(gomock.Any()).Return(nil, errors.New("some error")).Times(1)

	// No more interaction with metadata manager
	s.mockMetadataManager.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).Times(0)

	err := s.eagerNamespaceRefresher.UpdateNamespaceFailoverVersion(namespace.ID(namespaceID), targetFailoverVersion)

	s.Error(err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_Success() {
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespaceId.String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_1"},
			},
		},
		IsGlobalNamespace: true,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	ns, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Nil(err)
	s.Equal(namespaceId, ns.ID())
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_NamespaceNotBelongsToCurrentCluster() {
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: "not_current_cluster_1"},
				{ClusterName: "not_current_cluster_2"},
			},
		},
		IsGlobalNamespace: true,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.IsType(&serviceerror.FailedPrecondition{}, err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_ExecutorReturnsError() {
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_2"},
			},
		},
		IsGlobalNamespace: true,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	expectedError := errors.New("some error")
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(expectedError)
	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.Equal(expectedError, err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_NamespaceIsNotGlobalNamespace() {
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		IsGlobalNamespace: false,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.IsType(&serviceerror.FailedPrecondition{}, err)
}
