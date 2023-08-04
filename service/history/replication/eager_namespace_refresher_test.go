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
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	EagerNamespaceRefresherSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shard.ContextTest
		mockMetadataManager     *persistence.MockMetadataManager
		mockNamespaceRegistry   *namespace.MockRegistry
		eagerNamespaceRefresher EagerNamespaceRefresher
		logger                  log.Logger
	}
)

func (s *EagerNamespaceRefresherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.mockMetadataManager = persistence.NewMockMetadataManager(s.controller)
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.eagerNamespaceRefresher = NewEagerNamespaceRefresher(s.mockMetadataManager, s.mockNamespaceRegistry, s.logger)
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

func TestEagerNamespaceRefresherSuite(t *testing.T) {
	suite.Run(t, new(EagerNamespaceRefresherSuite))
}
