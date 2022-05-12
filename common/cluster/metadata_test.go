// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2021 Uber Technologies, Inc.
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
package cluster

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/server/common/dynamicconfig"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	metadataSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockClusterMetadataStore *persistence.MockClusterMetadataManager
		metadata                 *metadataImpl

		isGlobalNamespaceEnabled bool
		failoverVersionIncrement int64
		clusterName              string
		secondClusterName        string
	}
)

func TestMetadataSuite(t *testing.T) {
	s := new(metadataSuite)
	suite.Run(t, s)
}

func (s *metadataSuite) SetupSuite() {
}

func (s *metadataSuite) TearDownSuite() {

}

func (s *metadataSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.mockClusterMetadataStore = persistence.NewMockClusterMetadataManager(s.controller)

	s.isGlobalNamespaceEnabled = true
	s.failoverVersionIncrement = 100
	s.clusterName = uuid.New()
	s.secondClusterName = uuid.New()

	clusterInfo := map[string]ClusterInformation{
		s.clusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(1),
			RPCAddress:             uuid.New(),
			version:                1,
		},
		s.secondClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(4),
			RPCAddress:             uuid.New(),
			version:                1,
		},
	}
	s.metadata = NewMetadata(
		s.isGlobalNamespaceEnabled,
		s.failoverVersionIncrement,
		s.clusterName,
		s.clusterName,
		clusterInfo,
		s.mockClusterMetadataStore,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		log.NewNoopLogger(),
	).(*metadataImpl)
}

func (s *metadataSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *metadataSuite) Test_Initialization() {
	s.Equal(s.isGlobalNamespaceEnabled, s.metadata.IsGlobalNamespaceEnabled())
	s.Equal(s.clusterName, s.metadata.GetMasterClusterName())
	s.Equal(s.clusterName, s.metadata.GetCurrentClusterName())
	s.True(s.metadata.IsMasterCluster())
	s.Equal(s.failoverVersionIncrement, s.metadata.GetFailoverVersionIncrement())
}

func (s *metadataSuite) Test_GetNextFailoverVersion() {
	currentVersion := int64(102)
	s.Equal(currentVersion+s.failoverVersionIncrement-1, s.metadata.GetNextFailoverVersion(s.clusterName, currentVersion))
}

func (s *metadataSuite) Test_IsVersionFromSameCluster() {
	s.True(s.metadata.IsVersionFromSameCluster(101, 1001))
	s.False(s.metadata.IsVersionFromSameCluster(101, 103))
}

func (s *metadataSuite) Test_ClusterNameForFailoverVersion() {
	s.Equal(s.clusterName, s.metadata.ClusterNameForFailoverVersion(true, 101))
	s.Equal(s.secondClusterName, s.metadata.ClusterNameForFailoverVersion(true, 204))
}

func (s *metadataSuite) Test_RegisterMetadataChangeCallback() {
	s.metadata.RegisterMetadataChangeCallback(
		s,
		func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
			s.Equal(2, len(newClusterMetadata))
		})

	s.metadata.UnRegisterMetadataChangeCallback(s)
	s.Equal(0, len(s.metadata.clusterChangeCallback))
}

func (s *metadataSuite) Test_RefreshClusterMetadata_Success() {
	id := uuid.New()
	s.metadata.clusterChangeCallback[id] = func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
		oldMetadata, ok := oldClusterMetadata[id]
		s.True(ok)
		s.Nil(oldMetadata)
		newMetadata, ok := newClusterMetadata[id]
		s.True(ok)
		s.NotNil(newMetadata)

		oldMetadata, ok = oldClusterMetadata[s.secondClusterName]
		s.True(ok)
		s.NotNil(oldMetadata)
		newMetadata, ok = newClusterMetadata[s.secondClusterName]
		s.True(ok)
		s.Nil(newMetadata)
	}

	s.mockClusterMetadataStore.EXPECT().ListClusterMetadata(gomock.Any(), gomock.Any()).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					ClusterMetadata: persistencespb.ClusterMetadata{
						ClusterName:            s.clusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 1,
						ClusterAddress:         uuid.New(),
					},
					Version: 1,
				},
				{
					ClusterMetadata: persistencespb.ClusterMetadata{
						ClusterName:            id,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 2,
						ClusterAddress:         uuid.New(),
					},
					Version: 2,
				},
			},
		}, nil)
	err := s.metadata.refreshClusterMetadata(context.Background())
	s.NoError(err)
}

func (s *metadataSuite) Test_ListAllClusterMetadataFromDB_Success() {
	nextPageSizeToken := []byte{1}
	newClusterName := uuid.New()
	s.mockClusterMetadataStore.EXPECT().ListClusterMetadata(gomock.Any(), &persistence.ListClusterMetadataRequest{
		PageSize:      defaultClusterMetadataPageSize,
		NextPageToken: nil,
	}).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					ClusterMetadata: persistencespb.ClusterMetadata{
						ClusterName:            s.clusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 1,
						ClusterAddress:         uuid.New(),
					},
					Version: 1,
				},
			},
			NextPageToken: nextPageSizeToken,
		}, nil).Times(1)
	s.mockClusterMetadataStore.EXPECT().ListClusterMetadata(gomock.Any(), &persistence.ListClusterMetadataRequest{
		PageSize:      defaultClusterMetadataPageSize,
		NextPageToken: nextPageSizeToken,
	}).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					ClusterMetadata: persistencespb.ClusterMetadata{
						ClusterName:            newClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 2,
						ClusterAddress:         uuid.New(),
					},
					Version: 2,
				},
			},
		}, nil).Times(1)

	resp, err := s.metadata.listAllClusterMetadataFromDB(context.Background())
	s.NoError(err)
	s.Equal(2, len(resp))
}
