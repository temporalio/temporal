package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

type (
	metadataSuite struct {
		suite.Suite

		controller               *gomock.Controller
		mockClusterMetadataStore *persistence.MockClusterMetadataManager
		metadata                 *metadataImpl

		isGlobalNamespaceEnabled bool
		failoverVersionIncrement int64
		clusterName              string
		secondClusterName        string
		thirdClusterName         string
	}
)

func TestMetadataSuite(t *testing.T) {
	s := new(metadataSuite)
	suite.Run(t, s)
}

func (s *metadataSuite) TearDownSuite() {

}

func (s *metadataSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockClusterMetadataStore = persistence.NewMockClusterMetadataManager(s.controller)

	s.isGlobalNamespaceEnabled = true
	s.failoverVersionIncrement = 100
	s.clusterName = uuid.New()
	s.secondClusterName = uuid.New()
	s.thirdClusterName = uuid.New()

	clusterInfo := map[string]ClusterInformation{
		s.clusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(1),
			RPCAddress:             uuid.New(),
			ShardCount:             1,
			version:                1,
		},
		s.secondClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(4),
			RPCAddress:             uuid.New(),
			ShardCount:             2,
			version:                1,
		},
		s.thirdClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(5),
			RPCAddress:             uuid.New(),
			ShardCount:             1,
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
	require.Equal(s.T(), s.isGlobalNamespaceEnabled, s.metadata.IsGlobalNamespaceEnabled())
	require.Equal(s.T(), s.clusterName, s.metadata.GetMasterClusterName())
	require.Equal(s.T(), s.clusterName, s.metadata.GetCurrentClusterName())
	require.True(s.T(), s.metadata.IsMasterCluster())
	require.Equal(s.T(), s.failoverVersionIncrement, s.metadata.GetFailoverVersionIncrement())
}

func (s *metadataSuite) Test_GetNextFailoverVersion() {
	currentVersion := int64(102)
	require.Equal(s.T(), currentVersion+s.failoverVersionIncrement-1, s.metadata.GetNextFailoverVersion(s.clusterName, currentVersion))
}

func (s *metadataSuite) Test_IsVersionFromSameCluster() {
	require.True(s.T(), s.metadata.IsVersionFromSameCluster(101, 1001))
	require.False(s.T(), s.metadata.IsVersionFromSameCluster(101, 103))
}

func (s *metadataSuite) Test_ClusterNameForFailoverVersion() {
	clusterName := s.metadata.ClusterNameForFailoverVersion(true, 101)
	require.Equal(s.T(), s.clusterName, clusterName)

	clusterName2 := s.metadata.ClusterNameForFailoverVersion(true, 204)
	require.Equal(s.T(), s.secondClusterName, clusterName2)

	clusterName3 := s.metadata.ClusterNameForFailoverVersion(true, 217)
	require.Equal(s.T(), unknownClusterNamePrefix+"17", clusterName3)
}

func (s *metadataSuite) Test_RegisterMetadataChangeCallback() {
	s.metadata.RegisterMetadataChangeCallback(
		s,
		func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
			require.Equal(s.T(), 3, len(newClusterMetadata))
		})

	s.metadata.UnRegisterMetadataChangeCallback(s)
	require.Equal(s.T(), 0, len(s.metadata.clusterChangeCallback))
}

func (s *metadataSuite) Test_RefreshClusterMetadata_Success() {
	id := uuid.New()
	s.metadata.clusterChangeCallback[id] = func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
		oldMetadata, ok := oldClusterMetadata[id]
		require.True(s.T(), ok)
		require.Nil(s.T(), oldMetadata)
		newMetadata, ok := newClusterMetadata[id]
		require.True(s.T(), ok)
		require.NotNil(s.T(), newMetadata)

		oldMetadata, ok = oldClusterMetadata[s.secondClusterName]
		require.True(s.T(), ok)
		require.NotNil(s.T(), oldMetadata)
		newMetadata, ok = newClusterMetadata[s.secondClusterName]
		require.True(s.T(), ok)
		require.Nil(s.T(), newMetadata)

		oldMetadata, ok = oldClusterMetadata[s.thirdClusterName]
		require.True(s.T(), ok)
		require.NotNil(s.T(), oldMetadata)
		newMetadata, ok = newClusterMetadata[s.thirdClusterName]
		require.True(s.T(), ok)
		require.NotNil(s.T(), newMetadata)
	}

	s.mockClusterMetadataStore.EXPECT().ListClusterMetadata(gomock.Any(), gomock.Any()).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					// No change and not include in callback
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.clusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 1,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.New(),
						HttpAddress:            uuid.New(),
					},
					Version: 1,
				},
				{
					// Updated, included in callback
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.thirdClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 1,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.New(),
						HttpAddress:            uuid.New(),
						Tags:                   map[string]string{"test": "test"},
					},
					Version: 2,
				},
				{
					// Newly added, included in callback
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            id,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 2,
						HistoryShardCount:      2,
						ClusterAddress:         uuid.New(),
						HttpAddress:            uuid.New(),
						Tags:                   map[string]string{"test": "test"},
					},
					Version: 2,
				},
			},
		}, nil)
	err := s.metadata.refreshClusterMetadata(context.Background())
	require.NoError(s.T(), err)
	clusterInfo := s.metadata.GetAllClusterInfo()
	require.Equal(s.T(), "test", clusterInfo[s.thirdClusterName].Tags["test"])
	require.Equal(s.T(), "test", clusterInfo[id].Tags["test"])
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
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.clusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 1,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.New(),
						HttpAddress:            uuid.New(),
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
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            newClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 2,
						HistoryShardCount:      2,
						ClusterAddress:         uuid.New(),
						HttpAddress:            uuid.New(),
					},
					Version: 2,
				},
			},
		}, nil).Times(1)

	resp, err := s.metadata.listAllClusterMetadataFromDB(context.Background())
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, len(resp))
}
