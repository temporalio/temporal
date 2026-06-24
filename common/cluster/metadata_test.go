package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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
		*require.Assertions

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
	s.clusterName = uuid.NewString()
	s.secondClusterName = uuid.NewString()
	s.thirdClusterName = uuid.NewString()

	clusterInfo := map[string]ClusterInformation{
		s.clusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(1),
			RPCAddress:             uuid.NewString(),
			ShardCount:             1,
			version:                1,
		},
		s.secondClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(4),
			RPCAddress:             uuid.NewString(),
			ShardCount:             2,
			version:                1,
		},
		s.thirdClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(5),
			RPCAddress:             uuid.NewString(),
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
	clusterName := s.metadata.ClusterNameForFailoverVersion(true, 101)
	s.Equal(s.clusterName, clusterName)

	clusterName2 := s.metadata.ClusterNameForFailoverVersion(true, 204)
	s.Equal(s.secondClusterName, clusterName2)

	clusterName3 := s.metadata.ClusterNameForFailoverVersion(true, 217)
	s.Equal(unknownClusterNamePrefix+"17", clusterName3)
}

func (s *metadataSuite) Test_RegisterMetadataChangeCallback() {
	s.metadata.RegisterMetadataChangeCallback(
		s,
		func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
			s.Len(newClusterMetadata, 3)
		})

	s.metadata.UnRegisterMetadataChangeCallback(s)
	s.Empty(s.metadata.clusterChangeCallback)
}

func (s *metadataSuite) Test_RefreshClusterMetadata_Success() {
	id := uuid.NewString()
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

		oldMetadata, ok = oldClusterMetadata[s.thirdClusterName]
		s.True(ok)
		s.NotNil(oldMetadata)
		newMetadata, ok = newClusterMetadata[s.thirdClusterName]
		s.True(ok)
		s.NotNil(newMetadata)
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
						ClusterAddress:         uuid.NewString(),
						HttpAddress:            uuid.NewString(),
					},
					Version: 1,
				},
				{
					// Updated, included in callback (Tags changed)
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.thirdClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 5,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.NewString(),
						HttpAddress:            uuid.NewString(),
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
						ClusterAddress:         uuid.NewString(),
						HttpAddress:            uuid.NewString(),
						Tags:                   map[string]string{"test": "test"},
					},
					Version: 2,
				},
			},
		}, nil)
	err := s.metadata.refreshClusterMetadata(context.Background())
	s.NoError(err)
	clusterInfo := s.metadata.GetAllClusterInfo()
	s.Equal("test", clusterInfo[s.thirdClusterName].Tags["test"])
	s.Equal("test", clusterInfo[id].Tags["test"])
}

func (s *metadataSuite) Test_ValidateClusterInformation() {
	const increment int64 = 10
	cases := []struct {
		name        string
		clusterName string
		info        ClusterInformation
		wantErr     string
	}{
		{
			name:        "happy path",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: 1, RPCAddress: "host:7233"},
		},
		{
			name:        "disabled with empty RPCAddress is allowed",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: false, InitialFailoverVersion: 1, RPCAddress: ""},
		},
		{
			name:        "empty cluster name",
			clusterName: "",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: 1, RPCAddress: "host:7233"},
			wantErr:     "cluster name must not be empty",
		},
		{
			name:        "InitialFailoverVersion zero",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: 0, RPCAddress: "host:7233"},
			wantErr:     "InitialFailoverVersion must be > 0",
		},
		{
			name:        "InitialFailoverVersion negative",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: -1, RPCAddress: "host:7233"},
			wantErr:     "InitialFailoverVersion must be > 0",
		},
		{
			name:        "InitialFailoverVersion equal to increment",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: increment, RPCAddress: "host:7233"},
			wantErr:     "must be < FailoverVersionIncrement",
		},
		{
			name:        "InitialFailoverVersion greater than increment",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: increment + 1, RPCAddress: "host:7233"},
			wantErr:     "must be < FailoverVersionIncrement",
		},
		{
			name:        "enabled with empty RPCAddress",
			clusterName: "alpha",
			info:        ClusterInformation{Enabled: true, InitialFailoverVersion: 1, RPCAddress: ""},
			wantErr:     "RPCAddress must not be empty when Enabled=true",
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			err := ValidateClusterInformation(tc.clusterName, tc.info, increment)
			if tc.wantErr == "" {
				s.NoError(err)
			} else {
				s.Require().Error(err)
				s.Contains(err.Error(), tc.wantErr)
			}
		})
	}
}

// Test_RefreshClusterMetadata_InvalidRow_PreservesState verifies that a bad row
// in cluster_metadata (one violating the per-cluster invariants) is rejected
// without mutating the in-memory state. Before the hardening this scenario
// panicked the refresher goroutine.
func (s *metadataSuite) Test_RefreshClusterMetadata_InvalidRow_PreservesState() {
	badClusterName := uuid.NewString()
	s.mockClusterMetadataStore.EXPECT().ListClusterMetadata(gomock.Any(), gomock.Any()).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				// Existing clusters unchanged (Version matches in-memory, so they
				// short-circuit out of the diff loop).
				{
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.clusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 1,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.NewString(),
					},
					Version: 1,
				},
				{
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.secondClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 4,
						HistoryShardCount:      2,
						ClusterAddress:         uuid.NewString(),
					},
					Version: 1,
				},
				{
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            s.thirdClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 5,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.NewString(),
					},
					Version: 1,
				},
				// New cluster row with InitialFailoverVersion=0, which violates
				// the invariant. This is the row the refresher must reject.
				{
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterName:            badClusterName,
						IsConnectionEnabled:    true,
						InitialFailoverVersion: 0,
						HistoryShardCount:      1,
						ClusterAddress:         uuid.NewString(),
					},
					Version: 1,
				},
			},
		}, nil)

	before := s.metadata.GetAllClusterInfo()
	err := s.metadata.refreshClusterMetadata(context.Background())
	s.Require().Error(err)
	s.Contains(err.Error(), "rejecting cluster metadata refresh")

	after := s.metadata.GetAllClusterInfo()
	s.Equal(before, after, "in-memory state must be unchanged when refresh is rejected")
	_, badPresent := after[badClusterName]
	s.False(badPresent, "bad cluster must not have been partially added")
}

func (s *metadataSuite) Test_ListAllClusterMetadataFromDB_Success() {
	nextPageSizeToken := []byte{1}
	newClusterName := uuid.NewString()
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
						ClusterAddress:         uuid.NewString(),
						HttpAddress:            uuid.NewString(),
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
						ClusterAddress:         uuid.NewString(),
						HttpAddress:            uuid.NewString(),
					},
					Version: 2,
				},
			},
		}, nil).Times(1)

	resp, err := s.metadata.listAllClusterMetadataFromDB(context.Background())
	s.NoError(err)
	s.Len(resp, 2)
}
