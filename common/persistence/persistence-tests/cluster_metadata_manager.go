package persistencetests

import (
	"context"
	"net"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	versionpb "go.temporal.io/api/version/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/debug"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

type (
	// ClusterMetadataManagerSuite runs tests that cover the ClusterMetadata read/write scenarios
	ClusterMetadataManagerSuite struct {
		*TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that require.NotNil(s.T(), nil) will stop the test,
		// not merely log an error

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation

// SetupTest implementation
func (s *ClusterMetadataManagerSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
}

// TearDownTest implementation
func (s *ClusterMetadataManagerSuite) TearDownTest() {
	// Ensure all tests clean up after themselves
	// Todo: MetaMgr should provide api to clear all members
	s.waitForPrune(1 * time.Second)
	s.cancel()
}

// TearDownSuite implementation
func (s *ClusterMetadataManagerSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// TestClusterMembershipEmptyInitially verifies the GetClusterMembers() works with an initial empty table
func (s *ClusterMetadataManagerSuite) TestClusterMembershipEmptyInitially() {
	resp, err := s.ClusterMetadataManager.GetClusterMembers(s.ctx, &p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10})
	require.Nil(s.T(), err)
	require.NotNil(s.T(), resp)
	require.Empty(s.T(), resp.ActiveMembers)
}

// TestClusterMembershipUpsertCanReadAny verifies that we can UpsertClusterMembership and read our result
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertCanReadAny() {
	req := &p.UpsertClusterMembershipRequest{
		HostID:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		RPCAddress:   net.ParseIP("127.0.0.2"),
		RPCPort:      123,
		Role:         p.Frontend,
		SessionStart: time.Now().UTC(),
		RecordExpiry: time.Second,
	}

	err := s.ClusterMetadataManager.UpsertClusterMembership(s.ctx, req)
	require.Nil(s.T(), err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(s.ctx, &p.GetClusterMembersRequest{})

	require.Nil(s.T(), err)
	require.NotNil(s.T(), resp)
	require.NotEmpty(s.T(), resp.ActiveMembers)

	s.waitForPrune(5 * time.Second)
}

// TestClusterMembershipUpsertCanPageRead verifies that we can UpsertClusterMembership and read our result
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertCanPageRead() {
	expectedIds := make(map[string]int, 100)
	for i := 0; i < 100; i++ {
		hostID := primitives.NewUUID().Downcast()
		expectedIds[primitives.UUIDString(hostID)]++
		req := &p.UpsertClusterMembershipRequest{
			HostID:       hostID,
			RPCAddress:   net.ParseIP("127.0.0.2"),
			RPCPort:      123,
			Role:         p.Frontend,
			SessionStart: time.Now().UTC(),
			RecordExpiry: 3 * time.Second,
		}

		err := s.ClusterMetadataManager.UpsertClusterMembership(s.ctx, req)
		require.NoError(s.T(), err)
	}

	hostCount := 0
	var nextPageToken []byte
	for {
		resp, err := s.ClusterMetadataManager.GetClusterMembers(s.ctx, &p.GetClusterMembersRequest{PageSize: 9, NextPageToken: nextPageToken})
		require.NoError(s.T(), err)
		nextPageToken = resp.NextPageToken
		for _, member := range resp.ActiveMembers {
			expectedIds[primitives.UUIDString(member.HostID)]--
			hostCount++
		}

		if nextPageToken == nil {
			break
		}
	}

	require.Equal(s.T(), 100, hostCount)
	for id, val := range expectedIds {
		require.Zero(s.T(), val, "identifier was either not found in db, or shouldn't be there - "+id)
	}

	s.waitForPrune(5 * time.Second)
}

func (s *ClusterMetadataManagerSuite) validateUpsert(req *p.UpsertClusterMembershipRequest, resp *p.GetClusterMembersResponse, err error) {
	require.Nil(s.T(), err)
	require.NotNil(s.T(), resp)
	require.NotEmpty(s.T(), resp.ActiveMembers)
	require.Equal(s.T(), len(resp.ActiveMembers), 1)
	// Have to round to 1 second due to SQL implementations. Cassandra truncates at 1ms.
	require.Equal(s.T(), resp.ActiveMembers[0].SessionStart.Round(time.Second), req.SessionStart.Round(time.Second))
	require.Equal(s.T(), resp.ActiveMembers[0].RPCAddress.String(), req.RPCAddress.String())
	require.Equal(s.T(), resp.ActiveMembers[0].RPCPort, req.RPCPort)
	require.True(s.T(), resp.ActiveMembers[0].RecordExpiry.After(time.Now().UTC()))
	require.Equal(s.T(), resp.ActiveMembers[0].HostID, req.HostID)
	require.Equal(s.T(), resp.ActiveMembers[0].Role, req.Role)
}

// TestClusterMembershipReadFiltersCorrectly verifies that we can UpsertClusterMembership and read our result using filters
func (s *ClusterMetadataManagerSuite) TestClusterMembershipReadFiltersCorrectly() {
	now := time.Now().UTC()
	req := &p.UpsertClusterMembershipRequest{
		HostID:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		RPCAddress:   net.ParseIP("127.0.0.2"),
		RPCPort:      123,
		Role:         p.Frontend,
		SessionStart: now,
		RecordExpiry: time.Second * 4,
	}

	err := s.ClusterMetadataManager.UpsertClusterMembership(s.ctx, req)
	require.Nil(s.T(), err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10, HostIDEquals: req.HostID},
	)

	s.validateUpsert(req, resp, err)

	time.Sleep(time.Second * 1)
	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Millisecond, HostIDEquals: req.HostID},
	)

	require.Nil(s.T(), err)
	require.NotNil(s.T(), resp)
	require.Empty(s.T(), resp.ActiveMembers)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{RoleEquals: p.Matching},
	)

	require.Nil(s.T(), err)
	require.NotNil(s.T(), resp)
	require.Empty(s.T(), resp.ActiveMembers)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{SessionStartedAfter: time.Now().UTC()},
	)

	require.Nil(s.T(), err)
	require.NotNil(s.T(), resp)
	require.Empty(s.T(), resp.ActiveMembers)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{SessionStartedAfter: now.Add(-time.Minute), RPCAddressEquals: req.RPCAddress, HostIDEquals: req.HostID},
	)

	s.validateUpsert(req, resp, err)
	s.waitForPrune(5 * time.Second)
}

// TestClusterMembershipUpsertExpiresCorrectly verifies RecordExpiry functions properly for ClusterMembership records
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertExpiresCorrectly() {
	req := &p.UpsertClusterMembershipRequest{
		HostID:       uuid.NewUUID(),
		RPCAddress:   net.ParseIP("127.0.0.2"),
		RPCPort:      123,
		Role:         p.Frontend,
		SessionStart: time.Now().UTC(),
		RecordExpiry: time.Second,
	}

	err := s.ClusterMetadataManager.UpsertClusterMembership(s.ctx, req)
	require.NoError(s.T(), err)

	err = s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	require.NoError(s.T(), err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10, HostIDEquals: req.HostID},
	)

	require.NoError(s.T(), err)
	require.NotNil(s.T(), resp)
	require.NotEmpty(s.T(), resp.ActiveMembers)
	require.Equal(s.T(), len(resp.ActiveMembers), 1)
	// Have to round to 1 second due to SQL implementations. Cassandra truncates at 1ms.
	require.Equal(s.T(), resp.ActiveMembers[0].SessionStart.Round(time.Second), req.SessionStart.Round(time.Second))
	require.Equal(s.T(), resp.ActiveMembers[0].RPCAddress.String(), req.RPCAddress.String())
	require.Equal(s.T(), resp.ActiveMembers[0].RPCPort, req.RPCPort)
	require.True(s.T(), resp.ActiveMembers[0].RecordExpiry.After(time.Now().UTC()))
	require.Equal(s.T(), resp.ActiveMembers[0].HostID, req.HostID)
	require.Equal(s.T(), resp.ActiveMembers[0].Role, req.Role)

	s.waitForPrune(5 * time.Second)
}

// waitForPrune waits up for the persistence backend to prune all records. Some persistence backends
// may not remove TTL'd entries at the exact instant they should expire, so we allow some timing flexibility here.
func (s *ClusterMetadataManagerSuite) waitForPrune(waitFor time.Duration) {
	require.Eventually(s.T(), func() bool {
		err := s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
		require.Nil(s.T(), err)

		resp, err := s.ClusterMetadataManager.GetClusterMembers(
			s.ctx,
			&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10},
		)
		require.NoError(s.T(), err)
		require.NotNil(s.T(), resp)
		return len(resp.ActiveMembers) == 0

	},
		waitFor,
		500*time.Millisecond)
}

// TestClusterMembershipUpsertInvalidExpiry verifies we cannot specify a non-positive RecordExpiry duration
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertInvalidExpiry() {
	req := &p.UpsertClusterMembershipRequest{
		HostID:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		RPCAddress:   net.ParseIP("127.0.0.2"),
		RPCPort:      123,
		Role:         p.Frontend,
		SessionStart: time.Now().UTC(),
		RecordExpiry: time.Second * 0,
	}

	err := s.ClusterMetadataManager.UpsertClusterMembership(s.ctx, req)
	require.NotNil(s.T(), err)
	require.IsType(s.T(), err, p.ErrInvalidMembershipExpiry)
}

// TestInitImmutableMetadataReadWrite runs through the various cases of ClusterMetadata behavior
// Cases:
// 1 - Get, no data persisted
// 2 - Init, no data persisted
// 3 - Get, data persisted
// 4 - Init, data persisted
// 5 - Update, add version info and make sure it's persisted and can be retrieved.
// 6 - Delete, no data persisted
func (s *ClusterMetadataManagerSuite) TestInitImmutableMetadataReadWrite() {
	clusterNameToPersist := "testing"
	historyShardsToPersist := int32(43)
	clusterIdToPersist := "12345"
	clusterAddress := "cluster-address"
	clusterHttpAddress := "cluster-http-address"
	failoverVersionIncrement := int64(10)
	initialFailoverVersion := int64(1)

	// Case 1 - Get, mo data persisted
	// Fetch the persisted values, there should be nothing on start.
	// This doesn't error on no row found, but returns an empty record.
	getResp, err := s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	require.NotNil(s.T(), err)
	require.IsType(s.T(), &serviceerror.NotFound{}, err)
	require.Nil(s.T(), getResp)

	// Case 2 - Init, no data persisted yet
	// First commit, this should be persisted
	initialResp, err := s.ClusterMetadataManager.SaveClusterMetadata(
		s.ctx,
		&p.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				ClusterName:              clusterNameToPersist,
				HistoryShardCount:        historyShardsToPersist,
				ClusterId:                clusterIdToPersist,
				ClusterAddress:           clusterAddress,
				HttpAddress:              clusterHttpAddress,
				FailoverVersionIncrement: failoverVersionIncrement,
				InitialFailoverVersion:   initialFailoverVersion,
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			}})

	require.Nil(s.T(), err)
	require.True(s.T(), initialResp) // request should be applied as this is first initialize

	// Case 3 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	require.Nil(s.T(), err)
	require.True(s.T(), getResp != nil)
	require.Equal(s.T(), clusterNameToPersist, getResp.ClusterName)
	require.Equal(s.T(), historyShardsToPersist, getResp.HistoryShardCount)
	require.Equal(s.T(), clusterIdToPersist, getResp.ClusterId)
	require.Equal(s.T(), clusterAddress, getResp.ClusterAddress)
	require.Equal(s.T(), clusterHttpAddress, getResp.HttpAddress)
	require.Equal(s.T(), failoverVersionIncrement, getResp.FailoverVersionIncrement)
	require.Equal(s.T(), initialFailoverVersion, getResp.InitialFailoverVersion)
	require.True(s.T(), getResp.IsGlobalNamespaceEnabled)
	require.True(s.T(), getResp.IsConnectionEnabled)

	// Case 4 - Init, data persisted
	// Attempt to overwrite with new values
	secondResp, err := s.ClusterMetadataManager.SaveClusterMetadata(s.ctx, &p.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:       clusterNameToPersist,
			HistoryShardCount: int32(77),
		}})

	require.Nil(s.T(), err)
	require.False(s.T(), secondResp) // Should not have applied, and should match values from first request

	// Refetch persisted
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initial values
	require.Nil(s.T(), err)
	require.NotNil(s.T(), getResp)
	require.Equal(s.T(), clusterNameToPersist, getResp.ClusterName)
	require.Equal(s.T(), historyShardsToPersist, getResp.HistoryShardCount)
	require.Equal(s.T(), clusterIdToPersist, getResp.ClusterId)
	require.Equal(s.T(), clusterAddress, getResp.ClusterAddress)
	require.Equal(s.T(), clusterHttpAddress, getResp.HttpAddress)
	require.Equal(s.T(), failoverVersionIncrement, getResp.FailoverVersionIncrement)
	require.Equal(s.T(), initialFailoverVersion, getResp.InitialFailoverVersion)
	require.True(s.T(), getResp.IsGlobalNamespaceEnabled)
	require.True(s.T(), getResp.IsConnectionEnabled)

	// Case 5 - Update version info
	getResp.VersionInfo = &versionpb.VersionInfo{
		Current: &versionpb.ReleaseInfo{
			Version: "1.0",
		},
	}
	thirdResp, err := s.ClusterMetadataManager.SaveClusterMetadata(s.ctx, &p.SaveClusterMetadataRequest{
		ClusterMetadata: getResp.ClusterMetadata,
		Version:         getResp.Version,
	})
	require.Nil(s.T(), err)
	require.True(s.T(), thirdResp)
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})
	require.Nil(s.T(), err)
	require.NotNil(s.T(), getResp)
	require.Equal(s.T(), "1.0", getResp.VersionInfo.Current.Version)

	// Case 6 - Delete Cluster Metadata
	err = s.ClusterMetadataManager.DeleteClusterMetadata(s.ctx, &p.DeleteClusterMetadataRequest{ClusterName: clusterNameToPersist})
	require.Nil(s.T(), err)
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	require.NotNil(s.T(), err)
	require.IsType(s.T(), &serviceerror.NotFound{}, err)
	require.Nil(s.T(), getResp)

	// Case 7 - Update current cluster metadata
	clusterNameToPersist = "active"
	initialResp, err = s.ClusterMetadataManager.SaveClusterMetadata(
		s.ctx,
		&p.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				ClusterName:              clusterNameToPersist,
				HistoryShardCount:        historyShardsToPersist,
				ClusterId:                clusterIdToPersist,
				ClusterAddress:           clusterAddress,
				HttpAddress:              clusterHttpAddress,
				FailoverVersionIncrement: failoverVersionIncrement,
				InitialFailoverVersion:   initialFailoverVersion,
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			}})
	require.Nil(s.T(), err)
	require.True(s.T(), initialResp)

	// Case 8 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	require.Nil(s.T(), err)
	require.True(s.T(), getResp != nil)
	require.Equal(s.T(), clusterNameToPersist, getResp.ClusterName)
	require.Equal(s.T(), historyShardsToPersist, getResp.HistoryShardCount)
	require.Equal(s.T(), clusterIdToPersist, getResp.ClusterId)
	require.Equal(s.T(), clusterAddress, getResp.ClusterAddress)
	require.Equal(s.T(), clusterHttpAddress, getResp.HttpAddress)
	require.Equal(s.T(), failoverVersionIncrement, getResp.FailoverVersionIncrement)
	require.Equal(s.T(), initialFailoverVersion, getResp.InitialFailoverVersion)
	require.True(s.T(), getResp.IsGlobalNamespaceEnabled)
	require.True(s.T(), getResp.IsConnectionEnabled)

	// Case 9 - Update current cluster metadata
	getResp.VersionInfo = &versionpb.VersionInfo{
		Current: &versionpb.ReleaseInfo{
			Version: "2.0",
		},
	}
	applied, err := s.ClusterMetadataManager.SaveClusterMetadata(s.ctx, &p.SaveClusterMetadataRequest{
		ClusterMetadata: getResp.ClusterMetadata,
		Version:         getResp.Version,
	})
	require.True(s.T(), applied)
	require.NoError(s.T(), err)

	// Case 10 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})
	require.NoError(s.T(), err)
	require.Equal(s.T(), "2.0", getResp.VersionInfo.Current.Version)

	// Case 11 - List
	_, err = s.ClusterMetadataManager.SaveClusterMetadata(
		s.ctx,
		&p.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				ClusterName:              clusterNameToPersist + "2",
				HistoryShardCount:        historyShardsToPersist,
				ClusterId:                clusterIdToPersist,
				ClusterAddress:           clusterAddress,
				HttpAddress:              clusterHttpAddress,
				FailoverVersionIncrement: failoverVersionIncrement,
				InitialFailoverVersion:   initialFailoverVersion,
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			}})
	require.NoError(s.T(), err)

	resp, err := s.ClusterMetadataManager.ListClusterMetadata(s.ctx, &p.ListClusterMetadataRequest{PageSize: 1})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, len(resp.ClusterMetadata))
	resp, err = s.ClusterMetadataManager.ListClusterMetadata(s.ctx, &p.ListClusterMetadataRequest{PageSize: 1, NextPageToken: resp.NextPageToken})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, len(resp.ClusterMetadata))
}
