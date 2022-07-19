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
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

type (
	// ClusterMetadataManagerSuite runs tests that cover the ClusterMetadata read/write scenarios
	ClusterMetadataManagerSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

// SetupSuite implementation
func (s *ClusterMetadataManagerSuite) SetupSuite() {
}

// SetupTest implementation
func (s *ClusterMetadataManagerSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*30)
}

// TearDownTest implementation
func (s *ClusterMetadataManagerSuite) TearDownTest() {
	s.cancel()
}

// TearDownSuite implementation
func (s *ClusterMetadataManagerSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// TestClusterMembershipEmptyInitially verifies the GetClusterMembers() works with an initial empty table
func (s *ClusterMetadataManagerSuite) TestClusterMembershipEmptyInitially() {
	resp, err := s.ClusterMetadataManager.GetClusterMembers(s.ctx, &p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10})
	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)
}

// TestClusterMembershipUpsertCanRead verifies that we can UpsertClusterMembership and read our result
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
	s.Nil(err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(s.ctx, &p.GetClusterMembersRequest{})

	s.Nil(err)
	s.NotNil(resp)
	s.NotEmpty(resp.ActiveMembers)
}

// TestClusterMembershipUpsertCanRead verifies that we can UpsertClusterMembership and read our result
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertCanPageRead() {
	// Expire previous records
	// Todo: MetaMgr should provide api to clear all members
	time.Sleep(time.Second * 3)
	err := s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	s.Nil(err)

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
		s.NoError(err)
	}

	hostCount := 0
	var nextPageToken []byte
	for {
		resp, err := s.ClusterMetadataManager.GetClusterMembers(s.ctx, &p.GetClusterMembersRequest{PageSize: 9, NextPageToken: nextPageToken})
		s.NoError(err)
		nextPageToken = resp.NextPageToken
		for _, member := range resp.ActiveMembers {
			expectedIds[primitives.UUIDString(member.HostID)]--
			hostCount++
		}

		if nextPageToken == nil {
			break
		}
	}

	s.Equal(100, hostCount)
	for id, val := range expectedIds {
		s.Zero(val, "identifier was either not found in db, or shouldn't be there - "+id)
	}

	time.Sleep(time.Second * 3)
	err = s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 1000})
	s.NoError(err)
}

func (s *ClusterMetadataManagerSuite) validateUpsert(req *p.UpsertClusterMembershipRequest, resp *p.GetClusterMembersResponse, err error) {
	s.Nil(err)
	s.NotNil(resp)
	s.NotEmpty(resp.ActiveMembers)
	s.Equal(len(resp.ActiveMembers), 1)
	// Have to round to 1 second due to SQL implementations. Cassandra truncates at 1ms.
	s.Equal(resp.ActiveMembers[0].SessionStart.Round(time.Second), req.SessionStart.Round(time.Second))
	s.Equal(resp.ActiveMembers[0].RPCAddress.String(), req.RPCAddress.String())
	s.Equal(resp.ActiveMembers[0].RPCPort, req.RPCPort)
	s.True(resp.ActiveMembers[0].RecordExpiry.After(time.Now().UTC()))
	s.Equal(resp.ActiveMembers[0].HostID, req.HostID)
	s.Equal(resp.ActiveMembers[0].Role, req.Role)
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
	s.Nil(err)

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

	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{RoleEquals: p.Matching},
	)

	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{SessionStartedAfter: time.Now().UTC()},
	)

	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{SessionStartedAfter: now.Add(-time.Minute), RPCAddressEquals: req.RPCAddress, HostIDEquals: req.HostID},
	)

	s.validateUpsert(req, resp, err)

	time.Sleep(time.Second * 3)
	err = s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 1000})
	s.NoError(err)
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
	s.NoError(err)

	err = s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	s.NoError(err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10, HostIDEquals: req.HostID},
	)

	s.NoError(err)
	s.NotNil(resp)
	s.NotEmpty(resp.ActiveMembers)
	s.Equal(len(resp.ActiveMembers), 1)
	// Have to round to 1 second due to SQL implementations. Cassandra truncates at 1ms.
	s.Equal(resp.ActiveMembers[0].SessionStart.Round(time.Second), req.SessionStart.Round(time.Second))
	s.Equal(resp.ActiveMembers[0].RPCAddress.String(), req.RPCAddress.String())
	s.Equal(resp.ActiveMembers[0].RPCPort, req.RPCPort)
	s.True(resp.ActiveMembers[0].RecordExpiry.After(time.Now().UTC()))
	s.Equal(resp.ActiveMembers[0].HostID, req.HostID)
	s.Equal(resp.ActiveMembers[0].Role, req.Role)

	time.Sleep(time.Second * 2)

	err = s.ClusterMetadataManager.PruneClusterMembership(s.ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	s.Nil(err)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		s.ctx,
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10},
	)

	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)
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
	s.NotNil(err)
	s.IsType(err, p.ErrInvalidMembershipExpiry)
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
	failoverVersionIncrement := int64(10)
	initialFailoverVersion := int64(1)

	// Case 1 - Get, mo data persisted
	// Fetch the persisted values, there should be nothing on start.
	// This doesn't error on no row found, but returns an empty record.
	getResp, err := s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Nil(getResp)

	// Case 2 - Init, no data persisted yet
	// First commit, this should be persisted
	initialResp, err := s.ClusterMetadataManager.SaveClusterMetadata(
		s.ctx,
		&p.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterNameToPersist,
				HistoryShardCount:        historyShardsToPersist,
				ClusterId:                clusterIdToPersist,
				ClusterAddress:           clusterAddress,
				FailoverVersionIncrement: failoverVersionIncrement,
				InitialFailoverVersion:   initialFailoverVersion,
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			}})

	s.Nil(err)
	s.True(initialResp) // request should be applied as this is first initialize

	// Case 3 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	s.Nil(err)
	s.True(getResp != nil)
	s.Equal(clusterNameToPersist, getResp.ClusterName)
	s.Equal(historyShardsToPersist, getResp.HistoryShardCount)
	s.Equal(clusterIdToPersist, getResp.ClusterId)
	s.Equal(clusterAddress, getResp.ClusterAddress)
	s.Equal(failoverVersionIncrement, getResp.FailoverVersionIncrement)
	s.Equal(initialFailoverVersion, getResp.InitialFailoverVersion)
	s.True(getResp.IsGlobalNamespaceEnabled)
	s.True(getResp.IsConnectionEnabled)

	// Case 4 - Init, data persisted
	// Attempt to overwrite with new values
	secondResp, err := s.ClusterMetadataManager.SaveClusterMetadata(s.ctx, &p.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:       clusterNameToPersist,
			HistoryShardCount: int32(77),
		}})

	s.Nil(err)
	s.False(secondResp) // Should not have applied, and should match values from first request

	// Refetch persisted
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initial values
	s.Nil(err)
	s.NotNil(getResp)
	s.Equal(clusterNameToPersist, getResp.ClusterName)
	s.Equal(historyShardsToPersist, getResp.HistoryShardCount)
	s.Equal(clusterIdToPersist, getResp.ClusterId)
	s.Equal(clusterAddress, getResp.ClusterAddress)
	s.Equal(failoverVersionIncrement, getResp.FailoverVersionIncrement)
	s.Equal(initialFailoverVersion, getResp.InitialFailoverVersion)
	s.True(getResp.IsGlobalNamespaceEnabled)
	s.True(getResp.IsConnectionEnabled)

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
	s.Nil(err)
	s.True(thirdResp)
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})
	s.Nil(err)
	s.NotNil(getResp)
	s.Equal("1.0", getResp.ClusterMetadata.VersionInfo.Current.Version)

	// Case 6 - Delete Cluster Metadata
	err = s.ClusterMetadataManager.DeleteClusterMetadata(s.ctx, &p.DeleteClusterMetadataRequest{ClusterName: clusterNameToPersist})
	s.Nil(err)
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Nil(getResp)

	// Case 7 - Update current cluster metadata
	clusterNameToPersist = "active"
	initialResp, err = s.ClusterMetadataManager.SaveClusterMetadata(
		s.ctx,
		&p.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterNameToPersist,
				HistoryShardCount:        historyShardsToPersist,
				ClusterId:                clusterIdToPersist,
				ClusterAddress:           clusterAddress,
				FailoverVersionIncrement: failoverVersionIncrement,
				InitialFailoverVersion:   initialFailoverVersion,
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			}})
	s.Nil(err)
	s.True(initialResp)

	// Case 8 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})

	// Validate they match our initializations
	s.Nil(err)
	s.True(getResp != nil)
	s.Equal(clusterNameToPersist, getResp.ClusterName)
	s.Equal(historyShardsToPersist, getResp.HistoryShardCount)
	s.Equal(clusterIdToPersist, getResp.ClusterId)
	s.Equal(clusterAddress, getResp.ClusterAddress)
	s.Equal(failoverVersionIncrement, getResp.FailoverVersionIncrement)
	s.Equal(initialFailoverVersion, getResp.InitialFailoverVersion)
	s.True(getResp.IsGlobalNamespaceEnabled)
	s.True(getResp.IsConnectionEnabled)

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
	s.True(applied)
	s.NoError(err)

	// Case 10 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetClusterMetadata(s.ctx, &p.GetClusterMetadataRequest{ClusterName: clusterNameToPersist})
	s.NoError(err)
	s.Equal("2.0", getResp.ClusterMetadata.VersionInfo.Current.Version)

	// Case 11 - List
	_, err = s.ClusterMetadataManager.SaveClusterMetadata(
		s.ctx,
		&p.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterNameToPersist + "2",
				HistoryShardCount:        historyShardsToPersist,
				ClusterId:                clusterIdToPersist,
				ClusterAddress:           clusterAddress,
				FailoverVersionIncrement: failoverVersionIncrement,
				InitialFailoverVersion:   initialFailoverVersion,
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			}})
	s.NoError(err)

	resp, err := s.ClusterMetadataManager.ListClusterMetadata(s.ctx, &p.ListClusterMetadataRequest{PageSize: 1})
	s.NoError(err)
	s.Equal(1, len(resp.ClusterMetadata))
	resp, err = s.ClusterMetadataManager.ListClusterMetadata(s.ctx, &p.ListClusterMetadataRequest{PageSize: 1, NextPageToken: resp.NextPageToken})
	s.NoError(err)
	s.Equal(1, len(resp.ClusterMetadata))
}
