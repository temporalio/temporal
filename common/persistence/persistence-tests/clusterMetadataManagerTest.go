// Copyright (c) 2020 Temporal Technologies, Inc.
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
	"os"
	"testing"
	"time"

	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"

	persist "github.com/temporalio/temporal/.gen/go/persistenceblobs"
	p "github.com/temporalio/temporal/common/persistence"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type (
	// ClusterMetadataManagerSuite runs tests that cover the ClusterMetadata read/write scenarios
	ClusterMetadataManagerSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (s *ClusterMetadataManagerSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (s *ClusterMetadataManagerSuite) SetupTest() {
	s.ClusterMetadataManager.PruneClusterMembership(&p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *ClusterMetadataManagerSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// TestClusterMembershipEmptyInitially verifies the GetClusterMembers() works with an initial empty table
func (s *ClusterMetadataManagerSuite) TestClusterMembershipEmptyInitially() {
	resp, err := s.ClusterMetadataManager.GetClusterMembers(&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10})
	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)
}

// TestClusterMembershipUpsertCanRead verifies that we can UpsertClusterMembership and read our result
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertCanRead() {
	req := &p.UpsertClusterMembershipRequest{
		HostID:         []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		RPCAddress:     "127.0.0.2",
		SessionStarted: time.Now().UTC(),
		RecordExpiry:   time.Second,
	}
	err := s.ClusterMetadataManager.UpsertClusterMembership(req)
	s.Nil(err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10})

	s.Nil(err)
	s.NotNil(resp)
	s.NotEmpty(resp.ActiveMembers)
	s.Equal(len(resp.ActiveMembers), 1)
	// Have to round to 1 second due to SQL implementations. Cassandra truncates at 1ms.
	s.Equal(resp.ActiveMembers[0].SessionStarted.Round(time.Second), req.SessionStarted.Round(time.Second))
	s.Equal(resp.ActiveMembers[0].RPCAddress, req.RPCAddress)
	s.Equal(resp.ActiveMembers[0].HostID, req.HostID)
}

// TestClusterMembershipUpsertExpiresCorrectly verifies RecordExpiry functions properly for ClusterMembership records
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertExpiresCorrectly() {
	req := &p.UpsertClusterMembershipRequest{
		HostID:         []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		RPCAddress:     "127.0.0.2",
		SessionStarted: time.Now().UTC(),
		RecordExpiry:   time.Second,
	}
	err := s.ClusterMetadataManager.UpsertClusterMembership(req)
	s.NoError(err)

	err = s.ClusterMetadataManager.PruneClusterMembership(&p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	s.NoError(err)

	resp, err := s.ClusterMetadataManager.GetClusterMembers(
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10})

	s.NoError(err)
	s.NotNil(resp)
	s.NotEmpty(resp.ActiveMembers)
	s.Equal(len(resp.ActiveMembers), 1)
	// Have to round to 1 second due to SQL implementations. Cassandra truncates at 1ms.
	s.Equal(resp.ActiveMembers[0].SessionStarted.Round(time.Second), req.SessionStarted.Round(time.Second))
	s.Equal(resp.ActiveMembers[0].RPCAddress, req.RPCAddress)
	s.Equal(resp.ActiveMembers[0].HostID, req.HostID)

	time.Sleep(time.Second * 2)

	err = s.ClusterMetadataManager.PruneClusterMembership(&p.PruneClusterMembershipRequest{MaxRecordsPruned: 100})
	s.Nil(err)

	resp, err = s.ClusterMetadataManager.GetClusterMembers(
		&p.GetClusterMembersRequest{LastHeartbeatWithin: time.Minute * 10})

	s.Nil(err)
	s.NotNil(resp)
	s.Empty(resp.ActiveMembers)
}

// TestClusterMembershipUpsertInvalidExpiry verifies we cannot specify a non-positive RecordExpiry duration
func (s *ClusterMetadataManagerSuite) TestClusterMembershipUpsertInvalidExpiry() {
	req := &p.UpsertClusterMembershipRequest{
		HostID:         []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		RPCAddress:     "127.0.0.2",
		SessionStarted: time.Now().UTC(),
		RecordExpiry:   time.Second * 0,
	}
	err := s.ClusterMetadataManager.UpsertClusterMembership(req)
	s.NotNil(err)
	s.IsType(err, p.ErrInvalidMembershipExpiry)
}

// TestInitImmutableMetadataReadWrite runs through the various cases of ClusterMetadata behavior
// Cases:
// 1 - Get, no data persisted
// 2 - Init, no data persisted
// 3 - Get, data persisted
// 4 - Init, data persisted
func (s *ClusterMetadataManagerSuite) TestInitImmutableMetadataReadWrite() {
	// Case 1 - Get, mo data persisted
	// Fetch the persisted values, there should be nothing on start.
	// This doesn't error on no row found, but returns an empty record.
	getResp, err := s.ClusterMetadataManager.GetImmutableClusterMetadata()

	// Validate they match our initializations
	s.NotNil(err)
	s.IsType(&shared.EntityNotExistsError{}, err)
	s.Nil(getResp)

	var clusterNameToPersist = "testing"
	var historyShardsToPersist = 43

	// Case 2 - Init, no data persisted yet
	// First commit, this should be persisted
	initialResp, err := s.ClusterMetadataManager.InitializeImmutableClusterMetadata(
		&p.InitializeImmutableClusterMetadataRequest{
			ImmutableClusterMetadata: persist.ImmutableClusterMetadata{
				ClusterName:       &clusterNameToPersist,
				HistoryShardCount: common.Int32Ptr(int32(historyShardsToPersist)),
			}})

	s.Nil(err)
	s.NotNil(initialResp)
	s.NotNil(initialResp.PersistedImmutableData)
	s.True(initialResp.RequestApplied) // request should be applied as this is first initialize
	s.True(*initialResp.PersistedImmutableData.ClusterName == clusterNameToPersist)
	s.True(int(*initialResp.PersistedImmutableData.HistoryShardCount) == historyShardsToPersist)

	// Case 3 - Get, data persisted
	// Fetch the persisted values
	getResp, err = s.ClusterMetadataManager.GetImmutableClusterMetadata()

	// Validate they match our initializations
	s.Nil(err)
	s.True(getResp != nil)
	s.True(*getResp.ClusterName == clusterNameToPersist)
	s.True(int(*getResp.HistoryShardCount) == historyShardsToPersist)

	// Case 4 - Init, data persisted
	// Attempt to overwrite with new values
	var wrongClusterName string = "overWriteClusterName"
	secondResp, err := s.ClusterMetadataManager.InitializeImmutableClusterMetadata(&p.InitializeImmutableClusterMetadataRequest{
		ImmutableClusterMetadata: persist.ImmutableClusterMetadata{
			ClusterName:       &wrongClusterName,
			HistoryShardCount: common.Int32Ptr(int32(77)),
		}})

	s.Nil(err)
	s.NotNil(secondResp)
	s.NotNil(secondResp.PersistedImmutableData)
	s.False(secondResp.RequestApplied) // Should not have applied, and should match values from first request
	s.True(*secondResp.PersistedImmutableData.ClusterName == clusterNameToPersist)
	s.True(int(*secondResp.PersistedImmutableData.HistoryShardCount) == historyShardsToPersist)

	// Refetch persisted
	getResp, err = s.ClusterMetadataManager.GetImmutableClusterMetadata()

	// Validate they match our initial values
	s.Nil(err)
	s.True(getResp != nil)
	s.True(*getResp.ClusterName == clusterNameToPersist)
	s.True(int(*getResp.HistoryShardCount) == historyShardsToPersist)
}
