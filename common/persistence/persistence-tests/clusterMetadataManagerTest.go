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

	"github.com/temporalio/temporal/common"

	persist "github.com/temporalio/temporal/.gen/go/persistence"
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
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *ClusterMetadataManagerSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
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
	s.Nil(err)
	s.NotNil(getResp)
	s.Nil(getResp.ClusterName)
	s.Nil(getResp.HistoryShardCount)

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
