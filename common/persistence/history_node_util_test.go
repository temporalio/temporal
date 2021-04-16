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

package persistence

import (
	"math/rand"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	historyNodeMetadataSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestHistoryNodeMetadataSuite(t *testing.T) {
	s := new(historyNodeMetadataSuite)
	suite.Run(t, s)
}

func (s *historyNodeMetadataSuite) SetupSuite() {
}

func (s *historyNodeMetadataSuite) TearDownSuite() {

}

func (s *historyNodeMetadataSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyNodeMetadataSuite) TearDownTest() {

}

func (s *historyNodeMetadataSuite) TestIndexNodeIDToNode() {
	branch := &persistencespb.HistoryBranch{
		TreeId:   uuid.New(),
		BranchId: uuid.New(),
	}
	numNodeIDs := 10
	nodePerNodeID := 10

	prevTransactionID := int64(0)
	transactionIDToNode := map[int64]historyNodeMetadata{}
	for nodeID := common.FirstEventID; nodeID < int64(numNodeIDs+1); nodeID++ {
		var nextTransactionID *int64
		for i := 0; i < nodePerNodeID; i++ {
			transactionID := rand.Int63()
			if nextTransactionID == nil || *nextTransactionID < transactionID {
				nextTransactionID = &transactionID
			}
			node := s.newRandomHistoryNodeMetadata(branch, nodeID, transactionID, prevTransactionID)
			transactionIDToNode[node.transactionID] = node
		}
		prevTransactionID = *nextTransactionID
	}

	nodeIDToNode := indexNodeIDToNode(transactionIDToNode)
	for nodeID := common.FirstEventID; nodeID < int64(numNodeIDs+1); nodeID++ {
		nodes := nodeIDToNode[int64(nodeID)]
		for i := 1; i < nodePerNodeID; i++ {
			s.True(nodes[i-1].transactionID >= nodes[i].transactionID)
		}
	}
}

func (s *historyNodeMetadataSuite) TestReverselyLinkNode() {
	branch := &persistencespb.HistoryBranch{
		TreeId:   uuid.New(),
		BranchId: uuid.New(),
	}
	numNodeIDs := 10
	nodePerNodeID := 10

	var expectedNodes []historyNodeMetadata
	prevTransactionID := int64(0)
	transactionIDToNode := map[int64]historyNodeMetadata{}
	for nodeID := common.FirstEventID; nodeID < int64(numNodeIDs+1); nodeID++ {
		var nextTransactionID *int64
		for i := 0; i < nodePerNodeID; i++ {
			transactionID := rand.Int63()
			if nextTransactionID == nil || *nextTransactionID < transactionID {
				nextTransactionID = &transactionID
			}
			node := s.newRandomHistoryNodeMetadata(branch, nodeID, transactionID, prevTransactionID)
			transactionIDToNode[node.transactionID] = node
		}
		prevTransactionID = *nextTransactionID
		expectedNodes = append([]historyNodeMetadata{transactionIDToNode[prevTransactionID]}, expectedNodes...)
	}
	lastValidNode := s.newRandomHistoryNodeMetadata(branch, int64(numNodeIDs+1), rand.Int63(), prevTransactionID)
	transactionIDToNode[lastValidNode.transactionID] = lastValidNode
	expectedNodes = append([]historyNodeMetadata{lastValidNode}, expectedNodes...)

	nodes, err := reverselyLinkNode(lastValidNode.nodeID, lastValidNode.transactionID, transactionIDToNode)
	s.NoError(err)
	s.Equal(expectedNodes, nodes)
}

func (s *historyNodeMetadataSuite) TestTrimNodes() {
	branch := &persistencespb.HistoryBranch{
		TreeId:   uuid.New(),
		BranchId: uuid.New(),
	}

	node1Valid := s.newRandomHistoryNodeMetadata(branch, 1, rand.Int63(), 0)
	node1Stale0 := s.newRandomHistoryNodeMetadata(branch, 1, node1Valid.transactionID-11, 0)
	node1Stale1 := s.newRandomHistoryNodeMetadata(branch, 1, node1Valid.transactionID-22, 0)
	node1Trim0 := s.newRandomHistoryNodeMetadata(branch, 1, node1Valid.transactionID+33, 0)
	node1Trim1 := s.newRandomHistoryNodeMetadata(branch, 1, node1Valid.transactionID+44, 0)
	// reverse sort by transaction ID
	node1s := []historyNodeMetadata{node1Trim1, node1Trim0, node1Valid, node1Stale0, node1Stale1}

	node2Valid := s.newRandomHistoryNodeMetadata(branch, 2, rand.Int63(), 0)
	// reverse sort by transaction ID
	node2s := []historyNodeMetadata{node2Valid}

	node3Valid := s.newRandomHistoryNodeMetadata(branch, 3, rand.Int63(), 0)
	node3Stale0 := s.newRandomHistoryNodeMetadata(branch, 3, node3Valid.transactionID-100, 0)
	node3Stale1 := s.newRandomHistoryNodeMetadata(branch, 3, node3Valid.transactionID-200, 0)
	// reverse sort by transaction ID
	node3s := []historyNodeMetadata{node3Valid, node3Stale0, node3Stale1}

	node4Valid := s.newRandomHistoryNodeMetadata(branch, 4, rand.Int63(), 0)
	node4Trim0 := s.newRandomHistoryNodeMetadata(branch, 4, node4Valid.transactionID+1024, 0)
	node4Trim1 := s.newRandomHistoryNodeMetadata(branch, 4, node4Valid.transactionID+2048, 0)
	// reverse sort by transaction ID
	node4s := []historyNodeMetadata{node4Trim1, node4Trim0, node4Valid}

	nodeIDToNodes := map[int64][]historyNodeMetadata{
		1: node1s,
		2: node2s,
		3: node3s,
		4: node4s,
	}

	nodesToTrim := trimNodes(nodeIDToNodes, []historyNodeMetadata{node4Valid, node3Valid, node2Valid, node1Valid})
	s.Equal([]historyNodeMetadata{node4Trim1, node4Trim0, node1Trim1, node1Trim0}, nodesToTrim)
}

func (s *historyNodeMetadataSuite) newRandomHistoryNodeMetadata(
	branch *persistencespb.HistoryBranch,
	nodeID int64,
	transactionID int64,
	prevTransactionID int64,
) historyNodeMetadata {
	return historyNodeMetadata{
		branchInfo:        branch,
		nodeID:            nodeID,
		transactionID:     transactionID,
		prevTransactionID: prevTransactionID,
	}
}
