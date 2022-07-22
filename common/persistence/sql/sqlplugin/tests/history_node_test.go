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

package tests

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyNodeSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryNode
	}
)

const (
	testHistoryNodeEncoding = "random encoding"
)

var (
	testHistoryNodeData = []byte("random history node data")
)

func newHistoryNodeSuite(
	t *testing.T,
	store sqlplugin.HistoryNode,
) *historyNodeSuite {
	return &historyNodeSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyNodeSuite) SetupSuite() {

}

func (s *historyNodeSuite) TearDownSuite() {

}

func (s *historyNodeSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyNodeSuite) TearDownTest() {

}

func (s *historyNodeSuite) TestInsert_Success() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := rand.Int63()
	prevTransactionID := rand.Int63()
	transactionID := rand.Int63()

	node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, prevTransactionID, transactionID)
	result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyNodeSuite) TestInsert_Fail_Duplicate() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := rand.Int63()
	prevTransactionID := rand.Int63()
	transactionID := rand.Int63()

	node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, prevTransactionID, transactionID)
	result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	node = s.newRandomNodeRow(shardID, treeID, branchID, nodeID, prevTransactionID, transactionID)
	_, err = s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
	s.NoError(err) // TODO persistence layer should do proper error translation
}

func (s *historyNodeSuite) TestInsertSelect_Single() {
	pageSize := 100

	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := int64(1)
	prevTransactionID := rand.Int63()
	transactionID := rand.Int63()

	node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, prevTransactionID, transactionID)
	result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: nodeID,
		MinTxnID:  sql.MinTxnID,
		MaxNodeID: math.MaxInt64,
		PageSize:  pageSize,
	}
	rows, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
	s.NoError(err)
	// NOTE: TxnID is *= -1 within InsertIntoHistoryNode
	node.TxnID = -node.TxnID
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
		rows[index].BranchID = branchID
	}
	s.Equal([]sqlplugin.HistoryNodeRow{node}, rows)
}

func (s *historyNodeSuite) TestInsertSelect_Multiple() {
	numNodeIDs := 100
	nodePerNodeID := 2 + rand.Intn(8)
	pageSize := 10 + rand.Intn(10)

	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	nodeID := int64(1)
	minNodeID := nodeID
	maxNodeID := minNodeID + int64(numNodeIDs)

	var nodes []sqlplugin.HistoryNodeRow
	for i := 0; i < numNodeIDs; i++ {
		for j := 0; j < nodePerNodeID; j++ {
			node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, rand.Int63(), rand.Int63())
			result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
			s.NoError(err)
			rowsAffected, err := result.RowsAffected()
			s.NoError(err)
			s.Equal(1, int(rowsAffected))
			nodes = append(nodes, node)
		}
		nodeID++
	}

	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: minNodeID,
		MinTxnID:  sql.MinTxnID,
		MaxNodeID: maxNodeID,
		PageSize:  pageSize,
	}
	var rows []sqlplugin.HistoryNodeRow
	for {
		rowsPerPage, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
		s.NoError(err)
		rows = append(rows, rowsPerPage...)

		if len(rowsPerPage) > 0 {
			lastNode := rowsPerPage[len(rowsPerPage)-1]
			selectFilter.MinNodeID = lastNode.NodeID
			selectFilter.MinTxnID = lastNode.TxnID
		} else {
			break
		}
	}

	// NOTE: TxnID is *= -1 within InsertIntoHistoryNode
	for index := range nodes {
		nodes[index].TxnID = -nodes[index].TxnID
	}
	sort.Slice(nodes, func(i, j int) bool {
		this := nodes[i]
		that := nodes[j]

		if this.NodeID < that.NodeID {
			return true
		} else if this.NodeID > that.NodeID {
			return false
		}

		// larger transaction ID means newer
		if this.TxnID < that.TxnID {
			return false
		} else if this.TxnID > that.TxnID {
			return true
		}

		// same
		return true
	})
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
		rows[index].BranchID = branchID
	}
	s.Equal(nodes, rows)
}

func (s *historyNodeSuite) TestDeleteSelect() {
	pageSize := 100

	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := int64(1)

	deleteFilter := sqlplugin.HistoryNodeDeleteFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: nodeID,
	}
	result, err := s.store.RangeDeleteFromHistoryNode(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: nodeID,
		MinTxnID:  sql.MinTxnID,
		MaxNodeID: math.MaxInt64,
		PageSize:  pageSize,
	}
	rows, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
		rows[index].BranchID = branchID
	}
	s.Equal([]sqlplugin.HistoryNodeRow(nil), rows)
}

func (s *historyNodeSuite) TestInsertDeleteSelect_Single() {
	pageSize := 100

	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := int64(1)
	prevTransactionID := rand.Int63()
	transactionID := rand.Int63()

	node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, prevTransactionID, transactionID)
	result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
	// transaction ID is *= -1 within InsertIntoHistoryNode
	node.TxnID = -node.TxnID

	result, err = s.store.DeleteFromHistoryNode(newExecutionContext(), &node)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: nodeID,
		MinTxnID:  sql.MinTxnID,
		MaxNodeID: math.MaxInt64,
		PageSize:  pageSize,
	}
	rows, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
		rows[index].BranchID = branchID
	}
	s.Equal([]sqlplugin.HistoryNodeRow(nil), rows)
}

func (s *historyNodeSuite) TestInsertDeleteSelect_Multiple() {
	numNodeIDs := 50
	nodePerNodeID := 2
	pageSize := 100

	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	nodeID := int64(1)
	minNodeID := nodeID

	for i := 0; i < numNodeIDs; i++ {
		for j := 0; j < nodePerNodeID; j++ {
			node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, rand.Int63(), rand.Int63())
			result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
			s.NoError(err)
			rowsAffected, err := result.RowsAffected()
			s.NoError(err)
			s.Equal(1, int(rowsAffected))
		}
		nodeID++
	}

	deleteFilter := sqlplugin.HistoryNodeDeleteFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: minNodeID,
	}
	result, err := s.store.RangeDeleteFromHistoryNode(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numNodeIDs*nodePerNodeID, int(rowsAffected))

	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:   shardID,
		TreeID:    treeID,
		BranchID:  branchID,
		MinNodeID: nodeID,
		MinTxnID:  sql.MinTxnID,
		MaxNodeID: math.MaxInt64,
		PageSize:  pageSize,
	}
	rows, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
		rows[index].BranchID = branchID
	}
	s.Equal([]sqlplugin.HistoryNodeRow(nil), rows)
}

func (s *historyNodeSuite) newRandomNodeRow(
	shardID int32,
	treeID primitives.UUID,
	branchID primitives.UUID,
	nodeID int64,
	prevTransactionID int64,
	transactionID int64,
) sqlplugin.HistoryNodeRow {
	return sqlplugin.HistoryNodeRow{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		NodeID:       nodeID,
		PrevTxnID:    prevTransactionID,
		TxnID:        transactionID,
		Data:         shuffle.Bytes(testHistoryNodeData),
		DataEncoding: testHistoryNodeEncoding,
	}
}
