package tests

import (
	"cmp"
	"math"
	"math/rand"
	"slices"
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

func NewHistoryNodeSuite(
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
	for range numNodeIDs {
		for range nodePerNodeID {
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

func (s *historyNodeSuite) TestInsertSelectReverse_Multiple() {
	numNodeIDs := 20
	nodePerNodeID := 3
	// Not a multiple of nodePerNodeID, so page boundaries fall inside a node ID.
	pageSize := 2

	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	minNodeID := int64(1)
	maxNodeID := minNodeID + int64(numNodeIDs)

	var nodes []sqlplugin.HistoryNodeRow
	// maxNodeID is exclusive, so its rows are inserted but must not be returned.
	for nodeID := minNodeID; nodeID <= maxNodeID; nodeID++ {
		for txnID := int64(1); txnID <= int64(nodePerNodeID); txnID++ {
			node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, txnID-1, txnID)
			_, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
			s.NoError(err)
			if nodeID == maxNodeID {
				continue
			}
			// NOTE: TxnID is *= -1 within InsertIntoHistoryNode
			node.TxnID = -node.TxnID
			nodes = append(nodes, node)
		}
	}
	// Reverse order sorts the stored (negated) transaction ID descending, so the read-back one ascends.
	slices.SortFunc(nodes, func(a, b sqlplugin.HistoryNodeRow) int {
		return cmp.Or(cmp.Compare(b.NodeID, a.NodeID), cmp.Compare(a.TxnID, b.TxnID))
	})

	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		MinNodeID:    minNodeID,
		MaxNodeID:    maxNodeID - 1,
		MaxTxnID:     sql.MaxTxnID,
		PageSize:     pageSize,
		ReverseOrder: true,
	}
	var rows []sqlplugin.HistoryNodeRow
	for {
		rowsPerPage, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
		s.NoError(err)
		rows = append(rows, rowsPerPage...)
		if len(rowsPerPage) < pageSize {
			break
		}
		lastNode := rowsPerPage[len(rowsPerPage)-1]
		selectFilter.MaxNodeID = lastNode.NodeID
		selectFilter.MaxTxnID = lastNode.TxnID
	}

	nodeAndTxnIDs := func(rows []sqlplugin.HistoryNodeRow) [][2]int64 {
		ids := make([][2]int64, len(rows))
		for index, row := range rows {
			ids[index] = [2]int64{row.NodeID, row.TxnID}
		}
		return ids
	}
	s.Equal(nodeAndTxnIDs(nodes), nodeAndTxnIDs(rows))
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

	for range numNodeIDs {
		for range nodePerNodeID {
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
