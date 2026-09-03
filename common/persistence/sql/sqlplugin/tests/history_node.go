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

// TestInsertSelectReverseCursorBoundary verifies that the reverse cursor uses
// strict < semantics: the cursor row is excluded and all rows before it at the
// same node_id are included.
func (s *historyNodeSuite) TestInsertSelectReverseCursorBoundary() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := int64(10)

	// Insert 3 txns at the same node_id
	for txnID := int64(1); txnID <= 3; txnID++ {
		node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, rand.Int63(), txnID)
		result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
		s.NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.NoError(err)
		s.Equal(1, int(rowsAffected))
	}

	// Cursor at (nodeID=10, txnID=2).
	// Stored: txn_id -1, -2, -3. Query: (node_id, txn_id) < (10, -2)
	// -3 < -2 → included (logical txn=3). -2 < -2 → excluded. -1 < -2 → excluded.
	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		MinNodeID:    nodeID,
		MaxNodeID:    nodeID,
		MaxTxnID:     2,
		PageSize:     10,
		ReverseOrder: true,
	}
	rows, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
	s.NoError(err)
	// Only txn_id=3 (stored as -3, which is < -2)
	s.Len(rows, 1, "only rows before cursor should be returned")
	s.Equal(nodeID, rows[0].NodeID)
	s.Equal(int64(3), rows[0].TxnID) // negated back on read
}

// TestInsertSelectReverseMultiPageSameNode verifies that reverse pagination
// across multiple pages within a single node_id produces no duplicates or gaps,
// and that the page boundary correctly splits within the same node's txn_ids.
func (s *historyNodeSuite) TestInsertSelectReverseMultiPageSameNode() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()
	nodeID := int64(5)

	// Insert 5 txns at the same node_id
	for txnID := int64(1); txnID <= 5; txnID++ {
		node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, rand.Int63(), txnID)
		result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
		s.NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.NoError(err)
		s.Equal(1, int(rowsAffected))
	}

	// Page through in reverse with pageSize=2
	var allRows []sqlplugin.HistoryNodeRow
	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		MinNodeID:    nodeID,
		MaxNodeID:    nodeID,
		MaxTxnID:     sql.MaxTxnID,
		PageSize:     2,
		ReverseOrder: true,
	}
	for {
		page, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
		s.NoError(err)
		if len(page) == 0 {
			break
		}
		allRows = append(allRows, page...)
		lastRow := page[len(page)-1]
		selectFilter.MaxNodeID = lastRow.NodeID
		selectFilter.MaxTxnID = lastRow.TxnID
	}
	s.Len(allRows, 5, "reverse pagination must return all rows exactly once")
	s.assertUniqueAndExpectedKeys(allRows, nodeID, nodeID, 1, 5)
}

// TestInsertSelectReverseAcrossNodes verifies reverse pagination across
// multiple node_ids, each with multiple txn_ids. Earlier nodes must return
// ALL their txn_ids regardless of the cursor's txn_id value.
func (s *historyNodeSuite) TestInsertSelectReverseAcrossNodes() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	// Insert 3 txns at each of node_id 1, 2, 3 (9 rows total)
	totalInserted := 0
	for nodeID := int64(1); nodeID <= 3; nodeID++ {
		for txnID := int64(1); txnID <= 3; txnID++ {
			node := s.newRandomNodeRow(shardID, treeID, branchID, nodeID, rand.Int63(), txnID)
			result, err := s.store.InsertIntoHistoryNode(newExecutionContext(), &node)
			s.NoError(err)
			rowsAffected, err := result.RowsAffected()
			s.NoError(err)
			s.Equal(1, int(rowsAffected))
			totalInserted++
		}
	}
	s.Equal(9, totalInserted)

	// Reverse page through all with pageSize=2, starting from sentinel
	var allRows []sqlplugin.HistoryNodeRow
	selectFilter := sqlplugin.HistoryNodeSelectFilter{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		MinNodeID:    1,
		MaxNodeID:    3,
		MaxTxnID:     sql.MaxTxnID,
		PageSize:     2,
		ReverseOrder: true,
	}
	for {
		page, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), selectFilter)
		s.NoError(err)
		if len(page) == 0 {
			break
		}
		allRows = append(allRows, page...)
		lastRow := page[len(page)-1]
		selectFilter.MaxNodeID = lastRow.NodeID
		selectFilter.MaxTxnID = lastRow.TxnID
	}
	// Must get all 9 rows — no gaps, no duplicates
	s.Len(allRows, 9, "reverse pagination across nodes must return all rows")
	s.assertUniqueAndExpectedKeys(allRows, 1, 3, 1, 3)
}

// assertUniqueAndExpectedKeys checks that rows contain no duplicate (NodeID, TxnID)
// pairs and that their set equals the expected cartesian product of
// [minNode, maxNode] × [minTxn, maxTxn].
func (s *historyNodeSuite) assertUniqueAndExpectedKeys(
	rows []sqlplugin.HistoryNodeRow,
	minNode, maxNode, minTxn, maxTxn int64,
) {
	type key struct{ NodeID, TxnID int64 }

	got := make(map[key]struct{}, len(rows))
	for _, r := range rows {
		k := key{r.NodeID, r.TxnID}
		_, dup := got[k]
		s.False(dup, "duplicate row: NodeID=%d TxnID=%d", r.NodeID, r.TxnID)
		got[k] = struct{}{}
	}

	expected := make(map[key]struct{})
	for n := minNode; n <= maxNode; n++ {
		for t := minTxn; t <= maxTxn; t++ {
			expected[key{n, t}] = struct{}{}
		}
	}
	s.Equal(expected, got, "returned key set must match expected set")
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
