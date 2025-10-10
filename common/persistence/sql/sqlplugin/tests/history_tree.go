package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyTreeSuite struct {
		suite.Suite

		store sqlplugin.HistoryTree
	}
)

const (
	testHistoryTreeEncoding = "random encoding"
)

var (
	testHistoryTreeData = []byte("random history tree data")
)

func NewHistoryTreeSuite(
	t *testing.T,
	store sqlplugin.HistoryTree,
) *historyTreeSuite {
	return &historyTreeSuite{

		store: store,
	}
}



func (s *historyTreeSuite) TearDownSuite() {

}



func (s *historyTreeSuite) TearDownTest() {

}

func (s *historyTreeSuite) TestInsert_Success() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	node := s.newRandomTreeRow(shardID, treeID, branchID)
	result, err := s.store.InsertIntoHistoryTree(newExecutionContext(), &node)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyTreeSuite) TestInsert_Duplicate_Success() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	node := s.newRandomTreeRow(shardID, treeID, branchID)
	result, err := s.store.InsertIntoHistoryTree(newExecutionContext(), &node)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	node = s.newRandomTreeRow(shardID, treeID, branchID)
	result, err = s.store.InsertIntoHistoryTree(newExecutionContext(), &node)
	require.NoError(s.T(), err)
	_, err = result.RowsAffected()
	require.NoError(s.T(), err)
	// TODO cannot assert on the number of rows affect
	//  since MySQL and PostgreSQL have different behavior
}

func (s *historyTreeSuite) TestInsertSelect() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	tree := s.newRandomTreeRow(shardID, treeID, branchID)
	result, err := s.store.InsertIntoHistoryTree(newExecutionContext(), &tree)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.HistoryTreeSelectFilter{
		ShardID: shardID,
		TreeID:  treeID,
	}
	rows, err := s.store.SelectFromHistoryTree(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
	}
	require.Equal(s.T(), []sqlplugin.HistoryTreeRow{tree}, rows)
}

func (s *historyTreeSuite) TestDeleteSelect() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	deleteFilter := sqlplugin.HistoryTreeDeleteFilter{
		ShardID:  shardID,
		TreeID:   treeID,
		BranchID: branchID,
	}
	result, err := s.store.DeleteFromHistoryTree(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.HistoryTreeSelectFilter{
		ShardID: shardID,
		TreeID:  treeID,
	}
	rows, err := s.store.SelectFromHistoryTree(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
	}
	require.Equal(s.T(), []sqlplugin.HistoryTreeRow(nil), rows)
}

func (s *historyTreeSuite) TestInsertDeleteSelect() {
	shardID := rand.Int31()
	treeID := primitives.NewUUID()
	branchID := primitives.NewUUID()

	tree := s.newRandomTreeRow(shardID, treeID, branchID)
	result, err := s.store.InsertIntoHistoryTree(newExecutionContext(), &tree)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	deleteFilter := sqlplugin.HistoryTreeDeleteFilter{
		ShardID:  shardID,
		TreeID:   treeID,
		BranchID: branchID,
	}
	result, err = s.store.DeleteFromHistoryTree(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.HistoryTreeSelectFilter{
		ShardID: shardID,
		TreeID:  treeID,
	}
	rows, err := s.store.SelectFromHistoryTree(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].TreeID = treeID
	}
	require.Equal(s.T(), []sqlplugin.HistoryTreeRow(nil), rows)
}

func (s *historyTreeSuite) newRandomTreeRow(
	shardID int32,
	treeID primitives.UUID,
	branchID primitives.UUID,
) sqlplugin.HistoryTreeRow {
	return sqlplugin.HistoryTreeRow{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		Data:         shuffle.Bytes(testHistoryTreeData),
		DataEncoding: testHistoryTreeEncoding,
	}
}
