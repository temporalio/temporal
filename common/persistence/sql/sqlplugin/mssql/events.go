package mssql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// below are templates for history_node table
	addHistoryNodesQuery = `MERGE history_node WITH (HOLDLOCK) AS target ` +
		`USING (SELECT :shard_id AS shard_id, :tree_id AS tree_id, :branch_id AS branch_id, :node_id AS node_id, ` +
		`:prev_txn_id AS prev_txn_id, :txn_id AS txn_id, :data AS data, :data_encoding AS data_encoding) AS source ` +
		`ON target.shard_id = source.shard_id AND target.tree_id = source.tree_id AND target.branch_id = source.branch_id ` +
		`AND target.node_id = source.node_id AND target.txn_id = source.txn_id ` +
		`WHEN MATCHED THEN UPDATE SET prev_txn_id = source.prev_txn_id, data = source.data, data_encoding = source.data_encoding ` +
		`WHEN NOT MATCHED THEN INSERT (shard_id, tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (source.shard_id, source.tree_id, source.branch_id, source.node_id, source.prev_txn_id, source.txn_id, source.data, source.data_encoding);`

	getHistoryNodesQuery = `SELECT TOP (?) node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND ((node_id = ? AND txn_id > ?) OR node_id > ?) AND node_id < ? ` +
		`ORDER BY shard_id, tree_id, branch_id, node_id, txn_id `

	getHistoryNodesReverseQuery = `SELECT TOP (?) node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? AND ((node_id = ? AND txn_id < ?) OR node_id < ?) ` +
		`ORDER BY shard_id, tree_id, branch_id DESC, node_id DESC, txn_id DESC `

	getHistoryNodeMetadataQuery = `SELECT TOP (?) node_id, prev_txn_id, txn_id FROM history_node ` +
		`WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND ((node_id = ? AND txn_id > ?) OR node_id > ?) AND node_id < ? ` +
		`ORDER BY shard_id, tree_id, branch_id, node_id, txn_id `

	deleteHistoryNodeQuery = `DELETE FROM history_node WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ? `

	deleteHistoryNodesQuery = `DELETE FROM history_node WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? `

	// below are templates for history_tree table
	addHistoryTreeQuery = `MERGE history_tree WITH (HOLDLOCK) AS target ` +
		`USING (SELECT :shard_id AS shard_id, :tree_id AS tree_id, :branch_id AS branch_id, :data AS data, :data_encoding AS data_encoding) AS source ` +
		`ON target.shard_id = source.shard_id AND target.tree_id = source.tree_id AND target.branch_id = source.branch_id ` +
		`WHEN MATCHED THEN UPDATE SET data = source.data, data_encoding = source.data_encoding ` +
		`WHEN NOT MATCHED THEN INSERT (shard_id, tree_id, branch_id, data, data_encoding) ` +
		`VALUES (source.shard_id, source.tree_id, source.branch_id, source.data, source.data_encoding);`

	getHistoryTreeQuery = `SELECT branch_id, data, data_encoding FROM history_tree WHERE shard_id = ? AND tree_id = ? `

	// conceptually this query is WHERE (shard_id, tree_id, branch_id) > (?, ?, ?)
	// but T-SQL does not support row-value comparisons, so it's spelled out
	paginateBranchesQuery = `SELECT TOP (?) shard_id, tree_id, branch_id, data, data_encoding
		FROM history_tree
		WHERE (shard_id = ? AND ((tree_id = ? AND branch_id > ?) OR tree_id > ?)) OR shard_id > ?
		ORDER BY shard_id, tree_id, branch_id`

	deleteHistoryTreeQuery = `DELETE FROM history_tree WHERE shard_id = ? AND tree_id = ? AND branch_id = ? `
)

// For history_node table:

// InsertIntoHistoryNode inserts a row into history_node table
func (mdb *db) InsertIntoHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	row.TxnID = -row.TxnID
	return mdb.NamedExecContext(ctx,
		addHistoryNodesQuery,
		row,
	)
}

// DeleteFromHistoryNode delete a row from history_node table
func (mdb *db) DeleteFromHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	row.TxnID = -row.TxnID
	return mdb.ExecContext(ctx,
		deleteHistoryNodeQuery,
		row.ShardID,
		row.TreeID,
		row.BranchID,
		row.NodeID,
		row.TxnID,
	)
}

// RangeSelectFromHistoryNode reads one or more rows from history_node table
func (mdb *db) RangeSelectFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeSelectFilter,
) ([]sqlplugin.HistoryNodeRow, error) {
	var query string
	if filter.MetadataOnly {
		query = getHistoryNodeMetadataQuery
	} else if filter.ReverseOrder {
		query = getHistoryNodesReverseQuery
	} else {
		query = getHistoryNodesQuery
	}

	// NOTE: TOP (?) places the page size placeholder first
	var args []any
	if filter.ReverseOrder {
		args = []any{
			filter.PageSize,
			filter.ShardID,
			filter.TreeID,
			filter.BranchID,
			filter.MinNodeID,
			filter.MaxTxnID,
			-filter.MaxTxnID,
			filter.MaxNodeID,
		}
	} else {
		args = []any{
			filter.PageSize,
			filter.ShardID,
			filter.TreeID,
			filter.BranchID,
			filter.MinNodeID,
			-filter.MinTxnID, // NOTE: transaction ID is *= -1 when stored
			filter.MinNodeID,
			filter.MaxNodeID,
		}
	}

	var rows []sqlplugin.HistoryNodeRow
	if err := mdb.SelectContext(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	// NOTE: since we let txn_id multiple by -1 when inserting, we have to revert it back here
	for index := range rows {
		rows[index].TxnID = -rows[index].TxnID
	}
	return rows, nil
}

// RangeDeleteFromHistoryNode deletes one or more rows from history_node table
func (mdb *db) RangeDeleteFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeDeleteFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		deleteHistoryNodesQuery,
		filter.ShardID,
		filter.TreeID,
		filter.BranchID,
		filter.MinNodeID,
	)
}

// For history_tree table:

// InsertIntoHistoryTree inserts a row into history_tree table
func (mdb *db) InsertIntoHistoryTree(
	ctx context.Context,
	row *sqlplugin.HistoryTreeRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		addHistoryTreeQuery,
		row,
	)
}

// SelectFromHistoryTree reads one or more rows from history_tree table
func (mdb *db) SelectFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeSelectFilter,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := mdb.SelectContext(ctx,
		&rows,
		getHistoryTreeQuery,
		filter.ShardID,
		filter.TreeID,
	)
	return rows, err
}

// PaginateBranchesFromHistoryTree reads up to page.Limit rows from the history_tree table sorted by their primary key,
// while skipping the first page.Offset rows.
func (mdb *db) PaginateBranchesFromHistoryTree(
	ctx context.Context,
	page sqlplugin.HistoryTreeBranchPage,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := mdb.SelectContext(
		ctx,
		&rows,
		paginateBranchesQuery,
		page.Limit,
		page.ShardID,
		page.TreeID,
		page.BranchID,
		page.TreeID,
		page.ShardID,
	)
	return rows, err
}

// DeleteFromHistoryTree deletes one or more rows from history_tree table
func (mdb *db) DeleteFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeDeleteFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		deleteHistoryTreeQuery,
		filter.ShardID,
		filter.TreeID,
		filter.BranchID,
	)
}
