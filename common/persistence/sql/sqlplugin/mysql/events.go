// Copyright (c) 2017 Uber Technologies, Inc.
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

package mysql

import (
	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	// below are templates for history_node table
	addHistoryNodesQuery = `INSERT INTO history_node (` +
		`shard_id, tree_id, branch_id, node_id, txn_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :node_id, :txn_id, :data, :data_encoding) `

	getHistoryNodesQuery = `SELECT node_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? and node_id < ? ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT ? `

	deleteHistoryNodesQuery = `DELETE FROM history_node WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? `

	// below are templates for history_tree table
	addHistoryTreeQuery = `INSERT INTO history_tree (` +
		`shard_id, tree_id, branch_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :data, :data_encoding) `

	getHistoryTreeQuery = `SELECT branch_id, data, data_encoding FROM history_tree WHERE shard_id = ? AND tree_id = ? `

	deleteHistoryTreeQuery = `DELETE FROM history_tree WHERE shard_id = ? AND tree_id = ? AND branch_id = ? `
)

// For history_node table:

// InsertIntoHistoryNode inserts a row into history_node table
func (mdb *db) InsertIntoHistoryNode(row *sqlplugin.HistoryNodeRow) (sql.Result, error) {
	// NOTE: Query 5.6 doesn't support clustering order, to workaround, we let txn_id multiple by -1
	*row.TxnID *= -1
	return mdb.conn.NamedExec(addHistoryNodesQuery, row)
}

// SelectFromHistoryNode reads one or more rows from history_node table
func (mdb *db) SelectFromHistoryNode(filter *sqlplugin.HistoryNodeFilter) ([]sqlplugin.HistoryNodeRow, error) {
	var rows []sqlplugin.HistoryNodeRow
	err := mdb.conn.Select(&rows, getHistoryNodesQuery,
		filter.ShardID, filter.TreeID, filter.BranchID, *filter.MinNodeID, *filter.MaxNodeID, *filter.PageSize)
	// NOTE: since we let txn_id multiple by -1 when inserting, we have to revert it back here
	for _, row := range rows {
		*row.TxnID *= -1
	}
	return rows, err
}

// DeleteFromHistoryNode deletes one or more rows from history_node table
func (mdb *db) DeleteFromHistoryNode(filter *sqlplugin.HistoryNodeFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteHistoryNodesQuery, filter.ShardID, filter.TreeID, filter.BranchID, *filter.MinNodeID)
}

// For history_tree table:

// InsertIntoHistoryTree inserts a row into history_tree table
func (mdb *db) InsertIntoHistoryTree(row *sqlplugin.HistoryTreeRow) (sql.Result, error) {
	return mdb.conn.NamedExec(addHistoryTreeQuery, row)
}

// SelectFromHistoryTree reads one or more rows from history_tree table
func (mdb *db) SelectFromHistoryTree(filter *sqlplugin.HistoryTreeFilter) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := mdb.conn.Select(&rows, getHistoryTreeQuery, filter.ShardID, filter.TreeID)
	return rows, err
}

// DeleteFromHistoryTree deletes one or more rows from history_tree table
func (mdb *db) DeleteFromHistoryTree(filter *sqlplugin.HistoryTreeFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteHistoryTreeQuery, filter.ShardID, filter.TreeID, *filter.BranchID)
}
