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

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

const (
	// below are templates for history_node table
	addHistoryNodesQry = `INSERT INTO history_node (` +
		`shard_id, tree_id, branch_id, node_id, txn_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :node_id, :txn_id, :data, :data_encoding) `

	getHistoryNodesQry = `SELECT node_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? and node_id < ? ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT ? `

	deleteHistoryNodesQry = `DELETE FROM history_node WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? `

	// below are templates for history_tree table
	addHistoryTreeQry = `INSERT INTO history_tree (` +
		`shard_id, tree_id, branch_id, in_progress, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :in_progress, :data, :data_encoding) `

	getHistoryTreeQry = `SELECT branch_id, in_progress, data, data_encoding FROM history_tree WHERE shard_id = ? AND tree_id = ? `

	deleteHistoryTreeQry = `DELETE FROM history_tree WHERE shard_id = ? AND tree_id = ? AND branch_id = ? `

	updateHistoryTreeQry = `UPDATE history_tree set in_progress = :in_progress WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id `
)

// For history_node table:

// InsertIntoHistoryNode inserts a row into history_node table
func (mdb *DB) InsertIntoHistoryNode(row *sqldb.HistoryNodeRow) (sql.Result, error) {
	// NOTE: MySQL 5.6 doesn't support clustering order, to workaround, we let txn_id multiple by -1
	*row.TxnID *= -1
	return mdb.conn.NamedExec(addHistoryNodesQry, row)
}

// SelectFromHistoryNode reads one or more rows from history_node table
func (mdb *DB) SelectFromHistoryNode(filter *sqldb.HistoryNodeFilter) ([]sqldb.HistoryNodeRow, error) {
	var rows []sqldb.HistoryNodeRow
	err := mdb.conn.Select(&rows, getHistoryNodesQry,
		filter.ShardID, filter.TreeID, filter.BranchID, *filter.MinNodeID, *filter.MaxNodeID, *filter.PageSize)
	// NOTE: since we let txn_id multiple by -1 when inserting, we have to revert it back here
	for _, row := range rows {
		*row.TxnID *= -1
	}
	return rows, err
}

// DeleteFromHistoryNode deletes one or more rows from history_node table
func (mdb *DB) DeleteFromHistoryNode(filter *sqldb.HistoryNodeFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteHistoryNodesQry, filter.ShardID, filter.TreeID, filter.BranchID, *filter.MinNodeID)
}

// For history_tree table:

// InsertIntoHistoryTree inserts a row into history_tree table
func (mdb *DB) InsertIntoHistoryTree(row *sqldb.HistoryTreeRow) (sql.Result, error) {
	return mdb.conn.NamedExec(addHistoryTreeQry, row)
}

// SelectFromHistoryTree reads one or more rows from history_tree table
func (mdb *DB) SelectFromHistoryTree(filter *sqldb.HistoryTreeFilter) ([]sqldb.HistoryTreeRow, error) {
	var rows []sqldb.HistoryTreeRow
	err := mdb.conn.Select(&rows, getHistoryTreeQry, filter.ShardID, filter.TreeID)
	return rows, err
}

// UpdateHistoryTree updates a row in history_tree table
func (mdb *DB) UpdateHistoryTree(row *sqldb.HistoryTreeRow) (sql.Result, error) {
	return mdb.conn.NamedExec(updateHistoryTreeQry, row)
}

// DeleteFromHistoryTree deletes one or more rows from history_tree table
func (mdb *DB) DeleteFromHistoryTree(filter *sqldb.HistoryTreeFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteHistoryTreeQry, filter.ShardID, filter.TreeID, *filter.BranchID)
}
