package mysql

import (
	"database/sql"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const (
	createShardQry = `INSERT INTO
 shards (shard_id, range_id, data, data_encoding) VALUES (?, ?, ?, ?)`

	getShardQry = `SELECT
 shard_id, range_id, data, data_encoding
 FROM shards WHERE shard_id = ?`

	updateShardQry = `UPDATE shards 
 SET range_id = ?, data = ?, data_encoding = ? 
 WHERE shard_id = ?`

	lockShardQry     = `SELECT range_id FROM shards WHERE shard_id = ? FOR UPDATE`
	readLockShardQry = `SELECT range_id FROM shards WHERE shard_id = ? LOCK IN SHARE MODE`
)

// InsertIntoShards inserts one or more rows into shards table
func (mdb *db) InsertIntoShards(row *sqlplugin.ShardsRow) (sql.Result, error) {
	return mdb.conn.Exec(createShardQry, row.ShardID, row.RangeID, row.Data, row.DataEncoding)
}

// UpdateShards updates one or more rows into shards table
func (mdb *db) UpdateShards(row *sqlplugin.ShardsRow) (sql.Result, error) {
	return mdb.conn.Exec(updateShardQry, row.RangeID, row.Data, row.DataEncoding, row.ShardID)
}

// SelectFromShards reads one or more rows from shards table
func (mdb *db) SelectFromShards(filter *sqlplugin.ShardsFilter) (*sqlplugin.ShardsRow, error) {
	var row sqlplugin.ShardsRow
	err := mdb.conn.Get(&row, getShardQry, filter.ShardID)
	if err != nil {
		return nil, err
	}
	return &row, err
}

// ReadLockShards acquires a read lock on a single row in shards table
func (mdb *db) ReadLockShards(filter *sqlplugin.ShardsFilter) (int, error) {
	var rangeID int
	err := mdb.conn.Get(&rangeID, readLockShardQry, filter.ShardID)
	return rangeID, err
}

// WriteLockShards acquires a write lock on a single row in shards table
func (mdb *db) WriteLockShards(filter *sqlplugin.ShardsFilter) (int, error) {
	var rangeID int
	err := mdb.conn.Get(&rangeID, lockShardQry, filter.ShardID)
	return rangeID, err
}
