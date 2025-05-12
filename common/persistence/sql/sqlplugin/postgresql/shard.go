package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createShardQry = `INSERT INTO
 shards (shard_id, range_id, data, data_encoding) VALUES ($1, $2, $3, $4)`

	getShardQry = `SELECT
 shard_id, range_id, data, data_encoding
 FROM shards WHERE shard_id = $1`

	updateShardQry = `UPDATE shards 
 SET range_id = $1, data = $2, data_encoding = $3 
 WHERE shard_id = $4`

	lockShardQry     = `SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE`
	readLockShardQry = `SELECT range_id FROM shards WHERE shard_id = $1 FOR SHARE`
)

// InsertIntoShards inserts one or more rows into shards table
func (pdb *db) InsertIntoShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		createShardQry,
		row.ShardID,
		row.RangeID,
		row.Data,
		row.DataEncoding,
	)
}

// UpdateShards updates one or more rows into shards table
func (pdb *db) UpdateShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		updateShardQry,
		row.RangeID,
		row.Data,
		row.DataEncoding,
		row.ShardID,
	)
}

// SelectFromShards reads one or more rows from shards table
func (pdb *db) SelectFromShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (*sqlplugin.ShardsRow, error) {
	var row sqlplugin.ShardsRow
	err := pdb.GetContext(ctx,
		&row,
		getShardQry,
		filter.ShardID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// ReadLockShards acquires a read lock on a single row in shards table
func (pdb *db) ReadLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		readLockShardQry,
		filter.ShardID,
	)
	return rangeID, err
}

// WriteLockShards acquires a write lock on a single row in shards table
func (pdb *db) WriteLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		lockShardQry,
		filter.ShardID,
	)
	return rangeID, err
}
