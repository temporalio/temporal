package sqlite

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	upsertStreamSegmentQry = `INSERT OR REPLACE INTO stream_segments(
shard_id, namespace_id, stream_id, segment_id, txn_id,
first_offset, last_offset, item_count, payload_hash, data, data_encoding)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	rangeSelectStreamSegmentsQry = `SELECT s.shard_id, s.namespace_id, s.stream_id,
s.segment_id, s.txn_id, s.first_offset, s.last_offset, s.item_count,
s.payload_hash, s.data, s.data_encoding
FROM stream_segments s
JOIN (
  SELECT segment_id, MAX(txn_id) AS txn_id
  FROM stream_segments
  WHERE shard_id = ? AND namespace_id = ? AND stream_id = ?
    AND last_offset >= ? AND first_offset < ?
    AND (? <= 0 OR txn_id <= ?)
  GROUP BY segment_id
) latest ON latest.segment_id = s.segment_id AND latest.txn_id = s.txn_id
WHERE s.shard_id = ? AND s.namespace_id = ? AND s.stream_id = ?
ORDER BY s.first_offset
LIMIT ?`

	deleteStreamSegmentsByTxnQry = `DELETE FROM stream_segments
WHERE shard_id = ? AND namespace_id = ? AND stream_id = ? AND txn_id = ?`

	deleteStreamSegmentsPrefixQry = `DELETE FROM stream_segments
WHERE shard_id = ? AND namespace_id = ? AND stream_id = ? AND last_offset < ?`

	deleteStreamSegmentsQry = `DELETE FROM stream_segments
WHERE shard_id = ? AND namespace_id = ? AND stream_id = ?`
)

func (mdb *db) InsertIntoStreamSegments(
	ctx context.Context,
	row *sqlplugin.StreamSegmentRow,
) (sql.Result, error) {
	return mdb.conn.ExecContext(
		ctx,
		upsertStreamSegmentQry,
		row.ShardID,
		row.NamespaceID,
		row.StreamID,
		row.SegmentID,
		row.TxnID,
		row.FirstOffset,
		row.LastOffset,
		row.ItemCount,
		row.PayloadHash,
		row.Data,
		row.DataEncoding,
	)
}

func (mdb *db) RangeSelectFromStreamSegments(
	ctx context.Context,
	filter sqlplugin.StreamSegmentRangeFilter,
) ([]sqlplugin.StreamSegmentRow, error) {
	var rows []sqlplugin.StreamSegmentRow
	err := mdb.conn.SelectContext(
		ctx,
		&rows,
		rangeSelectStreamSegmentsQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.StreamID,
		filter.StartOffset,
		filter.EndOffset,
		filter.CommittedTxnID,
		filter.CommittedTxnID,
		filter.ShardID,
		filter.NamespaceID,
		filter.StreamID,
		filter.Limit,
	)
	return rows, err
}

func (mdb *db) DeleteFromStreamSegmentsByTxn(
	ctx context.Context,
	filter sqlplugin.StreamSegmentTxnFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx, deleteStreamSegmentsByTxnQry, filter.ShardID, filter.NamespaceID, filter.StreamID, filter.TxnID)
}

func (mdb *db) DeleteFromStreamSegmentsPrefix(
	ctx context.Context,
	filter sqlplugin.StreamSegmentPrefixFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx, deleteStreamSegmentsPrefixQry, filter.ShardID, filter.NamespaceID, filter.StreamID, filter.BelowOffset)
}

func (mdb *db) DeleteFromStreamSegments(
	ctx context.Context,
	filter sqlplugin.StreamSegmentStreamFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx, deleteStreamSegmentsQry, filter.ShardID, filter.NamespaceID, filter.StreamID)
}
