package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	upsertStreamSegmentQry = `INSERT INTO stream_segments(
shard_id, namespace_id, stream_id, segment_id, txn_id,
first_offset, last_offset, item_count, payload_hash, data, data_encoding)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (shard_id, namespace_id, stream_id, segment_id, txn_id)
DO UPDATE SET
first_offset = EXCLUDED.first_offset,
last_offset = EXCLUDED.last_offset,
item_count = EXCLUDED.item_count,
payload_hash = EXCLUDED.payload_hash,
data = EXCLUDED.data,
data_encoding = EXCLUDED.data_encoding`

	rangeSelectStreamSegmentsQry = `SELECT s.shard_id, s.namespace_id, s.stream_id,
s.segment_id, s.txn_id, s.first_offset, s.last_offset, s.item_count,
s.payload_hash, s.data, s.data_encoding
FROM stream_segments s
JOIN (
  SELECT segment_id, MAX(txn_id) AS txn_id
  FROM stream_segments
  WHERE shard_id = $1 AND namespace_id = $2 AND stream_id = $3
    AND last_offset >= $4 AND first_offset < $5
    AND ($6 <= 0 OR txn_id <= $7)
  GROUP BY segment_id
) latest ON latest.segment_id = s.segment_id AND latest.txn_id = s.txn_id
WHERE s.shard_id = $8 AND s.namespace_id = $9 AND s.stream_id = $10
ORDER BY s.first_offset
LIMIT $11`

	deleteStreamSegmentsByTxnQry = `DELETE FROM stream_segments
WHERE shard_id = $1 AND namespace_id = $2 AND stream_id = $3 AND txn_id = $4`

	deleteStreamSegmentsPrefixQry = `DELETE FROM stream_segments
WHERE shard_id = $1 AND namespace_id = $2 AND stream_id = $3 AND last_offset < $4`

	deleteStreamSegmentsQry = `DELETE FROM stream_segments
WHERE shard_id = $1 AND namespace_id = $2 AND stream_id = $3`
)

func (pdb *db) InsertIntoStreamSegments(
	ctx context.Context,
	row *sqlplugin.StreamSegmentRow,
) (sql.Result, error) {
	return pdb.ExecContext(
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

func (pdb *db) RangeSelectFromStreamSegments(
	ctx context.Context,
	filter sqlplugin.StreamSegmentRangeFilter,
) ([]sqlplugin.StreamSegmentRow, error) {
	var rows []sqlplugin.StreamSegmentRow
	err := pdb.SelectContext(
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

func (pdb *db) DeleteFromStreamSegmentsByTxn(
	ctx context.Context,
	filter sqlplugin.StreamSegmentTxnFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, deleteStreamSegmentsByTxnQry, filter.ShardID, filter.NamespaceID, filter.StreamID, filter.TxnID)
}

func (pdb *db) DeleteFromStreamSegmentsPrefix(
	ctx context.Context,
	filter sqlplugin.StreamSegmentPrefixFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, deleteStreamSegmentsPrefixQry, filter.ShardID, filter.NamespaceID, filter.StreamID, filter.BelowOffset)
}

func (pdb *db) DeleteFromStreamSegments(
	ctx context.Context,
	filter sqlplugin.StreamSegmentStreamFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, deleteStreamSegmentsQry, filter.ShardID, filter.NamespaceID, filter.StreamID)
}
