package cassandra

import (
	"context"
	"sort"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives"
)

const (
	defaultStreamSegmentReadLimit = 1024

	templateInsertStreamSegment = `INSERT INTO stream_segments (
shard_id, namespace_id, stream_id, segment_id, txn_id,
first_offset, last_offset, item_count, payload_hash, data, data_encoding)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateSelectStreamSegments = `SELECT segment_id, txn_id, first_offset, last_offset,
item_count, payload_hash, data, data_encoding
FROM stream_segments
WHERE shard_id = ? AND namespace_id = ? AND stream_id = ?`

	templateDeleteStreamSegmentRow = `DELETE FROM stream_segments
WHERE shard_id = ? AND namespace_id = ? AND stream_id = ? AND segment_id = ? AND txn_id = ?`

	templateDeleteStreamSegments = `DELETE FROM stream_segments
WHERE shard_id = ? AND namespace_id = ? AND stream_id = ?`
)

type streamSegmentManager struct {
	session commongocql.Session
}

type cassandraStreamSegmentRow struct {
	SegmentID    int64
	TxnID        int64
	FirstOffset  int64
	LastOffset   int64
	ItemCount    int32
	PayloadHash  []byte
	Data         []byte
	DataEncoding string
}

func NewStreamSegmentManager(
	session commongocql.Session,
	_ log.Logger,
) p.StreamSegmentManager {
	return &streamSegmentManager{
		session: session,
	}
}

func (m *streamSegmentManager) GetName() string { return "cassandra" }

func (m *streamSegmentManager) Close() {}

func (m *streamSegmentManager) WriteTentative(
	ctx context.Context,
	req *p.WriteTentativeSegmentsRequest,
) (*p.WriteTentativeSegmentsResponse, error) {
	namespaceID, err := cassandraNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return nil, err
	}
	for _, row := range req.Rows {
		data, encoding := cassandraRowData(row)
		err := m.session.Query(
			templateInsertStreamSegment,
			req.Key.ShardID,
			namespaceID,
			req.Key.StreamID,
			row.SegmentID,
			row.TxnID,
			row.FirstOffset,
			row.LastOffset,
			row.ItemCount,
			row.PayloadHash,
			data,
			encoding,
		).WithContext(ctx).Exec()
		if err != nil {
			return nil, commongocql.ConvertError("WriteTentative stream_segments", err)
		}
	}
	return &p.WriteTentativeSegmentsResponse{}, nil
}

func (m *streamSegmentManager) ReadRange(
	ctx context.Context,
	req *p.ReadStreamRangeRequest,
) (*p.ReadStreamRangeResponse, error) {
	rows, err := m.readPartition(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	bySegment := make(map[int64]p.StreamSegmentRow)
	for _, row := range rows {
		if req.CommittedTxnID > 0 && row.TxnID > req.CommittedTxnID {
			continue
		}
		if row.LastOffset < req.StartOffset || row.FirstOffset >= req.EndOffset {
			continue
		}
		existing, ok := bySegment[row.SegmentID]
		if !ok || row.TxnID > existing.TxnID {
			bySegment[row.SegmentID] = p.StreamSegmentRow{
				SegmentID:   row.SegmentID,
				TxnID:       row.TxnID,
				FirstOffset: row.FirstOffset,
				LastOffset:  row.LastOffset,
				ItemCount:   row.ItemCount,
				PayloadHash: append([]byte(nil), row.PayloadHash...),
				Data:        p.NewDataBlob(append([]byte(nil), row.Data...), row.DataEncoding),
			}
		}
	}

	visibleRows := make([]p.StreamSegmentRow, 0, len(bySegment))
	for _, row := range bySegment {
		visibleRows = append(visibleRows, row)
	}
	sort.Slice(visibleRows, func(i, j int) bool {
		return visibleRows[i].FirstOffset < visibleRows[j].FirstOffset
	})

	limit := int(req.PageSize)
	if limit <= 0 {
		limit = defaultStreamSegmentReadLimit
	}
	resp := &p.ReadStreamRangeResponse{Rows: visibleRows}
	if len(visibleRows) > limit {
		resp.Rows = visibleRows[:limit]
		resp.NextPageOffset = visibleRows[limit].FirstOffset
	}
	return resp, nil
}

func (m *streamSegmentManager) DeleteByTxn(
	ctx context.Context,
	req *p.DeleteSegmentsByTxnRequest,
) error {
	return m.deletePartitionRows(ctx, req.Key, "DeleteByTxn stream_segments", func(row cassandraStreamSegmentRow) bool {
		return row.TxnID == req.TxnID
	})
}

func (m *streamSegmentManager) DeletePrefix(
	ctx context.Context,
	req *p.DeleteSegmentsPrefixRequest,
) error {
	return m.deletePartitionRows(ctx, req.Key, "DeletePrefix stream_segments", func(row cassandraStreamSegmentRow) bool {
		return row.LastOffset < req.BelowOffset
	})
}

func (m *streamSegmentManager) DeleteStream(
	ctx context.Context,
	req *p.DeleteStreamSegmentsRequest,
) error {
	namespaceID, err := cassandraNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return err
	}
	err = m.session.Query(
		templateDeleteStreamSegments,
		req.Key.ShardID,
		namespaceID,
		req.Key.StreamID,
	).WithContext(ctx).Exec()
	if err != nil {
		return commongocql.ConvertError("DeleteStream stream_segments", err)
	}
	return nil
}

func (m *streamSegmentManager) deletePartitionRows(
	ctx context.Context,
	key p.StreamSegmentKey,
	operation string,
	shouldDelete func(cassandraStreamSegmentRow) bool,
) error {
	namespaceID, err := cassandraNamespaceID(key.NamespaceID)
	if err != nil {
		return err
	}
	rows, err := m.readPartitionWithNamespace(ctx, key, namespaceID)
	if err != nil {
		return err
	}

	batch := m.session.NewBatch(commongocql.UnloggedBatch).WithContext(ctx)
	deleteCount := 0
	for _, row := range rows {
		if !shouldDelete(row) {
			continue
		}
		batch.Query(
			templateDeleteStreamSegmentRow,
			key.ShardID,
			namespaceID,
			key.StreamID,
			row.SegmentID,
			row.TxnID,
		)
		deleteCount++
	}
	if deleteCount == 0 {
		return nil
	}
	if err := m.session.ExecuteBatch(batch); err != nil {
		return commongocql.ConvertError(operation, err)
	}
	return nil
}

func (m *streamSegmentManager) readPartition(
	ctx context.Context,
	key p.StreamSegmentKey,
) ([]cassandraStreamSegmentRow, error) {
	namespaceID, err := cassandraNamespaceID(key.NamespaceID)
	if err != nil {
		return nil, err
	}
	return m.readPartitionWithNamespace(ctx, key, namespaceID)
}

func (m *streamSegmentManager) readPartitionWithNamespace(
	ctx context.Context,
	key p.StreamSegmentKey,
	namespaceID string,
) ([]cassandraStreamSegmentRow, error) {
	iter := m.session.Query(
		templateSelectStreamSegments,
		key.ShardID,
		namespaceID,
		key.StreamID,
	).WithContext(ctx).PageSize(defaultStreamSegmentReadLimit).Iter()

	var rows []cassandraStreamSegmentRow
	for {
		var row cassandraStreamSegmentRow
		if !iter.Scan(
			&row.SegmentID,
			&row.TxnID,
			&row.FirstOffset,
			&row.LastOffset,
			&row.ItemCount,
			&row.PayloadHash,
			&row.Data,
			&row.DataEncoding,
		) {
			break
		}
		row.PayloadHash = append([]byte(nil), row.PayloadHash...)
		row.Data = append([]byte(nil), row.Data...)
		rows = append(rows, row)
	}
	if err := iter.Close(); err != nil {
		return nil, commongocql.ConvertError("ReadRange stream_segments", err)
	}
	return rows, nil
}

func cassandraNamespaceID(namespaceID string) (string, error) {
	validated, err := primitives.ValidateUUID(namespaceID)
	if err != nil {
		return "", serviceerror.NewInvalidArgumentf("invalid namespace_id: %v", err)
	}
	return validated, nil
}

func cassandraRowData(row p.StreamSegmentRow) ([]byte, string) {
	if row.Data == nil {
		return nil, enumspb.ENCODING_TYPE_PROTO3.String()
	}
	return row.Data.Data, row.Data.EncodingType.String()
}
