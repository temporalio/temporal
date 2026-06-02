package sql

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

const defaultStreamSegmentReadLimit = 1024

type sqlStreamSegmentManager struct {
	SqlStore
}

func NewSQLStreamSegmentManager(
	db sqlplugin.DB,
	logger log.Logger,
	serializer serialization.Serializer,
) p.StreamSegmentManager {
	return &sqlStreamSegmentManager{
		SqlStore: NewSQLStore(db, logger, serializer),
	}
}

func (m *sqlStreamSegmentManager) WriteTentative(
	ctx context.Context,
	req *p.WriteTentativeSegmentsRequest,
) (*p.WriteTentativeSegmentsResponse, error) {
	namespaceID, err := sqlNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return nil, err
	}
	for _, row := range req.Rows {
		data, encoding := rowData(row)
		if _, err := m.DB.InsertIntoStreamSegments(ctx, &sqlplugin.StreamSegmentRow{
			ShardID:      req.Key.ShardID,
			NamespaceID:  namespaceID,
			StreamID:     req.Key.StreamID,
			SegmentID:    row.SegmentID,
			TxnID:        row.TxnID,
			FirstOffset:  row.FirstOffset,
			LastOffset:   row.LastOffset,
			ItemCount:    row.ItemCount,
			PayloadHash:  row.PayloadHash,
			Data:         data,
			DataEncoding: encoding,
		}); err != nil {
			return nil, serviceerror.NewUnavailablef("WriteTentative stream_segments: %v", err)
		}
	}
	return &p.WriteTentativeSegmentsResponse{}, nil
}

func (m *sqlStreamSegmentManager) ReadRange(
	ctx context.Context,
	req *p.ReadStreamRangeRequest,
) (*p.ReadStreamRangeResponse, error) {
	namespaceID, err := sqlNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return nil, err
	}
	limit := int(req.PageSize)
	if limit <= 0 {
		limit = defaultStreamSegmentReadLimit
	}
	rows, err := m.DB.RangeSelectFromStreamSegments(ctx, sqlplugin.StreamSegmentRangeFilter{
		ShardID:        req.Key.ShardID,
		NamespaceID:    namespaceID,
		StreamID:       req.Key.StreamID,
		StartOffset:    req.StartOffset,
		EndOffset:      req.EndOffset,
		CommittedTxnID: req.CommittedTxnID,
		Limit:          limit + 1,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailablef("ReadRange stream_segments: %v", err)
	}

	resp := &p.ReadStreamRangeResponse{
		Rows: make([]p.StreamSegmentRow, 0, min(len(rows), limit)),
	}
	if len(rows) > limit {
		resp.NextPageOffset = rows[limit].FirstOffset
		rows = rows[:limit]
	}
	for _, row := range rows {
		resp.Rows = append(resp.Rows, p.StreamSegmentRow{
			SegmentID:   row.SegmentID,
			TxnID:       row.TxnID,
			FirstOffset: row.FirstOffset,
			LastOffset:  row.LastOffset,
			ItemCount:   row.ItemCount,
			PayloadHash: row.PayloadHash,
			Data:        p.NewDataBlob(row.Data, row.DataEncoding),
		})
	}
	return resp, nil
}

func (m *sqlStreamSegmentManager) DeleteByTxn(
	ctx context.Context,
	req *p.DeleteSegmentsByTxnRequest,
) error {
	namespaceID, err := sqlNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return err
	}
	_, err = m.DB.DeleteFromStreamSegmentsByTxn(ctx, sqlplugin.StreamSegmentTxnFilter{
		ShardID:     req.Key.ShardID,
		NamespaceID: namespaceID,
		StreamID:    req.Key.StreamID,
		TxnID:       req.TxnID,
	})
	if err != nil {
		return serviceerror.NewUnavailablef("DeleteByTxn stream_segments: %v", err)
	}
	return nil
}

func (m *sqlStreamSegmentManager) DeletePrefix(
	ctx context.Context,
	req *p.DeleteSegmentsPrefixRequest,
) error {
	namespaceID, err := sqlNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return err
	}
	_, err = m.DB.DeleteFromStreamSegmentsPrefix(ctx, sqlplugin.StreamSegmentPrefixFilter{
		ShardID:     req.Key.ShardID,
		NamespaceID: namespaceID,
		StreamID:    req.Key.StreamID,
		BelowOffset: req.BelowOffset,
	})
	if err != nil {
		return serviceerror.NewUnavailablef("DeletePrefix stream_segments: %v", err)
	}
	return nil
}

func (m *sqlStreamSegmentManager) DeleteStream(
	ctx context.Context,
	req *p.DeleteStreamSegmentsRequest,
) error {
	namespaceID, err := sqlNamespaceID(req.Key.NamespaceID)
	if err != nil {
		return err
	}
	_, err = m.DB.DeleteFromStreamSegments(ctx, sqlplugin.StreamSegmentStreamFilter{
		ShardID:     req.Key.ShardID,
		NamespaceID: namespaceID,
		StreamID:    req.Key.StreamID,
	})
	if err != nil {
		return serviceerror.NewUnavailablef("DeleteStream stream_segments: %v", err)
	}
	return nil
}

func sqlNamespaceID(namespaceID string) ([]byte, error) {
	parsed, err := primitives.ParseUUID(namespaceID)
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("invalid namespace_id: %v", err)
	}
	return parsed, nil
}

func rowData(row p.StreamSegmentRow) ([]byte, string) {
	if row.Data == nil {
		return nil, enumspb.ENCODING_TYPE_PROTO3.String()
	}
	return row.Data.Data, row.Data.EncodingType.String()
}
