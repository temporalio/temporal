// Package streamsegmentstore holds implementations of the
// persistence.StreamSegmentManager interface — the `stream_segments`
// facet for native streams.  See native-streams/persistence-abi.html
// Approach C.
//
// The Memory implementation in this file is for unit tests and the M3
// development loop.  Production backends live in
// common/persistence/sql/ and common/persistence/cassandra/.
package streamsegmentstore

import (
	"context"
	"sort"
	"sync"

	"go.temporal.io/server/common/persistence"
)

// NewMemory returns an in-memory StreamSegmentManager.  Safe for
// concurrent use.
func NewMemory() *Memory {
	return &Memory{
		rows: make(map[memKey]persistence.StreamSegmentRow),
	}
}

// Memory is an in-memory StreamSegmentManager.  Rows are indexed by
// (shard, namespace, stream, segment_id, txn_id) — same key as the
// schema's PRIMARY KEY.
//
// Spec correspondence: the `segments` state variable in
// ../../spec/StreamCommit.tla.  Read filter matches the spec's
// CommittedSegments definition.
type Memory struct {
	mu   sync.RWMutex
	rows map[memKey]persistence.StreamSegmentRow

	// closed reports the closed/open lifecycle.  GetName / Close
	// surface this for parity with other Manager interfaces.
	closed bool
}

type memKey struct {
	shard       int32
	namespaceID string
	streamID    string
	segmentID   int64
	txnID       int64
}

func (m *Memory) keyFor(k persistence.StreamSegmentKey, segmentID, txnID int64) memKey {
	return memKey{
		shard:       k.ShardID,
		namespaceID: k.NamespaceID,
		streamID:    k.StreamID,
		segmentID:   segmentID,
		txnID:       txnID,
	}
}

func (m *Memory) GetName() string { return "memory" }
func (m *Memory) Close()           { m.mu.Lock(); m.closed = true; m.mu.Unlock() }

// WriteTentative writes one or more segment rows.  Idempotent on
// (segment_id, txn_id) re-write — last write wins for the data field,
// matching real backends.
func (m *Memory) WriteTentative(
	_ context.Context,
	req *persistence.WriteTentativeSegmentsRequest,
) (*persistence.WriteTentativeSegmentsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, row := range req.Rows {
		m.rows[m.keyFor(req.Key, row.SegmentID, row.TxnID)] = row
	}
	return &persistence.WriteTentativeSegmentsResponse{}, nil
}

// ReadRange returns committed rows in offset order.  Filters by
// txn_id <= CommittedTxnID and by offset overlap with
// [StartOffset, EndOffset).
func (m *Memory) ReadRange(
	_ context.Context,
	req *persistence.ReadStreamRangeRequest,
) (*persistence.ReadStreamRangeResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var rows []persistence.StreamSegmentRow
	// Per-segment_id, pick the row with the maximum committed txn_id
	// (matches the design's "max txn_id per segment" read rule).
	bySegment := make(map[int64]persistence.StreamSegmentRow)
	for k, row := range m.rows {
		if k.shard != req.Key.ShardID ||
			k.namespaceID != req.Key.NamespaceID ||
			k.streamID != req.Key.StreamID {
			continue
		}
		if req.CommittedTxnID > 0 && row.TxnID > req.CommittedTxnID {
			continue
		}
		if row.LastOffset < req.StartOffset || row.FirstOffset >= req.EndOffset {
			continue
		}
		if existing, ok := bySegment[row.SegmentID]; !ok || row.TxnID > existing.TxnID {
			bySegment[row.SegmentID] = row
		}
	}
	for _, row := range bySegment {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].FirstOffset < rows[j].FirstOffset })
	resp := &persistence.ReadStreamRangeResponse{Rows: rows}
	if req.PageSize > 0 && int32(len(rows)) > req.PageSize {
		resp.Rows = rows[:req.PageSize]
		resp.NextPageOffset = rows[req.PageSize].FirstOffset
	}
	return resp, nil
}

// DeleteByTxn removes all rows for one (stream, txn_id).
func (m *Memory) DeleteByTxn(
	_ context.Context,
	req *persistence.DeleteSegmentsByTxnRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.rows {
		if k.shard == req.Key.ShardID &&
			k.namespaceID == req.Key.NamespaceID &&
			k.streamID == req.Key.StreamID &&
			k.txnID == req.TxnID {
			delete(m.rows, k)
		}
	}
	return nil
}

// DeletePrefix removes rows whose last_offset is strictly below the
// watermark.  Matches the truncate semantics: items at LastOffset
// >= BelowOffset survive.
func (m *Memory) DeletePrefix(
	_ context.Context,
	req *persistence.DeleteSegmentsPrefixRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, row := range m.rows {
		if k.shard != req.Key.ShardID ||
			k.namespaceID != req.Key.NamespaceID ||
			k.streamID != req.Key.StreamID {
			continue
		}
		if row.LastOffset < req.BelowOffset {
			delete(m.rows, k)
		}
	}
	return nil
}

// DeleteStream removes all rows for a stream.
func (m *Memory) DeleteStream(
	_ context.Context,
	req *persistence.DeleteStreamSegmentsRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.rows {
		if k.shard == req.Key.ShardID &&
			k.namespaceID == req.Key.NamespaceID &&
			k.streamID == req.Key.StreamID {
			delete(m.rows, k)
		}
	}
	return nil
}

// CountRows is a test-only helper.
func (m *Memory) CountRows() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rows)
}
