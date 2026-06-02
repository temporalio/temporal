package persistence

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

// StreamSegmentManager is the persistence interface for the native-streams
// `stream_segments` facet.  See native-streams/persistence-abi.html
// Approach C and native-streams/exactly-once.html §6.
//
// The facet stores segment rows keyed by
// (shard_id, namespace_id, stream_id, segment_id, txn_id).  Multiple
// tentative versions of the same offset range coexist under different
// txn_ids; the read path filters by txn_id <= committed_txn_id where
// committed_txn_id is a field on the Stream chasm component.
//
// On Cassandra: write paths are non-CAS plain quorum writes; cross-facet
// commit uses chasm-side CAS plus the in-band proof-of-write check at
// commit time.  On SQL backends, the manager interface is the same but
// writes can be coalesced into the same DB transaction as the chasm
// transition (see SQL-collapse note in exactly-once.html §6).
type StreamSegmentManager interface {
	Closeable
	GetName() string

	// WriteTentative writes the segment rows for one txn_id of one
	// publish.  Idempotent under same-txn retry with identical bytes (the
	// same (segment_id, txn_id) clustering key rewrites the same payload).
	// A retry with different persisted row bytes is detected at commit
	// time via the payload-hash proof-of-write check; the row write itself
	// does not reject.
	WriteTentative(ctx context.Context, request *WriteTentativeSegmentsRequest) (*WriteTentativeSegmentsResponse, error)

	// ReadRange returns committed segment rows in the requested offset
	// range, honoring the txn_id <= committed_txn_id visibility filter.
	// Returns rows in offset order.
	ReadRange(ctx context.Context, request *ReadStreamRangeRequest) (*ReadStreamRangeResponse, error)

	// DeleteByTxn removes all rows for one (stream, txn_id).  Called from
	// the abort-cleanup side-effect task after a CAS failure, expiry
	// sweep, group abort, or close-cleanup.
	DeleteByTxn(ctx context.Context, request *DeleteSegmentsByTxnRequest) error

	// DeletePrefix removes rows whose offset is below the truncate
	// watermark.  Single range-tombstone on Cassandra; row-level delete
	// on SQL.  Called from Truncate and ForceTruncate.
	DeletePrefix(ctx context.Context, request *DeleteSegmentsPrefixRequest) error

	// DeleteStream removes all rows for a stream.  Called from
	// DeleteStream RPC (terminal).
	DeleteStream(ctx context.Context, request *DeleteStreamSegmentsRequest) error
}

// StreamSegmentKey identifies a stream within the persistence layer.
// Shard is derived from (namespace_id, stream_id) by the chasm engine.
type StreamSegmentKey struct {
	ShardID     int32
	NamespaceID string
	StreamID    string
}

// StreamSegmentRow is one persisted segment row.
type StreamSegmentRow struct {
	SegmentID   int64
	TxnID       int64
	FirstOffset int64
	LastOffset  int64
	ItemCount   int32
	// PayloadHash is SHA-256 over Data.Data, the exact bytes persisted in
	// stream_segments.data.
	PayloadHash []byte
	Data        *commonpb.DataBlob
}

// WriteTentativeSegmentsRequest writes one or more segment rows under a
// single (stream, txn_id) prefix.  All rows share the same TxnID; varying
// SegmentID lets a single publish span multiple segments under a single
// inflight when policy rolls a segment boundary mid-batch.
type WriteTentativeSegmentsRequest struct {
	Key  StreamSegmentKey
	Rows []StreamSegmentRow
}

type WriteTentativeSegmentsResponse struct{}

// ReadStreamRangeRequest filters by absolute offset range.  CommittedTxnID
// is the visibility frontier; only rows with txn_id <= CommittedTxnID
// are returned.
type ReadStreamRangeRequest struct {
	Key            StreamSegmentKey
	StartOffset    int64
	EndOffset      int64
	CommittedTxnID int64

	// PageSize caps the number of rows returned per call; pagination via
	// NextPageOffset on the response.  Zero means use the driver's
	// default.
	PageSize int32
}

type ReadStreamRangeResponse struct {
	Rows           []StreamSegmentRow
	NextPageOffset int64
}

// DeleteSegmentsByTxnRequest removes all rows for one (stream, txn_id).
type DeleteSegmentsByTxnRequest struct {
	Key   StreamSegmentKey
	TxnID int64
}

// DeleteSegmentsPrefixRequest removes rows whose last_offset is below
// the watermark.  Used by Truncate.  Coalesced into a single range
// tombstone on Cassandra per native-streams/cassandra-operability.html
// (range-tombstone discipline).
type DeleteSegmentsPrefixRequest struct {
	Key         StreamSegmentKey
	BelowOffset int64
}

// DeleteStreamSegmentsRequest removes all rows for a stream.
type DeleteStreamSegmentsRequest struct {
	Key StreamSegmentKey
}
