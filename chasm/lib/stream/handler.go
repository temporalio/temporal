// handler.go contains the orchestration layer above the chasm
// transitions in publish.go / group_commit.go.  Each Handler method maps
// to an external RPC.  Compare to chasm/lib/scheduler/handler.go.
//
// The orchestration's job is to bridge the chasm-side transitions and
// the persistence-facet I/O — specifically, the cross-facet commit
// protocol where step 1 (PreparePublish) and step 3 (CommitPublish) are
// chasm transitions but step 2 (WriteSegmentsTentative) is direct
// persistence I/O.

package stream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

// Handler is the orchestrator for native-streams RPCs.  Wires
// chasm.UpdateComponent calls to the Stream component's transitions and
// runs the persistence-facet writes between PreparePublish and
// CommitPublish.
//
// Implements the gRPC StreamServiceServer.
type Handler struct {
	streampb.UnimplementedStreamServiceServer

	logger        log.Logger
	segmentStore  persistence.StreamSegmentManager
	shardResolver ShardResolver
	namespaceReg  namespace.Registry
}

// ShardResolver maps a (namespace_id, stream_id) to the shard_id used in
// the stream_segments PRIMARY KEY.  Production implementation walks the
// chasm framework's shard-map; tests pass a fixed resolver.
type ShardResolver interface {
	Shard(namespaceID, streamID string) int32
}

// NewHandler constructs a Handler.
func NewHandler(
	logger log.Logger,
	segmentStore persistence.StreamSegmentManager,
	shardResolver ShardResolver,
	namespaceReg namespace.Registry,
) *Handler {
	return &Handler{
		logger:        logger,
		segmentStore:  segmentStore,
		shardResolver: shardResolver,
		namespaceReg:  namespaceReg,
	}
}

// CreateStream invokes chasm.StartExecution to create the Stream root
// component.  The (namespace_id, stream_id) pair is the BusinessID.
func (h *Handler) CreateStream(
	ctx context.Context,
	req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	if req.NamespaceId == "" || req.StreamId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	_, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.StreamId,
		},
		NewStream,
		req,
	)
	if err != nil {
		return nil, err
	}
	return &streampb.CreateStreamResponse{StreamId: req.StreamId}, nil
}

// Publish runs the cross-facet commit protocol for one publish:
//
//  1. UpdateComponent(stream, PreparePublish) → reserves [first, end),
//     records inflight with payload_hash.
//  2. WriteTentative against the stream_segments facet → durable rows
//     under (stream, txn_id).
//  3. UpdateComponent(stream, CommitPublish) → CAS + dedup + hash
//     verification (caller-supplied for M3; M4 caller verifies it here
//     by reading back the just-written rows via ReadRange).
//
// Dedup-replay short-circuits at step 1: PreparePublish returns
// IsDedupReplay=true and Publish exits without step 2 or 3.
//
// Spec correspondence: composes PreparePublish + WriteSegmentsTentative
// + CommitPublish actions.
func (h *Handler) Publish(
	ctx context.Context,
	req *streampb.PublishRequest,
) (*streampb.PublishResponse, error) {
	if err := validatePublishRequest(req); err != nil {
		return nil, err
	}
	ref := chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.StreamId,
	})
	streamKey := persistence.StreamSegmentKey{
		ShardID:     h.shardResolver.Shard(req.NamespaceId, req.StreamId),
		NamespaceID: req.NamespaceId,
		StreamID:    req.StreamId,
	}

	// Step 1: PreparePublish chasm transition.
	prep, _, err := chasm.UpdateComponent(
		ctx, ref,
		(*Stream).PreparePublish,
		PreparePublishInput{
			PublisherID: req.PublisherId,
			Sequence:    req.Sequence,
			ItemCount:   int32(len(req.Items)),
			PayloadHash: req.PayloadHash,
		},
	)
	if err != nil {
		return nil, err
	}
	if prep.IsDedupReplay {
		return &streampb.PublishResponse{
			FirstOffset: prep.FirstOffset,
			ItemCount:   int64(prep.ItemCount),
		}, nil
	}

	// Step 2: WriteTentative against the segment facet.
	if err := h.writeTentativeSegments(ctx, streamKey, prep, req.Items); err != nil {
		// Step-2 failure: caller retries.  The inflight remains in the
		// map and will be swept by SweepExpired if not retried before
		// inflightGraceDuration.
		return nil, err
	}

	// Step 2.5: proof-of-write read-back.  In the M4 SQL-collapse
	// optimization this fuses with step 3 in one DB transaction; for
	// now we do a separate read so the CommitPublish transition
	// receives a boolean it can trust.
	proofOK, err := h.verifyProofOfWrite(ctx, streamKey, prep)
	if err != nil {
		return nil, err
	}

	// Step 3: CommitPublish chasm transition.
	commit, _, err := chasm.UpdateComponent(
		ctx, ref,
		(*Stream).CommitPublish,
		CommitPublishInput{
			TxnID:                prep.TxnID,
			ProofOfWriteVerified: proofOK,
		},
	)
	if err != nil {
		return nil, err
	}
	return &streampb.PublishResponse{
		FirstOffset: commit.FirstOffset,
		ItemCount:   int64(commit.ItemCount),
	}, nil
}

func (h *Handler) writeTentativeSegments(
	ctx context.Context,
	key persistence.StreamSegmentKey,
	prep PreparePublishOutput,
	items []*streampb.PublishItem,
) error {
	if len(items) == 0 {
		return ErrInvalidPublishBatchEmpty
	}
	// One segment row per (segment_id, txn_id).  The reference
	// implementation puts the whole batch in one row covering the
	// reserved offset range. Until we chunk and merge physical segment
	// rows, use first_offset as the row segment key so successive appends
	// in the same configured segment do not hide one another under the
	// latest-txn-per-segment read rule.
	rowData, err := encodeSegmentRowData(prep.FirstOffset, items)
	if err != nil {
		return err
	}
	rows := []persistence.StreamSegmentRow{{
		SegmentID:   prep.FirstOffset,
		TxnID:       prep.TxnID,
		FirstOffset: prep.FirstOffset,
		LastOffset:  prep.FirstOffset + int64(prep.ItemCount) - 1,
		ItemCount:   prep.ItemCount,
		PayloadHash: hashSegmentRowData(rowData),
		Data:        &commonpb.DataBlob{Data: rowData},
	}}
	_, err = h.segmentStore.WriteTentative(ctx, &persistence.WriteTentativeSegmentsRequest{
		Key:  key,
		Rows: rows,
	})
	return err
}

// verifyProofOfWrite reads back the tentative rows we just wrote and
// checks coverage + hash equality.  Uses CommittedTxnID=0 to bypass the
// committed-txn filter (the rows we just wrote are by definition not
// yet in the committed set).
func (h *Handler) verifyProofOfWrite(
	ctx context.Context,
	key persistence.StreamSegmentKey,
	prep PreparePublishOutput,
) (bool, error) {
	resp, err := h.segmentStore.ReadRange(ctx, &persistence.ReadStreamRangeRequest{
		Key:            key,
		StartOffset:    prep.FirstOffset,
		EndOffset:      prep.FirstOffset + int64(prep.ItemCount),
		CommittedTxnID: 0,
	})
	if err != nil {
		return false, err
	}
	covered := make(map[int64]bool, prep.ItemCount)
	items := make([]*streampb.PublishItem, 0, prep.ItemCount)
	for _, row := range resp.Rows {
		if row.TxnID != prep.TxnID {
			continue
		}
		if row.Data == nil {
			return false, serviceerror.NewInternal("stream segment row missing data")
		}
		if len(row.PayloadHash) > 0 &&
			!bytes.Equal(hashSegmentRowData(row.Data.Data), row.PayloadHash) {
			return false, nil
		}
		rowItems, err := decodeSegmentRowData(row)
		if err != nil {
			return false, err
		}
		for _, rowItem := range rowItems {
			if rowItem.offset < prep.FirstOffset ||
				rowItem.offset >= prep.FirstOffset+int64(prep.ItemCount) {
				continue
			}
			covered[rowItem.offset] = true
			items = append(items, rowItem.item)
		}
	}
	endOffset := prep.FirstOffset + int64(prep.ItemCount)
	for off := prep.FirstOffset; off < endOffset; off++ {
		if !covered[off] {
			return false, nil
		}
	}
	if len(prep.PayloadHash) > 0 &&
		!bytes.Equal(hashPublishItemsPayload(items), prep.PayloadHash) {
		return false, nil
	}
	return true, nil
}

// DescribeStream returns the current frontier, audit fields, and
// selected counts.  Read-only chasm operation.
func (h *Handler) DescribeStream(
	ctx context.Context,
	req *streampb.DescribeStreamRequest,
) (*streampb.DescribeStreamResponse, error) {
	if req.NamespaceId == "" || req.StreamId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	ref := chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.StreamId,
	})
	resp, err := chasm.ReadComponent(
		ctx, ref,
		func(s *Stream, _ chasm.Context, _ *struct{}) (*streampb.DescribeStreamResponse, error) {
			return &streampb.DescribeStreamResponse{
				State:             s.StreamState,
				InflightCount:     int64(len(s.Inflights)),
				SubscriptionCount: int64(len(s.Subscriptions)),
				PublisherCount:    int64(len(s.Publishers)),
			}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ReadRange returns committed items in [start_offset, end_offset).
// Reads the visibility frontier from the chasm Stream component, then
// queries the segment facet with that committed_txn_id as filter.
func (h *Handler) ReadRange(
	ctx context.Context,
	req *streampb.ReadRangeRequest,
) (*streampb.ReadRangeResponse, error) {
	if req.NamespaceId == "" || req.StreamId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	if req.EndOffset <= req.StartOffset {
		return nil, serviceerror.NewInvalidArgument("end_offset must exceed start_offset")
	}
	ref := chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.StreamId,
	})
	type frontier struct {
		baseOffset     int64
		headOffset     int64
		committedTxnID int64
	}
	fr, err := chasm.ReadComponent(
		ctx, ref,
		func(s *Stream, _ chasm.Context, _ *struct{}) (frontier, error) {
			return frontier{
				baseOffset:     s.StreamState.BaseOffset,
				headOffset:     s.StreamState.HeadOffset,
				committedTxnID: s.StreamState.CommittedTxnId,
			}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	if req.StartOffset < fr.baseOffset {
		return nil, ErrOffsetTruncated
	}

	streamKey := persistence.StreamSegmentKey{
		ShardID:     h.shardResolver.Shard(req.NamespaceId, req.StreamId),
		NamespaceID: req.NamespaceId,
		StreamID:    req.StreamId,
	}
	storeResp, err := h.segmentStore.ReadRange(ctx, &persistence.ReadStreamRangeRequest{
		Key:            streamKey,
		StartOffset:    req.StartOffset,
		EndOffset:      req.EndOffset,
		CommittedTxnID: fr.committedTxnID,
	})
	if err != nil {
		return nil, err
	}
	items := make([]*streampb.PublishItem, 0, len(storeResp.Rows))
	offsets := make([]int64, 0, len(storeResp.Rows))
	topicFilter := make(map[string]struct{}, len(req.Topics))
	for _, topic := range req.Topics {
		topicFilter[topic] = struct{}{}
	}
	readEndOffset := req.EndOffset
	if fr.headOffset < readEndOffset {
		readEndOffset = fr.headOffset
	}
	for _, row := range storeResp.Rows {
		rowItems, err := decodeSegmentRowData(row)
		if err != nil {
			return nil, err
		}
		for _, rowItem := range rowItems {
			if rowItem.offset < req.StartOffset || rowItem.offset >= readEndOffset {
				continue
			}
			if len(topicFilter) > 0 {
				if _, ok := topicFilter[rowItem.item.Topic]; !ok {
					continue
				}
			}
			items = append(items, rowItem.item)
			offsets = append(offsets, rowItem.offset)
		}
	}
	nextOffset := storeResp.NextPageOffset
	if nextOffset == 0 {
		nextOffset = readEndOffset
	}
	return &streampb.ReadRangeResponse{
		Items:      items,
		NextOffset: nextOffset,
		Offsets:    offsets,
	}, nil
}

// Close seals the stream (phase 1).
func (h *Handler) Close(
	ctx context.Context,
	req *streampb.CloseRequest,
) (*streampb.CloseResponse, error) {
	if req.NamespaceId == "" || req.StreamId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	ref := chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.StreamId,
	})
	_, _, err := chasm.UpdateComponent(ctx, ref, (*Stream).Close, CloseInput{
		ClosedBy:    req.ClosedBy,
		CloseReason: req.CloseReason,
	})
	if err != nil {
		return nil, err
	}
	return &streampb.CloseResponse{}, nil
}

// Truncate moves base_offset forward.  See exactly-once.html "Truncate
// vs in-flight publish" for the guard set.
func (h *Handler) Truncate(
	ctx context.Context,
	req *streampb.TruncateRequest,
) (*streampb.TruncateResponse, error) {
	if req.NamespaceId == "" || req.StreamId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	ref := chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.StreamId,
	})
	out, _, err := chasm.UpdateComponent(ctx, ref, (*Stream).Truncate, TruncateInput{
		UpToOffset: req.UpToOffset,
		Force:      req.Force,
	})
	if err != nil {
		return nil, err
	}
	// Best-effort side-effect: delete segment rows below the new base.
	// On Cassandra this becomes a single range tombstone per
	// native-streams/cassandra-operability.html range-delete discipline.
	streamKey := persistence.StreamSegmentKey{
		ShardID:     h.shardResolver.Shard(req.NamespaceId, req.StreamId),
		NamespaceID: req.NamespaceId,
		StreamID:    req.StreamId,
	}
	if err := h.segmentStore.DeletePrefix(ctx, &persistence.DeleteSegmentsPrefixRequest{
		Key:         streamKey,
		BelowOffset: out.NewBaseOffset,
	}); err != nil {
		h.logger.Warn("stream_segments DeletePrefix failed; rows will be reclaimed by retention")
	}
	return &streampb.TruncateResponse{
		NewBaseOffset:                   out.NewBaseOffset,
		ForceTruncatedSubscriptionCount: int64(out.ForceTruncatedSubscriptionCount),
	}, nil
}

// DeleteStream is terminal: closes the stream (if open), removes all
// segment rows, and removes the chasm execution.
func (h *Handler) DeleteStream(
	ctx context.Context,
	req *streampb.DeleteStreamRequest,
) (*streampb.DeleteStreamResponse, error) {
	if req.NamespaceId == "" || req.StreamId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	ref := chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.StreamId,
	})
	_, _, err := chasm.UpdateComponent(ctx, ref, (*Stream).Close, CloseInput{
		ClosedBy:    "delete-stream",
		CloseReason: streampb.STREAM_CLOSE_REASON_DELETED,
	})
	if err != nil {
		// Already-closed is OK at this point; the segment-row delete still proceeds.
		h.logger.Warn("DeleteStream: close transition failed; continuing to row cleanup")
	}
	streamKey := persistence.StreamSegmentKey{
		ShardID:     h.shardResolver.Shard(req.NamespaceId, req.StreamId),
		NamespaceID: req.NamespaceId,
		StreamID:    req.StreamId,
	}
	if err := h.segmentStore.DeleteStream(ctx, &persistence.DeleteStreamSegmentsRequest{
		Key: streamKey,
	}); err != nil {
		return nil, err
	}
	return &streampb.DeleteStreamResponse{}, nil
}

// ListStreams paginates streams in a namespace via the CHASM visibility
// list API.
func (h *Handler) ListStreams(
	ctx context.Context,
	req *streampb.ListStreamsRequest,
) (*streampb.ListStreamsResponse, error) {
	if req.NamespaceId == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id required")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	namespaceName := req.NamespaceId
	if h.namespaceReg != nil {
		name, err := h.namespaceReg.GetNamespaceName(namespace.ID(req.NamespaceId))
		if err != nil {
			return nil, err
		}
		namespaceName = name.String()
	}
	resp, err := chasm.ListExecutions[*Stream, *streampb.StreamState](ctx, &chasm.ListExecutionsRequest{
		NamespaceName: namespaceName,
		PageSize:      int(pageSize),
		NextPageToken: req.NextPageToken,
		Query:         "",
	})
	if err != nil {
		return nil, err
	}
	streams := make([]*streampb.StreamSummary, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		state := exec.ChasmMemo
		summary := &streampb.StreamSummary{
			StreamId:    exec.BusinessID,
			HeadOffset:  state.GetHeadOffset(),
			BaseOffset:  state.GetBaseOffset(),
			Closed:      state.GetClosed(),
			CreatedTime: state.GetCreatedTime(),
			ClosedTime:  state.GetClosedTime(),
		}
		streams = append(streams, summary)
	}
	return &streampb.ListStreamsResponse{
		Streams:       streams,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func validatePublishRequest(req *streampb.PublishRequest) error {
	if req.NamespaceId == "" || req.StreamId == "" {
		return serviceerror.NewInvalidArgument("namespace_id and stream_id required")
	}
	if req.PublisherId == "" {
		return serviceerror.NewInvalidArgument("publisher_id required")
	}
	if len(req.Items) == 0 {
		return ErrInvalidPublishBatchEmpty
	}
	if len(req.PayloadHash) > 0 {
		// Best-effort sanity check that the caller's hash matches their bytes.
		if !bytes.Equal(hashPublishItemsPayload(req.Items), req.PayloadHash) {
			return serviceerror.NewInvalidArgument("payload_hash does not match items")
		}
	}
	return nil
}

type decodedSegmentRowItem struct {
	offset int64
	item   *streampb.PublishItem
}

func hashPublishItemsPayload(items []*streampb.PublishItem) []byte {
	hasher := sha256.New()
	for _, item := range items {
		hasher.Write(item.Data)
	}
	return hasher.Sum(nil)
}

func hashSegmentRowData(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func encodeSegmentRowData(firstOffset int64, items []*streampb.PublishItem) ([]byte, error) {
	var buf bytes.Buffer
	var scratch [8]byte
	for i, item := range items {
		topicBytes := []byte(item.Topic)
		if len(topicBytes) > int(^uint32(0)) || len(item.Data) > int(^uint32(0)) {
			return nil, serviceerror.NewInvalidArgument("stream item exceeds row framing limits")
		}
		binary.BigEndian.PutUint64(scratch[:8], uint64(firstOffset+int64(i)))
		buf.Write(scratch[:8])
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(topicBytes)))
		buf.Write(scratch[:4])
		buf.Write(topicBytes)
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(item.Data)))
		buf.Write(scratch[:4])
		buf.Write(item.Data)
	}
	return buf.Bytes(), nil
}

func decodeSegmentRowData(row persistence.StreamSegmentRow) ([]decodedSegmentRowItem, error) {
	if row.Data == nil {
		return nil, serviceerror.NewInternal("stream segment row missing data")
	}
	data := row.Data.Data
	items := make([]decodedSegmentRowItem, 0, row.ItemCount)
	for pos := 0; pos < len(data); {
		if len(data)-pos < 12 {
			return nil, serviceerror.NewInternal("malformed stream segment row")
		}
		offset := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		topicLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if topicLen < 0 || len(data)-pos < topicLen+4 {
			return nil, serviceerror.NewInternal("malformed stream segment row")
		}
		topic := string(data[pos : pos+topicLen])
		pos += topicLen
		payloadLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if payloadLen < 0 || len(data)-pos < payloadLen {
			return nil, serviceerror.NewInternal("malformed stream segment row")
		}
		payload := append([]byte(nil), data[pos:pos+payloadLen]...)
		pos += payloadLen
		items = append(items, decodedSegmentRowItem{
			offset: offset,
			item:   &streampb.PublishItem{Data: payload, Topic: topic},
		})
	}
	return items, nil
}
