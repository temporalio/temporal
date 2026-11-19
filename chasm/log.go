package chasm

import (
	"context"
	"errors"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

const (
	logNewBatchesFieldName = "newBatches"
)

var ErrInvalidBatchID = errors.New("invalid batch ID")

type LogMessageBatch[T proto.Message] []*LogMessage[T]

func (b LogMessageBatch[T]) BatchID() int64 {
	return b[0].sequenceID
}

func (b LogMessageBatch[T]) lastSequenceID() int64 {
	return b[len(b)-1].sequenceID
}

type logMessageInternalIf interface {
	SequenceID() int64
	ProtoMessage() proto.Message
}

type LogMessage[T proto.Message] struct {
	sequenceID int64
	message    proto.Message
}

var _ logMessageInternalIf = (*LogMessage[proto.Message])(nil)

func (lm *LogMessage[T]) SequenceID() int64 {
	return lm.sequenceID
}

func (lm *LogMessage[T]) Message() T {
	return lm.message.(T)
}

func (lm *LogMessage[T]) ProtoMessage() proto.Message {
	return lm.message
}

func NewLogMessage[T proto.Message](sequenceID int64, message T) *LogMessage[T] {
	return &LogMessage[T]{
		sequenceID: sequenceID,
		message:    message,
	}
}

type logInternalIf interface {
	init(node *Node)
	setNode(node *Node)
	getHeadBatchID() int64
	getLastSequenceID() int64
}

type Log[T proto.Message] struct {
	node     *Node
	metadata *persistencespb.ChasmLogMetadata

	headBatchID    int64
	lastSequenceID int64
	newBatches     []LogMessageBatch[T]
}

var _ logInternalIf = (*Log[proto.Message])(nil)

// init implements [logInternalIf].
func (log *Log[T]) init(node *Node) {
	var metadata *persistencespb.ChasmLogMetadata
	if node != nil {
		metadata = node.serializedNode.GetMetadata().GetLogAttributes().GetMetadata()
	}

	log.node = node
	log.metadata = metadata
	log.headBatchID = metadata.GetHeadBatchId()
	log.lastSequenceID = metadata.GetLastSequenceId()
	log.newBatches = nil
}

// setNode implements [logInternalIf].
func (log *Log[T]) setNode(node *Node) {
	log.node = node
}

// getHeadBatchID implements [logInternalIf].
func (log *Log[T]) getHeadBatchID() int64 {
	return log.headBatchID
}

// getLastSequenceID implements [logInternalIf].
func (log *Log[T]) getLastSequenceID() int64 {
	return log.lastSequenceID
}

// Append appends the messages as a single batch of messages at the end of Log.
// If called with no messages, it's no-op and returns 0.
// Otherwise, it returns the new batch's ID.
func (log *Log[T]) Append(messages ...T) int64 {
	if len(messages) == 0 {
		return 0
	}
	batch := make(LogMessageBatch[T], len(messages))
	for i, message := range messages {
		log.lastSequenceID++
		batch[i] = NewLogMessage(log.lastSequenceID, message)
	}
	if log.headBatchID == common.EmptyEventID {
		log.headBatchID = batch.BatchID()
	}
	log.newBatches = append(log.newBatches, batch)
	return batch.BatchID()
}

// AdvanceHead advances the head to the new batch ID.
func (log *Log[T]) AdvanceHead(ctx context.Context, newHeadBatchID int64) error {
	if newHeadBatchID < common.FirstEventID {
		return ErrInvalidBatchID
	}
	if newHeadBatchID <= log.metadata.GetLastSequenceId() {
		token, err := proto.Marshal(&tokenspb.HistoryEventRef{
			EventBatchId: newHeadBatchID,
		})
		if err != nil {
			return err
		}
		batch, err := log.node.backend.LoadChasmLogMessages(ctx, token, log.metadata.GetBranch())
		if err != nil {
			return err
		}
		if batch.Messages[0].SequenceId != newHeadBatchID {
			return ErrInvalidBatchID
		}
	} else {
		sequenceID := log.metadata.GetLastSequenceId()
		idx := 0
		for idx < len(log.newBatches) && log.newBatches[idx].BatchID() < newHeadBatchID {
			idx++
		}
		if idx-1 > 0 {
			sequenceID = log.newBatches[idx-1].lastSequenceID()
		}
		if sequenceID+1 != newHeadBatchID {
			return ErrInvalidBatchID
		}
		log.newBatches = log.newBatches[idx:]
	}
	log.headBatchID = newHeadBatchID
	return nil
}

// Read returns an iterator to the message at or after offset.
func (log *Log[T]) Read(ctx context.Context, offset int64) *LogMessageIterator[T] {
	batchID := max(common.FirstEventID, log.headBatchID+offset)
	return newLogMessageIterator(ctx, log, batchID)
}

// ReadBatch returns an iterator to the batch of messages at or after offset.
func (log *Log[T]) ReadBatch(ctx context.Context, offset int64) *LogMessageBatchIterator[T] {
	batchID := max(common.FirstEventID, log.headBatchID+offset)
	return newLogMessageBatchIterator(ctx, log, batchID)
}

type LogMessageBatchIterator[T proto.Message] struct {
	ctx context.Context
	log *Log[T]

	token             *tokenspb.HistoryEventRef
	currBatch         []*persistencespb.ChasmLogMessage
	currNewBatchesIdx int
}

func newLogMessageBatchIterator[T proto.Message](
	ctx context.Context,
	log *Log[T],
	batchID int64,
) *LogMessageBatchIterator[T] {
	return &LogMessageBatchIterator[T]{
		ctx: ctx,
		log: log,

		token: &tokenspb.HistoryEventRef{
			EventBatchId: batchID,
		},
	}
}

func (it *LogMessageBatchIterator[T]) HasNext() bool {
	if it.ctx.Err() != nil {
		return false
	}
	if it.currBatch != nil {
		return true
	}
	if it.token == nil {
		return false
	}
	if it.token.EventBatchId <= it.log.metadata.GetLastSequenceId() {
		token, err := proto.Marshal(it.token)
		if err != nil {
			return false
		}
		batch, err := it.log.node.backend.LoadChasmLogMessages(it.ctx, token, it.log.metadata.GetBranch())
		if err != nil {
			return false
		}
		if len(batch.GetMessages()) == 0 {
			return false
		}
		it.currBatch = batch.Messages
		it.token.EventBatchId = batch.Messages[len(batch.Messages)-1].SequenceId + 1
		return true
	}
	for it.currNewBatchesIdx < len(it.log.newBatches) &&
		it.log.newBatches[it.currNewBatchesIdx].BatchID() < it.token.EventBatchId {
		it.currNewBatchesIdx++
	}
	return it.currNewBatchesIdx < len(it.log.newBatches)
}

func (it *LogMessageBatchIterator[T]) Next() (LogMessageBatch[T], error) {
	var result LogMessageBatch[T]
	if it.currBatch != nil {
		currBatch := it.currBatch
		it.currBatch = nil
		result = make(LogMessageBatch[T], len(currBatch))
		for i, item := range currBatch {
			message, err := decodeChasmBlob[T](item.Data)
			if err != nil {
				return nil, err
			}
			result[i] = NewLogMessage(item.SequenceId, message)
		}
	} else if it.currNewBatchesIdx < len(it.log.newBatches) {
		currBatch := it.log.newBatches[it.currNewBatchesIdx]
		it.currNewBatchesIdx++
		result = make(LogMessageBatch[T], len(currBatch))
		copy(result, currBatch)
	} else {
		panic("iterator has no next value; you must check HasNext() before calling Next()")
	}
	return result, nil
}

type LogMessageIterator[T proto.Message] struct {
	ctx context.Context
	log *Log[T]

	token             *tokenspb.HistoryEventRef
	currBatch         []*persistencespb.ChasmLogMessage
	currBatchIdx      int
	currNewBatchesIdx int
}

func newLogMessageIterator[T proto.Message](
	ctx context.Context,
	log *Log[T],
	batchID int64,
) *LogMessageIterator[T] {
	return &LogMessageIterator[T]{
		ctx: ctx,
		log: log,

		token: &tokenspb.HistoryEventRef{
			EventBatchId: batchID,
		},
	}
}

func (it *LogMessageIterator[T]) HasNext() bool {
	if it.ctx.Err() != nil {
		return false
	}
	if it.currBatchIdx < len(it.currBatch) {
		return true
	}
	if it.token.EventBatchId <= it.log.metadata.GetLastSequenceId() {
		token, err := proto.Marshal(it.token)
		if err != nil {
			return false
		}
		batch, err := it.log.node.backend.LoadChasmLogMessages(it.ctx, token, it.log.metadata.GetBranch())
		if err != nil {
			return false
		}
		if len(batch.GetMessages()) == 0 {
			return false
		}
		it.currBatch = batch.Messages
		it.currBatchIdx = 0
		it.token.EventBatchId = batch.Messages[len(batch.Messages)-1].SequenceId + 1
	}
	for it.currNewBatchesIdx < len(it.log.newBatches) &&
		it.log.newBatches[it.currNewBatchesIdx].BatchID() < it.token.EventBatchId {
		it.currNewBatchesIdx++
	}
	if it.currNewBatchesIdx >= len(it.log.newBatches) {
		return false
	}
	if it.currBatchIdx < len(it.log.newBatches[it.currNewBatchesIdx]) {
		return true
	}
	it.currNewBatchesIdx++
	it.currBatchIdx = 0
	return it.currNewBatchesIdx < len(it.log.newBatches)
}

// Returns the next value of the iterator.
// It may panic if HasNext() is not called before each call to Next().
func (it *LogMessageIterator[T]) Next() (*LogMessage[T], error) {
	if it.currBatch != nil {
		if it.currBatchIdx < len(it.currBatch) {
			item := it.currBatch[it.currBatchIdx]
			it.currBatchIdx++
			message, err := decodeChasmBlob[T](item.Data)
			if err != nil {
				return nil, err
			}
			return NewLogMessage(item.SequenceId, message), nil
		}

		panic("iterator has no next value; you must check HasNext() before calling Next()")
	}

	if it.currNewBatchesIdx < len(it.log.newBatches) {
		batch := it.log.newBatches[it.currNewBatchesIdx]
		if it.currBatchIdx < len(batch) {
			message := batch[it.currBatchIdx]
			it.currBatchIdx++
			return message, nil
		}
	}

	panic("iterator has no next value; you must check HasNext() before calling Next()")
}

type LogMutation struct {
	Branch  *persistencespb.HistoryBranch
	Batches []*LogEventBatchMutation
}

type LogEventBatchMutation struct {
	TxnID     int64
	PrevTxnID int64
	Messages  []*persistencespb.ChasmLogMessage
}
