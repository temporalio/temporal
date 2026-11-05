package sqlplugin

import (
	"context"
	"errors"
)

var (
	// ErrNoPipelinedResults is returned by PipelinedTx reads and by the results of
	// its queued statements.
	ErrNoPipelinedResults = errors.New("pipelined statements return no results")
	// ErrPipelineUnsupported is returned when the plugin cannot pipeline.
	ErrPipelineUnsupported = errors.New("pipelined transactions are not supported")
	// ErrPipelineNotWritten wraps the error that stopped a PipelinedTx's Commit
	// before anything the transaction queued was written.
	ErrPipelineNotWritten = errors.New("pipelined transaction not written")
)

type (
	// PipelinedTx is a transaction that queues its statements until Commit sends
	// them as one implicit transaction, in one round trip once the connection has
	// prepared them. Its reads return no results, so the checks a caller would make
	// on them are statements that fail the transaction instead, and its
	// UpdateExecutionsWithCondition fails the transaction when it updates no row.
	// Commit returns ErrPipelineNotWritten only when nothing it queued was
	// written. Any other Commit error leaves the outcome unknown.
	PipelinedTx interface {
		Tx
		HistoryExecutionConditionalUpdater
		// ReadLockShardsWithRangeID fails the transaction unless the shard has rangeID.
		ReadLockShardsWithRangeID(ctx context.Context, filter ShardsFilter, rangeID int64) error
		// LockCurrentExecutionsWithRunID fails the transaction unless the current
		// execution has filter.RunID.
		LockCurrentExecutionsWithRunID(ctx context.Context, filter CurrentExecutionsFilter) error
		// UpdateCurrentExecutionsIfChanged updates the current execution only when
		// it differs from row.
		UpdateCurrentExecutionsIfChanged(ctx context.Context, row *CurrentExecutionsRow) error
	}

	// TxPipeliner lets SQL plugins run a transaction as a PipelinedTx.
	TxPipeliner interface {
		BeginPipelinedTx(ctx context.Context) (PipelinedTx, error)
	}
)
