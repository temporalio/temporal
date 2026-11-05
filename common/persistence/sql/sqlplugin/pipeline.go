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
	// ErrPipelineNotSent is returned by a PipelinedTx's Commit, wrapping the
	// driver error, when it sent nothing.
	ErrPipelineNotSent = errors.New("pipelined transaction sent nothing")

	// A PipelinedTx's Commit returns one of these errors, wrapping the database
	// error, when the check that stands for it fails.
	ErrShardRangeIDMismatch          = errors.New("shard does not have the range ID")
	ErrCurrentExecutionMissing       = errors.New("current execution does not exist")
	ErrCurrentExecutionRunIDMismatch = errors.New("current execution has another run ID")
	ErrExecutionMissing              = errors.New("execution does not exist")
	ErrExecutionConditionMismatch    = errors.New("execution does not match the condition")
)

type (
	// PipelinedTx is a transaction that queues its statements until Commit sends
	// them as one implicit transaction. Its reads return no results, so the checks
	// a caller would make on them are statements that fail the transaction instead.
	// Commit returns the error of the check that failed, or ErrPipelineNotSent when
	// it sent nothing. Any other Commit error leaves the outcome unknown.
	PipelinedTx interface {
		Tx
		// UpdateExecutionsWithCondition fails the transaction with
		// ErrExecutionMissing when the execution does not exist, and with
		// ErrExecutionConditionMismatch when it does not match the condition.
		HistoryExecutionConditionalUpdater
		// ReadLockShardsWithRangeID fails the transaction with ErrShardRangeIDMismatch
		// unless the shard has rangeID.
		ReadLockShardsWithRangeID(ctx context.Context, filter ShardsFilter, rangeID int64) error
		// LockCurrentExecutionsWithRunID fails the transaction with
		// ErrCurrentExecutionMissing when the current execution does not exist, and
		// with ErrCurrentExecutionRunIDMismatch unless it has filter.RunID.
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
