package sqlplugin

import (
	"context"
	"database/sql"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/primitives"
)

type (
	// ExecutionsRow represents a row in executions table
	ExecutionsRow struct {
		ShardID          int32
		NamespaceID      primitives.UUID
		WorkflowID       string
		RunID            primitives.UUID
		NextEventID      int64
		LastWriteVersion int64
		Data             []byte
		DataEncoding     string
		State            []byte
		StateEncoding    string
		DBRecordVersion  int64
	}

	// ExecutionsFilter contains the column names within executions table that
	// can be used to filter results through a WHERE clause
	ExecutionsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// CurrentExecutionsRow represents a row in current_executions table
	CurrentExecutionsRow struct {
		ShardID          int32
		NamespaceID      primitives.UUID
		WorkflowID       string
		RunID            primitives.UUID
		CreateRequestID  string
		StartTime        *time.Time
		LastWriteVersion int64
		State            enumsspb.WorkflowExecutionState
		Status           enumspb.WorkflowExecutionStatus
		Data             []byte
		DataEncoding     string
	}

	// CurrentExecutionsFilter contains the column names within current_executions table that
	// can be used to filter results through a WHERE clause
	CurrentExecutionsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// TODO remove this block in 1.12.x
	ExecutionVersion struct {
		DBRecordVersion int64
		NextEventID     int64
	}

	// HistoryExecution is the SQL persistence interface for history executions
	HistoryExecution interface {
		InsertIntoExecutions(ctx context.Context, row *ExecutionsRow) (sql.Result, error)
		UpdateExecutions(ctx context.Context, row *ExecutionsRow) (sql.Result, error)
		SelectFromExecutions(ctx context.Context, filter ExecutionsFilter) (*ExecutionsRow, error)
		DeleteFromExecutions(ctx context.Context, filter ExecutionsFilter) (sql.Result, error)
		ReadLockExecutions(ctx context.Context, filter ExecutionsFilter) (int64, int64, error)
		WriteLockExecutions(ctx context.Context, filter ExecutionsFilter) (int64, int64, error)

		LockCurrentExecutionsJoinExecutions(ctx context.Context, filter CurrentExecutionsFilter) ([]CurrentExecutionsRow, error)

		InsertIntoCurrentExecutions(ctx context.Context, row *CurrentExecutionsRow) (sql.Result, error)
		UpdateCurrentExecutions(ctx context.Context, row *CurrentExecutionsRow) (sql.Result, error)
		// SelectFromCurrentExecutions returns one or more rows from current_executions table
		// Required params - {shardID, namespaceID, workflowID}
		SelectFromCurrentExecutions(ctx context.Context, filter CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
		// DeleteFromCurrentExecutions deletes a single row that matches the filter criteria
		// If a row exist, that row will be deleted and this method will return success
		// If there is no row matching the filter criteria, this method will still return success
		// Callers can check the output of Result.RowsAffected() to see if a row was deleted or not
		// Required params - {shardID, namespaceID, workflowID, runID}
		DeleteFromCurrentExecutions(ctx context.Context, filter CurrentExecutionsFilter) (sql.Result, error)
		LockCurrentExecutions(ctx context.Context, filter CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
	}
)
