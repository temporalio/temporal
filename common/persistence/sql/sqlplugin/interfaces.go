//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interfaces_mock.go

package sqlplugin

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resolver"
)

type (
	DbKind int
)

const (
	DbKindUnknown DbKind = iota
	DbKindMain
	DbKindVisibility
)

var (
	// ErrNoPipelinedResults is returned by PipelinedTx reads and by the results of
	// its queued statements.
	ErrNoPipelinedResults = errors.New("pipelined statements return no results")
	// ErrPipelineUnsupported is returned when the plugin or its connection cannot pipeline.
	ErrPipelineUnsupported = errors.New("pipelined transactions are not supported")
	// ErrPipelineRejected wraps the database error a PipelinedTx statement
	// returned. Nothing the transaction queued was written.
	ErrPipelineRejected = errors.New("pipelined transaction rejected")
)

type VersionedBlob struct {
	Version      int64
	Data         []byte
	DataEncoding string
}

type (
	// Plugin defines the interface for any SQL database that needs to implement
	Plugin interface {
		CreateDB(dbKind DbKind, cfg *config.SQL, r resolver.ServiceResolver, l log.Logger, mh metrics.Handler) (GenericDB, error)

		GetVisibilityQueryConverter() VisibilityQueryConverter
	}

	// TableCRUD defines the API for interacting with the database tables
	TableCRUD interface {
		ClusterMetadata
		Namespace
		Visibility
		QueueMessage
		QueueMetadata
		QueueV2Message
		QueueV2Metadata

		MatchingTask
		MatchingTaskV2
		MatchingTaskQueue
		MatchingTaskQueueUserData

		NexusEndpoints

		HistoryNode
		HistoryTree

		HistoryShard

		HistoryExecution
		HistoryExecutionBuffer
		HistoryExecutionActivity
		HistoryExecutionChildWorkflow
		HistoryExecutionTimer
		HistoryExecutionRequestCancel
		HistoryExecutionSignal
		HistoryExecutionSignalRequest
		HistoryExecutionChasm

		HistoryImmediateTask
		HistoryScheduledTask
		HistoryTransferTask
		HistoryTimerTask
		HistoryReplicationTask
		HistoryReplicationDLQTask
		HistoryVisibilityTask
	}

	// AdminCRUD defines admin operations for CLI and test suites
	AdminCRUD interface {
		CreateSchemaVersionTables() error
		ReadSchemaVersion(database string) (string, error)
		UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error
		WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error
		ListTables(database string) ([]string, error)
		DropTable(table string) error
		DropAllTables(database string) error
		CreateDatabase(database string) error
		DropDatabase(database string) error
		Exec(stmt string, args ...any) error
	}

	// Tx defines the API for a SQL transaction
	Tx interface {
		TableCRUD
		Commit() error
		Rollback() error
	}

	// PipelinedTx is a transaction that queues its statements until Commit sends
	// them in one round trip, as one implicit transaction. Its reads return no
	// results, so the checks a caller would make on them are statements that fail
	// the transaction instead, and its UpdateExecutionsWithCondition fails the
	// transaction when it updates no row. Commit returns ErrPipelineRejected when a
	// statement failed. Any other Commit error leaves the outcome unknown.
	PipelinedTx interface {
		Tx
		HistoryExecutionConditionalUpdater
		HistoryCurrentExecutionConditionalUpdater
		// ReadLockShardsWithRangeID fails the transaction unless the shard has rangeID.
		ReadLockShardsWithRangeID(ctx context.Context, filter ShardsFilter, rangeID int64) error
		// LockCurrentExecutionsWithRunID fails the transaction unless the current
		// execution has filter.RunID.
		LockCurrentExecutionsWithRunID(ctx context.Context, filter CurrentExecutionsFilter) error
	}

	// TxPipeliner lets SQL plugins run a transaction as a PipelinedTx.
	TxPipeliner interface {
		BeginPipelinedTx(ctx context.Context) (PipelinedTx, error)
	}

	// DB defines the API for regular SQL operations of a Temporal server
	DB interface {
		TableCRUD
		GenericDB
		BeginTx(ctx context.Context) (Tx, error)
		IsDupEntryError(err error) bool
	}

	// AdminDB defines the API for admin SQL operations for CLI and testing suites
	AdminDB interface {
		AdminCRUD
		GenericDB
		ExpectedVersion() string
		VerifyVersion() error
	}

	// SchemaStatementRewriter lets SQL plugins adjust schema statements before execution.
	SchemaStatementRewriter interface {
		RewriteSchemaStatements(statements []string) []string
	}

	GenericDB interface {
		DbName() string
		PluginName() string
		Close() error
	}

	// Conn defines the API for a single database connection
	Conn interface {
		Rebind(query string) string
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
		GetContext(ctx context.Context, dest any, query string, args ...any) error
		SelectContext(ctx context.Context, dest any, query string, args ...any) error
		PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	}
)

func (k DbKind) String() string {
	switch k {
	case DbKindMain:
		return "main"
	case DbKindVisibility:
		return "visibility"
	default:
		return "unknown"
	}
}
