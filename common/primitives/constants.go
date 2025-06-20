package primitives

import (
	"time"

	"go.temporal.io/server/common/debug"
)

const (
	// DefaultTransactionSizeLimit is the largest allowed transaction size to persistence
	DefaultTransactionSizeLimit = 4 * 1024 * 1024
)

const (
	// DefaultWorkflowTaskTimeout sets the Default Workflow Task timeout for a Workflow
	DefaultWorkflowTaskTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

const (
	// GetHistoryMaxPageSize is the max page size for get history
	GetHistoryMaxPageSize = 256
	// ReadDLQMessagesPageSize is the max page size for read DLQ messages
	ReadDLQMessagesPageSize = 1000
)

const (
	DefaultHistoryMaxAutoResetPoints = 20
)

const (
	ScheduleWorkflowIDPrefix = "temporal-sys-scheduler:"
)
