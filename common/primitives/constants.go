package primitives

import (
	"strings"
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
	internalWorkflowIDPrefix = "temporal-sys-"
)

// IsInternalWorkflowID returns true if the workflow ID uses the prefix Temporal gives the workflows its system
// workers run, such as schedules and worker deployments. The prefix is not enforced for customer workflow IDs.
func IsInternalWorkflowID(workflowID string) bool {
	return strings.HasPrefix(workflowID, internalWorkflowIDPrefix)
}
