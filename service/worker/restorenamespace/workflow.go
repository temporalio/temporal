package restorenamespace

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName = "temporal-sys-restore-namespace"

	// RefreshExecutionsActivityName is the name the refresh activity is registered under.
	// DescribeNamespaceRestore reads its heartbeat for progress.
	RefreshExecutionsActivityName = "RefreshExecutions"
	CompleteRestoreActivityName   = "CompleteRestore"

	// StatsMemoKey holds the final RefreshStats once the refresh has finished.
	StatsMemoKey = "RestoreStats"

	refreshHeartbeatTimeout = time.Minute
	infiniteDuration        = 20 * 365 * 24 * time.Hour
)

type (
	RestoreJobInput struct {
		NamespaceID string
		RestoreID   string
		// The namespace's ConfigVersion once the restore was written. Every refresh requires the
		// history host to have the namespace REGISTERED at this version or later.
		RestoreVersion int64
	}

	RefreshStats struct {
		TotalEstimate  int64
		ProcessedCount int64
		// Executions that at least one cluster can't refresh, such as one whose mutable state it
		// can't read. They are included in ProcessedCount.
		SkippedCount     int64
		RefreshStartTime time.Time
	}

	// RefreshHeartbeat is the refresh activity's checkpoint. A retried activity starts again from
	// the page it was on.
	RefreshHeartbeat struct {
		RefreshStats
		// Token of the page being refreshed.
		PageToken []byte
		// Set once the last page is done.
		ListingDone bool
	}
)

// WorkflowID is the restore job's workflow ID in temporal-system. It is unique per restore, so a
// repeated RestoreNamespace call finds the same job.
func WorkflowID(namespaceID string, restoreID string) string {
	return fmt.Sprintf("%s/%s/%s", WorkflowName, namespaceID, restoreID)
}

// RestoreNamespaceWorkflow refreshes every execution of a restored namespace, then marks the restore
// COMPLETED. It makes no calls itself. If the restore is abandoned, superseded, or loses its active
// cluster, the refresh activity fails without retrying and the restore record already says so.
func RestoreNamespaceWorkflow(ctx workflow.Context, input RestoreJobInput) (RefreshStats, error) {
	refreshCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: infiniteDuration,
		HeartbeatTimeout:    refreshHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 1.7,
			MaximumInterval:    5 * time.Minute,
		},
	})
	var stats RefreshStats
	if err := workflow.ExecuteActivity(refreshCtx, RefreshExecutionsActivityName, input).Get(ctx, &stats); err != nil {
		return stats, err
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{StatsMemoKey: stats}); err != nil {
		return stats, err
	}

	completeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
	return stats, workflow.ExecuteActivity(completeCtx, CompleteRestoreActivityName, input).Get(ctx, nil)
}
