// Task executors.
package worker

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

const (
	// Maximum retries for rescheduling a single activity.
	maxRescheduleRetries = 3
	// Timeout for each reschedule RPC call.
	rescheduleTimeout = 5 * time.Second
)

// RescheduleErrorType categorizes reschedule errors for metrics.
type RescheduleErrorType string

const (
	RescheduleErrorRetry     RescheduleErrorType = "retry"
	RescheduleErrorPermanent RescheduleErrorType = "permanent"
	RescheduleErrorSkipped   RescheduleErrorType = "skipped" // workflow/activity no longer exists
)

// workerIDTag returns a log tag for the worker ID.
func workerIDTag(workerID string) tag.ZapTag {
	return tag.NewStringTag("worker-id", workerID)
}

// HistoryClient is the interface for history service operations needed for rescheduling.
// This is a subset of historyservice.HistoryServiceClient.
type HistoryClient interface {
	ForceRescheduleActivity(ctx context.Context, in *historyservice.ForceRescheduleActivityRequest, opts ...grpc.CallOption) (*historyservice.ForceRescheduleActivityResponse, error)
}

// LeaseExpiryTaskExecutor handles lease expiry events.
type LeaseExpiryTaskExecutor struct {
	logger            log.Logger
	metricsHandler    metrics.Handler
	historyClient     HistoryClient
	namespaceRegistry namespace.Registry
}

func NewLeaseExpiryTaskExecutor(
	logger log.Logger,
	metricsHandler metrics.Handler,
	historyClient HistoryClient,
	namespaceRegistry namespace.Registry,
) *LeaseExpiryTaskExecutor {
	return &LeaseExpiryTaskExecutor{
		logger:            logger,
		metricsHandler:    metricsHandler,
		historyClient:     historyClient,
		namespaceRegistry: namespaceRegistry,
	}
}

// Execute is called when a lease expiry timer fires.
// Transitions the worker to INACTIVE (terminal state) and reschedules all activities.
func (e *LeaseExpiryTaskExecutor) Execute(
	ctx chasm.MutableContext,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.LeaseExpiryTask,
) error {
	workerID := worker.workerID()

	// Debug: log heartbeat state
	heartbeat := worker.GetWorkerHeartbeat()
	activityInfo := heartbeat.GetActivityInfo()
	activities := activityInfo.GetRunningActivities()

	e.logger.Info("worker_chasm: Worker lease expired, marking as inactive and rescheduling activities",
		workerIDTag(workerID),
		tag.NewBoolTag("has_heartbeat", heartbeat != nil),
		tag.NewBoolTag("has_activity_info", activityInfo != nil),
		tag.NewInt("activity_count", len(activities)))

	namespaceID := ctx.ExecutionKey().NamespaceID

	// Transition to inactive state
	if err := TransitionLeaseExpired.Apply(worker, ctx, EventLeaseExpired{}); err != nil {
		return err
	}

	// Reschedule activities (best effort, don't fail the task)
	if len(activities) > 0 {
		e.rescheduleActivities(namespaceID, workerID, activities)
	} else {
		e.logger.Info("worker_chasm: No activities to reschedule for expired worker",
			workerIDTag(workerID))
	}

	return nil
}

// rescheduleActivities attempts to reschedule all activities for a dead worker.
func (e *LeaseExpiryTaskExecutor) rescheduleActivities(
	namespaceID string,
	workerID string,
	activities []*workerpb.ActivityBinding,
) {
	if e.historyClient == nil {
		e.logger.Warn("worker_chasm: History client not configured, skipping activity rescheduling",
			workerIDTag(workerID),
			tag.NewInt("activity_count", len(activities)))
		return
	}

	e.logger.Info("worker_chasm: Rescheduling activities for dead worker",
		workerIDTag(workerID),
		tag.NewInt("activity_count", len(activities)))

	for _, activity := range activities {
		e.rescheduleActivity(namespaceID, workerID, activity)
	}
}

// isNonRetryableRescheduleError checks if an error from ForceRescheduleActivity
// indicates a permanent condition that should not be retried.
func isNonRetryableRescheduleError(err error) bool {
	// Workflow or activity no longer exists - nothing to reschedule
	if _, ok := err.(*serviceerror.NotFound); ok {
		return true
	}
	// Workflow already completed - activity cannot be rescheduled
	if _, ok := err.(*serviceerror.FailedPrecondition); ok {
		return true
	}
	// Invalid request - won't succeed on retry
	if _, ok := err.(*serviceerror.InvalidArgument); ok {
		return true
	}
	return false
}

// rescheduleActivity attempts to reschedule a single activity with retries.
func (e *LeaseExpiryTaskExecutor) rescheduleActivity(
	namespaceID string,
	workerID string,
	activity *workerpb.ActivityBinding,
) {
	e.logger.Info("worker_chasm: Attempting to reschedule activity",
		workerIDTag(workerID),
		tag.WorkflowID(activity.GetWorkflowId()),
		tag.WorkflowRunID(activity.GetRunId()),
		tag.ActivityID(activity.GetActivityId()))

	var lastErr error
	for attempt := 1; attempt <= maxRescheduleRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), rescheduleTimeout)
		_, err := e.historyClient.ForceRescheduleActivity(ctx, &historyservice.ForceRescheduleActivityRequest{
			NamespaceId: namespaceID,
			WorkflowId:  activity.GetWorkflowId(),
			RunId:       activity.GetRunId(),
			ActivityId:  activity.GetActivityId(),
		})
		cancel()

		if err == nil {
			metrics.ChasmWorkerActivitiesRescheduled.With(e.metricsHandler).Record(1)
			e.logger.Info("worker_chasm: Activity rescheduled successfully",
				workerIDTag(workerID),
				tag.WorkflowID(activity.GetWorkflowId()),
				tag.ActivityID(activity.GetActivityId()))
			return
		}

		// Don't retry permanent errors
		if isNonRetryableRescheduleError(err) {
			metrics.ChasmWorkerRescheduleErrors.With(e.metricsHandler).Record(1,
				metrics.StringTag("error_type", string(RescheduleErrorSkipped)))
			e.logger.Info("worker_chasm: Activity reschedule skipped (workflow/activity no longer exists)",
				workerIDTag(workerID),
				tag.WorkflowID(activity.GetWorkflowId()),
				tag.ActivityID(activity.GetActivityId()),
				tag.Error(err))
			return
		}

		lastErr = err
		e.logger.Warn("worker_chasm: Failed to reschedule activity, retrying",
			workerIDTag(workerID),
			tag.WorkflowID(activity.GetWorkflowId()),
			tag.ActivityID(activity.GetActivityId()),
			tag.Attempt(int32(attempt)),
			tag.Error(err))

		metrics.ChasmWorkerRescheduleErrors.With(e.metricsHandler).Record(1,
			metrics.StringTag("error_type", string(RescheduleErrorRetry)))
	}

	// Exhausted retries - permanent failure
	metrics.ChasmWorkerRescheduleErrors.With(e.metricsHandler).Record(1,
		metrics.StringTag("error_type", string(RescheduleErrorPermanent)))
	e.logger.Error("worker_chasm: Failed to reschedule activity after retries",
		workerIDTag(workerID),
		tag.WorkflowID(activity.GetWorkflowId()),
		tag.ActivityID(activity.GetActivityId()),
		tag.Error(lastErr))
}

// Validate checks if the lease expiry task is still valid (implements TaskValidator interface).
func (e *LeaseExpiryTaskExecutor) Validate(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.LeaseExpiryTask,
) (bool, error) {
	valid, reason := worker.isLeaseExpiryTaskValid(attrs)
	if !valid && reason != "" {
		e.logger.Error("worker_chasm: "+reason, workerIDTag(worker.workerID()))
	}
	return valid, nil
}
