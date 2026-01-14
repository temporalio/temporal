// Task executors.
package worker

import (
	"context"
	"time"

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
	e.logger.Info("Worker lease expired, marking as inactive and rescheduling activities",
		workerIDTag(workerID))

	// Get activities to reschedule before transitioning to inactive
	activities := worker.GetWorkerHeartbeat().GetActivityInfo().GetRunningActivities()
	namespaceID := ctx.ExecutionKey().NamespaceID

	// Transition to inactive state
	if err := TransitionLeaseExpired.Apply(worker, ctx, EventLeaseExpired{}); err != nil {
		return err
	}

	// Reschedule activities (best effort, don't fail the task)
	if len(activities) > 0 {
		e.rescheduleActivities(namespaceID, workerID, activities)
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
		e.logger.Warn("History client not configured, skipping activity rescheduling",
			workerIDTag(workerID),
			tag.NewInt("activity_count", len(activities)))
		return
	}

	e.logger.Info("Rescheduling activities for dead worker",
		workerIDTag(workerID),
		tag.NewInt("activity_count", len(activities)))

	for _, activity := range activities {
		e.rescheduleActivity(namespaceID, workerID, activity)
	}
}

// rescheduleActivity attempts to reschedule a single activity with retries.
func (e *LeaseExpiryTaskExecutor) rescheduleActivity(
	namespaceID string,
	workerID string,
	activity *workerpb.ActivityBinding,
) {
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
			e.logger.Debug("Activity rescheduled successfully",
				workerIDTag(workerID),
				tag.WorkflowID(activity.GetWorkflowId()),
				tag.ActivityID(activity.GetActivityId()))
			return
		}

		lastErr = err
		e.logger.Warn("Failed to reschedule activity, retrying",
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
	e.logger.Error("Failed to reschedule activity after retries",
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
		e.logger.Error(reason, workerIDTag(worker.workerID()))
	}
	return valid, nil
}
