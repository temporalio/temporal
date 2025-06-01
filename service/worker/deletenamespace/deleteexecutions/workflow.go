package deleteexecutions

import (
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

const (
	WorkflowName = "temporal-sys-delete-executions-workflow"
	StatsQuery   = "stats"
)

type (
	DeleteExecutionsParams struct {
		Namespace   namespace.Name
		NamespaceID namespace.ID
		Config      DeleteExecutionsConfig

		// Number of Workflow Executions to delete. Used in statistics computation.
		// If not specified, then statistics are partially computed.
		TotalExecutionsCount int

		// To carry over progress results with ContinueAsNew.
		PreviousSuccessCount int
		PreviousErrorCount   int
		ContinueAsNewCount   int
		NextPageToken        []byte
		// Time when the first run (in a chain of CANs) of DeleteExecutionsWorkflow has started.
		FirstRunStartTime time.Time
	}

	DeleteExecutionsResult struct {
		SuccessCount int
		ErrorCount   int
	}

	DeleteExecutionsStats struct {
		DeleteExecutionsResult
		ContinueAsNewCount       int
		TotalExecutionsCount     int
		RemainingExecutionsCount int
		AverageRPS               int
		StartTime                time.Time
		ApproximateTimeLeft      time.Duration
		ApproximateEndTime       time.Time
	}
)

var (
	retryPolicy = &temporal.RetryPolicy{
		InitialInterval: 1 * time.Second,
		MaximumInterval: 10 * time.Second,
	}

	localActivityOptions = workflow.LocalActivityOptions{
		RetryPolicy:            retryPolicy,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
	}

	deleteWorkflowExecutionsActivityOptions = workflow.ActivityOptions{
		RetryPolicy:         retryPolicy,
		StartToCloseTimeout: 60 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
	}
)

func validateParams(ctx workflow.Context, params *DeleteExecutionsParams) error {
	if params.NamespaceID.IsEmpty() {
		return errors.NewInvalidArgument("namespace ID is required", nil)
	}
	if params.Namespace.IsEmpty() {
		return errors.NewInvalidArgument("namespace is required", nil)
	}
	if params.FirstRunStartTime.IsZero() {
		params.FirstRunStartTime = workflow.Now(ctx).UTC()
	}
	params.Config.ApplyDefaults()
	return nil
}

func DeleteExecutionsWorkflow(ctx workflow.Context, params DeleteExecutionsParams) (DeleteExecutionsResult, error) {
	logger := log.With(
		workflow.GetLogger(ctx),
		tag.WorkflowType(WorkflowName),
		tag.WorkflowNamespace(params.Namespace.String()),
		tag.WorkflowNamespaceID(params.NamespaceID.String()))

	logger.Info("Workflow started.")
	result := DeleteExecutionsResult{
		SuccessCount: params.PreviousSuccessCount,
		ErrorCount:   params.PreviousErrorCount,
	}

	if err := validateParams(ctx, &params); err != nil {
		return result, err
	}
	logger.Info("Effective config.", tag.Value(params.Config.String()))

	if err := workflow.SetQueryHandler(ctx, StatsQuery, func() (DeleteExecutionsStats, error) {
		now := workflow.Now(ctx).UTC()
		des := DeleteExecutionsStats{
			DeleteExecutionsResult: result,
			ContinueAsNewCount:     params.ContinueAsNewCount,
			TotalExecutionsCount:   params.TotalExecutionsCount,
			StartTime:              params.FirstRunStartTime,
		}
		if params.TotalExecutionsCount > 0 {
			des.RemainingExecutionsCount = params.TotalExecutionsCount - (result.SuccessCount + result.ErrorCount)
		}
		secondsSinceStart := int(now.Sub(params.FirstRunStartTime).Seconds())
		if secondsSinceStart > 0 {
			des.AverageRPS = (result.SuccessCount + result.ErrorCount) / secondsSinceStart
		}
		if des.AverageRPS > 0 {
			des.ApproximateTimeLeft = time.Duration(des.RemainingExecutionsCount/des.AverageRPS) * time.Second
		}
		des.ApproximateEndTime = now.Add(des.ApproximateTimeLeft)
		return des, nil
	}); err != nil {
		return result, err
	}

	var a *Activities
	var la *LocalActivities

	ctx = workflow.WithTaskQueue(ctx, primitives.DeleteNamespaceActivityTQ)

	nextPageToken := params.NextPageToken
	runningDeleteExecutionsActivityCount := 0
	runningDeleteExecutionsSelector := workflow.NewSelector(ctx)
	var lastDeleteExecutionsActivityErr error

	// Two activities DeleteExecutionsActivity and GetNextPageTokenActivity are executed here essentially in reverse order
	// because Get is called immediately for GetNextPageTokenActivity but not for DeleteExecutionsActivity.
	// These activities scan visibility storage independently but GetNextPageTokenActivity considered to be quick and can be done synchronously.
	// It reads nextPageToken and pass it DeleteExecutionsActivity. This allocates block of workflow executions to delete for
	// DeleteExecutionsActivity which takes much longer to complete. This is why this workflow starts
	// ConcurrentDeleteExecutionsActivities number of them and executes them concurrently on available workers.
	for i := 0; i < params.Config.PagesPerExecution; i++ {
		ctx1 := workflow.WithActivityOptions(ctx, deleteWorkflowExecutionsActivityOptions)
		deleteExecutionsFuture := workflow.ExecuteActivity(ctx1, a.DeleteExecutionsActivity, &DeleteExecutionsActivityParams{
			Namespace:     params.Namespace,
			NamespaceID:   params.NamespaceID,
			RPS:           params.Config.DeleteActivityRPS,
			ListPageSize:  params.Config.PageSize,
			NextPageToken: nextPageToken,
		})

		ctx2 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
		err := workflow.ExecuteLocalActivity(ctx2, la.GetNextPageTokenActivity, GetNextPageTokenParams{
			NamespaceID:   params.NamespaceID,
			Namespace:     params.Namespace,
			PageSize:      params.Config.PageSize,
			NextPageToken: nextPageToken,
		}).Get(ctx, &nextPageToken)
		if err != nil {
			return result, err
		}

		runningDeleteExecutionsActivityCount++
		runningDeleteExecutionsSelector.AddFuture(deleteExecutionsFuture, func(f workflow.Future) {
			runningDeleteExecutionsActivityCount--
			var der DeleteExecutionsActivityResult
			deErr := f.Get(ctx, &der)
			if deErr != nil {
				lastDeleteExecutionsActivityErr = deErr
				return
			}
			result.SuccessCount += der.SuccessCount
			result.ErrorCount += der.ErrorCount
		})

		if runningDeleteExecutionsActivityCount >= params.Config.ConcurrentDeleteExecutionsActivities {
			// Wait for one of running activities to complete.
			runningDeleteExecutionsSelector.Select(ctx)
			if lastDeleteExecutionsActivityErr != nil {
				return result, lastDeleteExecutionsActivityErr
			}
		}

		if nextPageToken == nil {
			break
		}
	}

	// Wait for all running activities to complete.
	for runningDeleteExecutionsActivityCount > 0 {
		runningDeleteExecutionsSelector.Select(ctx)
		if lastDeleteExecutionsActivityErr != nil {
			return result, lastDeleteExecutionsActivityErr
		}
	}

	// If nextPageToken is nil then there are no more workflow executions to delete.
	if nextPageToken == nil {
		if result.ErrorCount == 0 {
			logger.Info("Successfully deleted workflow executions.", tag.DeletedExecutionsCount(result.SuccessCount))
		} else {
			logger.Error("Finish deleting workflow executions with some errors.", tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount))
		}
		return result, nil
	}

	// Too many workflow executions, and ConcurrentDeleteExecutionsActivities number of activities has been completed already.
	// Continue as new to prevent workflow history size explosion.

	params.PreviousSuccessCount = result.SuccessCount
	params.PreviousErrorCount = result.ErrorCount
	params.ContinueAsNewCount++
	params.NextPageToken = nextPageToken

	logger.Info("There are more workflows to delete. Continuing workflow as new.", tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount), tag.Counter(params.ContinueAsNewCount))
	return result, workflow.NewContinueAsNewError(ctx, DeleteExecutionsWorkflow, params)
}
