package batcher

import (
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// InfiniteDuration is a long duration (20 yrs) we use for "infinite" workflows
	infiniteDuration                = 20 * 365 * 24 * time.Hour
	defaultAttemptsOnRetryableError = 50
	defaultActivityHeartBeatTimeout = time.Second * 10
)

const (
	// BatchOperationTypeMemo stores batch operation type in memo
	BatchOperationTypeMemo = "batch_operation_type"
	// BatchReasonMemo stores batch operation reason in memo
	BatchReasonMemo = "batch_operation_reason"
	// BatchOperationStatsMemo stores batch operation stats in memo
	BatchOperationStatsMemo = "batch_operation_stats"
	// BatchTypeTerminate is batch type for terminating workflows
	BatchTypeTerminate = "terminate"
	// BatchTypeCancel is the batch type for canceling workflows
	BatchTypeCancel = "cancel"
	// BatchTypeSignal is batch type for signaling workflows
	BatchTypeSignal = "signal"
	// BatchTypeDelete is batch type for deleting workflows
	BatchTypeDelete = "delete"
	// BatchTypeReset is batch type for resetting workflows
	BatchTypeReset = "reset"
	// BatchTypeUpdateOptions is batch type for updating the options of workflow executions
	BatchTypeUpdateOptions = "update_options"
	// BatchTypeUnpauseActivities is batch type for unpausing activities
	BatchTypeUnpauseActivities = "unpause_activities"
	// BatchTypeUpdateActivitiesOptions is batch type for updating the options of activities
	BatchTypeUpdateActivitiesOptions = "update_activity_options"
	// BatchTypeResetActivities is batch type for resetting activities
	BatchTypeResetActivities = "reset_activities"
)

var (
	OpenBatchOperationQuery = fmt.Sprintf("%s = '%s' AND %s = %d",
		sadefs.TemporalNamespaceDivision,
		NamespaceDivision,
		sadefs.ExecutionStatus,
		int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
	)
)

type (
	// HeartBeatDetails is the struct for heartbeat details
	HeartBeatDetails struct {
		PageToken   []byte
		CurrentPage int
		// This is just an estimation for visibility
		TotalEstimate int64
		// Number of workflows processed successfully
		SuccessCount int
		// Number of workflows that give up due to errors.
		ErrorCount int
	}

	task struct {
		// the workflow execution to process
		execution *commonpb.WorkflowExecution
		// the number of attempts to process the workflow execution
		attempts int
		// reference to the page this task belongs to (for tracking page completion)
		page *page
	}

	taskResponse struct {
		// the error result from processing the task (nil for success)
		err error
		// reference to the page the completed task belonged to
		page *page
	}
)

var (
	batchActivityRetryPolicy = temporal.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 1.7,
		MaximumInterval:    5 * time.Minute,
	}

	batchActivityOptions = workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Minute,
		StartToCloseTimeout:    infiniteDuration,
		RetryPolicy:            &batchActivityRetryPolicy,
	}
)

// BatchWorkflowProtobuf is the workflow that runs a batch job of resetting workflows.
func BatchWorkflowProtobuf(ctx workflow.Context, batchParams *batchspb.BatchOperationInput) (HeartBeatDetails, error) {
	if batchParams == nil {
		return HeartBeatDetails{}, errors.New("batchParams is nil")
	}

	batchParams = setDefaultParams(batchParams)
	batchActivityOptions.HeartbeatTimeout = batchParams.ActivityHeartbeatTimeout.AsDuration()
	opt := workflow.WithActivityOptions(ctx, batchActivityOptions)
	var result HeartBeatDetails
	var ac *activities
	err := workflow.ExecuteActivity(opt, ac.BatchActivityWithProtobuf, batchParams).Get(ctx, &result)
	if err != nil {
		return HeartBeatDetails{}, err
	}

	err = attachBatchOperationStats(ctx, result)
	if err != nil {
		return HeartBeatDetails{}, err
	}
	return result, err
}

type BatchOperationStats struct {
	NumSuccess int
	NumFailure int
}

// attachBatchOperationStats attaches statistics on the number of
// individual successes and failures to the memo of this workflow.
func attachBatchOperationStats(ctx workflow.Context, result HeartBeatDetails) error {
	memo := map[string]interface{}{
		BatchOperationStatsMemo: BatchOperationStats{
			NumSuccess: result.SuccessCount,
			NumFailure: result.ErrorCount,
		},
	}
	return workflow.UpsertMemo(ctx, memo)
}

func setDefaultParams(params *batchspb.BatchOperationInput) *batchspb.BatchOperationInput {
	if params.GetAttemptsOnRetryableError() <= 1 {
		params.AttemptsOnRetryableError = defaultAttemptsOnRetryableError
	}
	if params.GetActivityHeartbeatTimeout().AsDuration() <= 0 {
		params.ActivityHeartbeatTimeout = &durationpb.Duration{
			Seconds: int64(defaultActivityHeartBeatTimeout / time.Second),
		}
	}
	return params
}
