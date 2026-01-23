package batcher

import (
	"errors"
	"fmt"
	"time"

	batchpb "go.temporal.io/api/batch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
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

	OpenAdminBatchOperationQuery = fmt.Sprintf("%s = '%s' AND %s = %d",
		sadefs.TemporalNamespaceDivision,
		AdminNamespaceDivision,
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
		executionInfo *workflowpb.WorkflowExecutionInfo
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

// nolint:revive,cognitive-complexity
func ValidateBatchOperation(params *workflowservice.StartBatchOperationRequest) error {
	if params.GetOperation() == nil ||
		params.GetReason() == "" ||
		params.GetNamespace() == "" ||
		(params.GetVisibilityQuery() == "" && len(params.GetExecutions()) == 0) {
		return serviceerror.NewInvalidArgument("must provide required parameters: BatchType/Reason/Namespace/Query/Executions")
	}

	if len(params.GetJobId()) == 0 {
		return serviceerror.NewInvalidArgument("JobId is not set on request.")
	}
	if len(params.GetNamespace()) == 0 {
		return serviceerror.NewInvalidArgument("Namespace is not set on request.")
	}
	if len(params.GetVisibilityQuery()) == 0 && len(params.GetExecutions()) == 0 {
		return serviceerror.NewInvalidArgument("VisibilityQuery or Executions must be set on request.")
	}
	if len(params.GetVisibilityQuery()) != 0 && len(params.GetExecutions()) != 0 {
		return errors.New("batch query and executions are mutually exclusive")
	}
	if len(params.GetReason()) == 0 {
		return serviceerror.NewInvalidArgument("Reason is not set on request.")
	}
	if params.GetOperation() == nil {
		return serviceerror.NewInvalidArgument("Batch operation is not set on request.")
	}

	switch op := params.GetOperation().(type) {
	case *workflowservice.StartBatchOperationRequest_SignalOperation:
		if op.SignalOperation.GetSignal() == "" {
			return errors.New("must provide signal name")
		}
		return nil
	case *workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation:
		if op.UpdateWorkflowOptionsOperation.GetWorkflowExecutionOptions() == nil {
			return errors.New("must provide UpdateOptions")
		}
		if op.UpdateWorkflowOptionsOperation.GetUpdateMask() == nil {
			return errors.New("must provide UpdateMask")
		}
		// Validation for Versioning Override, if present, happens in history.
		return nil
	case *workflowservice.StartBatchOperationRequest_CancellationOperation,
		*workflowservice.StartBatchOperationRequest_TerminationOperation,
		*workflowservice.StartBatchOperationRequest_DeletionOperation:
		return nil
	case *workflowservice.StartBatchOperationRequest_ResetOperation:
		if op.ResetOperation == nil {
			return serviceerror.NewInvalidArgument("reset operation is not set")
		}
		if op.ResetOperation.Options != nil {
			if op.ResetOperation.Options.Target == nil {
				return serviceerror.NewInvalidArgument("batch reset missing target")
			}
		} else {
			//nolint:staticcheck // SA1019: GetResetType is deprecated but still needed for backward compatibility
			resetType := op.ResetOperation.GetResetType()
			if _, ok := enumspb.ResetType_name[int32(resetType)]; !ok || resetType == enumspb.RESET_TYPE_UNSPECIFIED {
				return serviceerror.NewInvalidArgumentf("unknown batch reset type %v", resetType)
			}
		}
	case *workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation:
		if op.UnpauseActivitiesOperation == nil {
			return serviceerror.NewInvalidArgument("unpause activities operation is not set")
		}
		if op.UnpauseActivitiesOperation.GetActivity() == nil {
			return serviceerror.NewInvalidArgument("activity filter must be set")
		}
		switch a := op.UnpauseActivitiesOperation.GetActivity().(type) {
		case *batchpb.BatchOperationUnpauseActivities_Type:
			if len(a.Type) == 0 {
				return serviceerror.NewInvalidArgument("Either activity type must be set, or match all should be set to true")
			}
		case *batchpb.BatchOperationUnpauseActivities_MatchAll:
			if !a.MatchAll {
				return serviceerror.NewInvalidArgument("Either activity type must be set, or match all should be set to true")
			}
		}
		return nil
	case *workflowservice.StartBatchOperationRequest_ResetActivitiesOperation:
		if op.ResetActivitiesOperation == nil {
			return serviceerror.NewInvalidArgument("reset activities operation is not set")
		}
		if op.ResetActivitiesOperation.GetActivity() == nil && !op.ResetActivitiesOperation.GetMatchAll() {
			return serviceerror.NewInvalidArgument("must provide ActivityType or MatchAll")
		}

		switch a := op.ResetActivitiesOperation.GetActivity().(type) {
		case *batchpb.BatchOperationResetActivities_Type:
			if len(a.Type) == 0 {
				return serviceerror.NewInvalidArgument("Either activity type must be set, or match all should be set to true")
			}
		case *batchpb.BatchOperationResetActivities_MatchAll:
			if !a.MatchAll {
				return serviceerror.NewInvalidArgument("Either activity type must be set, or match all should be set to true")
			}
		}
		return nil
	case *workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation:
		if op.UpdateActivityOptionsOperation == nil {
			return serviceerror.NewInvalidArgument("update activity options operation is not set")
		}
		if op.UpdateActivityOptionsOperation.GetActivityOptions() != nil && op.UpdateActivityOptionsOperation.GetRestoreOriginal() {
			return serviceerror.NewInvalidArgument("cannot set both activity options and restore original")
		}
		if op.UpdateActivityOptionsOperation.GetActivityOptions() == nil && !op.UpdateActivityOptionsOperation.GetRestoreOriginal() {
			return serviceerror.NewInvalidArgument("Either activity type must be set, or restore original should be set to true")
		}

		switch a := op.UpdateActivityOptionsOperation.GetActivity().(type) {
		case *batchpb.BatchOperationUpdateActivityOptions_Type:
			if len(a.Type) == 0 {
				return serviceerror.NewInvalidArgument("Either activity type must be set, or match all should be set to true")
			}
		case *batchpb.BatchOperationUpdateActivityOptions_MatchAll:
			if !a.MatchAll {
				return serviceerror.NewInvalidArgument("Either activity type must be set, or match all should be set to true")
			}
		}
		return nil
	default:
		return fmt.Errorf("not supported batch type: %v", params.GetOperation())
	}
	return nil
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
