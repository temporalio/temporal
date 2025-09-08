package batcher

import (
	"errors"
	"fmt"
	"time"

	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
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
		searchattribute.TemporalNamespaceDivision,
		NamespaceDivision,
		searchattribute.ExecutionStatus,
		int(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
	)
)

type (
	// TerminateParams is the parameters for terminating workflow
	TerminateParams struct {
	}

	// CancelParams is the parameters for canceling workflow
	CancelParams struct {
	}

	// SignalParams is the parameters for signaling workflow
	SignalParams struct {
		SignalName string
		Input      *commonpb.Payloads
	}

	// DeleteParams is the parameters for deleting workflow
	DeleteParams struct {
	}

	// ResetParams is the parameters for reseting workflow
	ResetParams struct {
		// This is a serialized commonpb.ResetOptions. We can't include it with the
		// correct type because workflow/activity arguments are going to be serialized with the
		// json dataconverter, which doesn't support the "oneof" field in ResetOptions.
		ResetOptions []byte
		resetOptions *commonpb.ResetOptions // deserialized version

		// This is a slice of serialized workflowpb.PostResetOperation.
		// For the same reason as ResetOptions, we manually serialize/deserialize it.
		PostResetOperations [][]byte
		postResetOperations []*workflowpb.PostResetOperation
		// Deprecated fields:
		ResetType        enumspb.ResetType
		ResetReapplyType enumspb.ResetReapplyType
	}

	// UpdateOptionsParams is the parameters for updating workflow execution options
	UpdateOptionsParams struct {
		WorkflowExecutionOptions *workflowpb.WorkflowExecutionOptions
		UpdateMask               *FieldMask
	}

	UnpauseActivitiesParams struct {
		Identity       string
		ActivityType   string
		MatchAll       bool
		ResetAttempts  bool
		ResetHeartbeat bool
		Jitter         time.Duration
	}

	UpdateActivitiesOptionsParams struct {
		Identity        string
		ActivityType    string
		MatchAll        bool
		ActivityOptions *ActivityOptions
		UpdateMask      *FieldMask
		RestoreOriginal bool
	}

	ActivityOptions struct {
		TaskQueue              *TaskQueue
		ScheduleToCloseTime    time.Duration
		ScheduleToStartTimeout time.Duration
		StartToCloseTimeout    time.Duration
		HeartbeatTimeout       time.Duration
		RetryPolicy            *RetryPolicy
	}

	TaskQueue struct {
		Name string
		Kind int32
	}

	RetryPolicy struct {
		InitialInterval        time.Duration
		BackoffCoefficient     float64
		MaximumInterval        time.Duration
		MaximumAttempts        int32
		NonRetryableErrorTypes []string
	}

	ResetActivitiesParams struct {
		Identity               string
		ActivityType           string
		MatchAll               bool
		ResetAttempts          bool
		ResetHeartbeat         bool
		KeepPaused             bool
		Jitter                 time.Duration
		RestoreOriginalOptions bool
	}

	FieldMask struct {
		Paths []string
	}

	// BatchParams is the parameters for batch operation workflow
	BatchParams struct {
		// Target namespace to execute batch operation
		Namespace string
		// To get the target workflows for processing
		Query string
		// Target workflows for processing
		Executions []*commonpb.WorkflowExecution
		// Reason for the operation
		Reason string
		// Supporting: signal,cancel,terminate,delete,reset
		BatchType string

		// Below are all optional
		// TerminateParams is params only for BatchTypeTerminate
		TerminateParams TerminateParams
		// CancelParams is params only for BatchTypeCancel
		CancelParams CancelParams
		// SignalParams is params only for BatchTypeSignal
		SignalParams SignalParams
		// DeleteParams is params only for BatchTypeDelete
		DeleteParams DeleteParams
		// ResetParams is params only for BatchTypeReset
		ResetParams ResetParams
		// UpdateOptionsParams is params only for BatchTypeUpdateOptions
		UpdateOptionsParams UpdateOptionsParams
		// UnpauseActivitiesParams is params only for BatchTypeUnpauseActivities
		UnpauseActivitiesParams UnpauseActivitiesParams
		// UpdateActivitiesOptionsParams is params only for BatchTypeUpdateActivitiesOptions
		UpdateActivitiesOptionsParams UpdateActivitiesOptionsParams
		// ResetActivitiesParams is params only for BatchTypeResetActivities
		ResetActivitiesParams ResetActivitiesParams

		// RPS sets the requests-per-second limit for the batch.
		// The default (and max) is defined by `worker.BatcherRPS` in the dynamic config.
		RPS float64
		// Number of goroutines running in parallel to process
		// This is moving to dynamic config.
		// TODO: Remove it from BatchParams after 1.19+
		Concurrency int
		// Number of attempts for each workflow to process in case of retryable error before giving up
		AttemptsOnRetryableError int
		// timeout for activity heartbeat
		ActivityHeartBeatTimeout time.Duration
		// errors that will not retry which consumes AttemptsOnRetryableError. Default to empty
		NonRetryableErrors []string
		// internal conversion for NonRetryableErrors
		_nonRetryableErrors map[string]struct{}
	}

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

// BatchWorkflow is the workflow that runs a batch job of resetting workflows.
func BatchWorkflow(ctx workflow.Context, batchParams BatchParams) (HeartBeatDetails, error) {
	batchParams = setDefaultParams(batchParams)
	err := validateParams(batchParams)
	if err != nil {
		return HeartBeatDetails{}, err
	}

	batchActivityOptions.HeartbeatTimeout = batchParams.ActivityHeartBeatTimeout
	opt := workflow.WithActivityOptions(ctx, batchActivityOptions)
	var result HeartBeatDetails
	var ac *activities
	err = workflow.ExecuteActivity(opt, ac.BatchActivity, batchParams).Get(ctx, &result)
	if err != nil {
		return HeartBeatDetails{}, err
	}

	err = attachBatchOperationStats(ctx, result)
	if err != nil {
		return HeartBeatDetails{}, err
	}
	return result, err
}

// BatchWorkflowProtobuf is the workflow that runs a batch job of resetting workflows.
func BatchWorkflowProtobuf(ctx workflow.Context, batchParams *batchspb.BatchOperationInput) (HeartBeatDetails, error) {
	if batchParams == nil {
		return HeartBeatDetails{}, errors.New("batchParams is nil")
	}

	batchParams = setDefaultParamsProtobuf(batchParams)
	err := ValidateBatchOperation(batchParams.Request)
	if err != nil {
		return HeartBeatDetails{}, err
	}

	batchActivityOptions.HeartbeatTimeout = batchParams.ActivityHeartbeatTimeout.AsDuration()
	opt := workflow.WithActivityOptions(ctx, batchActivityOptions)
	var result HeartBeatDetails
	var ac *activities
	err = workflow.ExecuteActivity(opt, ac.BatchActivityWithProtobuf, batchParams).Get(ctx, &result)
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

func validateParams(params BatchParams) error {
	if params.BatchType == "" ||
		params.Reason == "" ||
		params.Namespace == "" ||
		(params.Query == "" && len(params.Executions) == 0) {
		return errors.New("must provide required parameters: BatchType/Reason/Namespace/Query/Executions")
	}

	if len(params.Query) > 0 && len(params.Executions) > 0 {
		return errors.New("batch query and executions are mutually exclusive")
	}

	switch params.BatchType {
	case BatchTypeSignal:
		if params.SignalParams.SignalName == "" {
			return errors.New("must provide signal name")
		}
		return nil
	case BatchTypeUpdateOptions:
		if params.UpdateOptionsParams.WorkflowExecutionOptions == nil {
			return errors.New("must provide UpdateOptions")
		}
		if params.UpdateOptionsParams.UpdateMask == nil {
			return errors.New("must provide UpdateMask")
		}
		return worker_versioning.ValidateVersioningOverride(params.UpdateOptionsParams.WorkflowExecutionOptions.VersioningOverride)
	case BatchTypeCancel, BatchTypeTerminate, BatchTypeDelete, BatchTypeReset:
		return nil
	case BatchTypeUnpauseActivities:
		if params.UnpauseActivitiesParams.ActivityType == "" && !params.UnpauseActivitiesParams.MatchAll {
			return fmt.Errorf("must provide ActivityType or MatchAll")
		}
		return nil
	case BatchTypeResetActivities:
		if params.ResetActivitiesParams.ActivityType == "" && !params.ResetActivitiesParams.MatchAll {
			return errors.New("must provide ActivityType or MatchAll")
		}
		return nil
	case BatchTypeUpdateActivitiesOptions:
		if params.UpdateActivitiesOptionsParams.ActivityType == "" && !params.UpdateActivitiesOptionsParams.MatchAll {
			return errors.New("must provide ActivityType or MatchAll")
		}
		return nil
	default:
		return fmt.Errorf("not supported batch type: %v", params.BatchType)
	}
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
		return worker_versioning.ValidateVersioningOverride(op.UpdateWorkflowOptionsOperation.GetWorkflowExecutionOptions().GetVersioningOverride())
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

func setDefaultParams(params BatchParams) BatchParams {
	if params.AttemptsOnRetryableError <= 1 {
		params.AttemptsOnRetryableError = defaultAttemptsOnRetryableError
	}
	if params.ActivityHeartBeatTimeout <= 0 {
		params.ActivityHeartBeatTimeout = defaultActivityHeartBeatTimeout
	}
	if len(params.NonRetryableErrors) > 0 {
		params._nonRetryableErrors = make(map[string]struct{}, len(params.NonRetryableErrors))
		for _, estr := range params.NonRetryableErrors {
			params._nonRetryableErrors[estr] = struct{}{}
		}
	}
	return params
}

func setDefaultParamsProtobuf(params *batchspb.BatchOperationInput) *batchspb.BatchOperationInput {
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
