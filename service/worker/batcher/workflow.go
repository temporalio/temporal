// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package batcher

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/searchattribute"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
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
		// Deprecated fields:
		ResetType        enumspb.ResetType
		ResetReapplyType enumspb.ResetReapplyType
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

	taskDetail struct {
		execution *commonpb.WorkflowExecution
		attempts  int
		// passing along the current heartbeat details to make heartbeat within a task so that it won't timeout
		hbd HeartBeatDetails
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
		return fmt.Errorf("must provide required parameters: BatchType/Reason/Namespace/Query/Executions")
	}

	if len(params.Query) > 0 && len(params.Executions) > 0 {
		return fmt.Errorf("batch query and executions are mutually exclusive")
	}

	switch params.BatchType {
	case BatchTypeSignal:
		if params.SignalParams.SignalName == "" {
			return fmt.Errorf("must provide signal name")
		}
		return nil
	case BatchTypeCancel, BatchTypeTerminate, BatchTypeDelete, BatchTypeReset:
		return nil
	default:
		return fmt.Errorf("not supported batch type: %v", params.BatchType)
	}
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
