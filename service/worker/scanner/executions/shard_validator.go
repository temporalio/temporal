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

package executions

import (
	"context"
	"errors"

	"github.com/gocql/gocql"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/quotas"
)

type (
	// shardValidator Runs check on a single shard of executions table
	shardValidator interface {
		validate(shardID int32) *shardValidationResult
	}

	shardValidatorImpl struct {
		execMgrFactory     persistence.ExecutionManagerFactory
		limiter            quotas.RateLimiter
		executionsPageSize int
		historyDB          persistence.HistoryManager
		validators         []executionValidator
	}

	// shardValidationResult summary of shard validation
	shardValidationResult struct {
		ShardID           int32
		TotalDBRequests   int64
		ScanStats         shardValidationReportExecutionsScanned
		IsFailure         bool
		Details           string
		Error             error
		ExecutionFailures []executionValidationFailure
	}

	// executionValidationFailure details of single execution validation failure.
	executionValidationFailure struct {
		Namespace   string
		WorkflowID  string
		RunID       string
		BranchID    string
		TreeID      string
		NextEventID int64
		CloseStatus enumspb.WorkflowExecutionStatus

		// validatorTag -> ValidatorFailure
		Failures map[string]executionValidatorResult
	}
)

// createShardValidator returns instance of shardValidator implementation
func createShardValidator(
	execMgrProvider persistence.ExecutionManagerFactory,
	limiter quotas.RateLimiter,
	executionsPageSize int,
	historyDB persistence.HistoryManager,
	validators []executionValidator,
) *shardValidatorImpl {
	return &shardValidatorImpl{
		execMgrFactory:     execMgrProvider,
		limiter:            limiter,
		executionsPageSize: executionsPageSize,
		historyDB:          historyDB,
		validators:         validators,
	}
}

func (s *shardValidatorImpl) getPaginationFn(
	execMgr persistence.ExecutionManager,
	dbRequests *int64,
) collection.PaginationFn {
	return func(paginationToken []byte) ([]interface{}, []byte, error) {
		req := &persistence.ListConcreteExecutionsRequest{
			PageSize:  executionsPageSize,
			PageToken: paginationToken,
		}
		preconditionForDBCall(dbRequests, s.limiter)
		resp, err := execMgr.ListConcreteExecutions(req)
		if err != nil {
			return nil, nil, err
		}
		var paginateItems []interface{}
		for _, state := range resp.States {
			paginateItems = append(paginateItems, state)
		}
		return paginateItems, resp.PageToken, nil
	}
}

func (s *shardValidatorImpl) validate(shardID int32) *shardValidationResult {
	report := &shardValidationResult{
		ShardID: shardID,
	}

	execMgr, err := s.execMgrFactory.NewExecutionManager(shardID)
	if err != nil {
		report.IsFailure = true
		report.Details = "Failed to receive mutableState manager"
		report.Error = err
		return report
	}

	iter := collection.NewPagingIterator(s.getPaginationFn(execMgr, &report.TotalDBRequests))
	for iter.HasNext() {
		execution, err := iter.Next()
		if err != nil {
			report.IsFailure = true
			report.Details = "Failed to fetch next execution."
			report.Error = err
			return report
		}
		report.ScanStats.TotalExecutionsCount++

		executionValidationFailure := validateExecution(
			execution.(*persistencespb.WorkflowMutableState),
			shardID,
			s.limiter,
			s.historyDB,
			&report.TotalDBRequests,
			s.validators,
		)
		if executionValidationFailure != nil {
			report.IsFailure = true
			report.Details = "One or more execution validators reported failure."
			report.ScanStats.CorruptedExecutionsCount++
			report.ExecutionFailures = append(report.ExecutionFailures, *executionValidationFailure)
		}
	}
	return report
}

func newExecutionValidationFailure(src *executionData) *executionValidationFailure {
	result := &executionValidationFailure{
		Namespace:   src.mutableState.ExecutionInfo.NamespaceId,
		WorkflowID:  src.mutableState.ExecutionInfo.WorkflowId,
		RunID:       src.mutableState.ExecutionState.RunId,
		NextEventID: src.mutableState.NextEventId,
		CloseStatus: src.mutableState.ExecutionState.Status,
	}
	if src.historyBranch != nil {
		result.BranchID = src.historyBranch.BranchId
		result.TreeID = src.historyBranch.TreeId
	}
	return result
}
func newExecutionValidationFailureWithFailures(src *executionData, failures map[string]executionValidatorResult) *executionValidationFailure {
	result := newExecutionValidationFailure(src)
	result.Failures = failures
	return result
}

func validateExecution(
	mutableState *persistencespb.WorkflowMutableState,
	shardID int32,
	limiter quotas.RateLimiter,
	historyDB persistence.HistoryManager,
	dbRequestsCount *int64,
	validators []executionValidator,
) *executionValidationFailure {

	executionData := &executionData{
		mutableState:  mutableState,
		history:       nil,
		historyBranch: nil,
	}

	var executionValidationResult executionValidatorResult
	executionValidationResult, executionData.history, executionData.historyBranch = fetchHistoryBranch(
		mutableState.ExecutionInfo,
		shardID,
		limiter,
		historyDB,
		dbRequestsCount,
	)
	if !executionValidationResult.isValid {
		return newExecutionValidationFailureWithFailures(
			executionData, map[string]executionValidatorResult{"history_fetcher": executionValidationResult},
		)
	}

	var result *executionValidationFailure = nil
	for _, executionValidator := range validators {
		executionValidationResult = executionValidator.validate(executionData)
		if !executionValidationResult.isValid {
			if result == nil {
				result = newExecutionValidationFailure(executionData)
				result.Failures = map[string]executionValidatorResult{}
			}
			result.Failures[executionValidator.validatorTag()] = executionValidationResult
		}
	}
	return result
}

func fetchHistoryBranch(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	shardID int32,
	limiter quotas.RateLimiter,
	historyDB persistence.HistoryManager,
	totalDBRequests *int64,
) (executionValidatorResult, []*historypb.HistoryEvent, *persistencespb.HistoryBranch) {
	var branch *persistencespb.HistoryBranch
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err != nil {
		return executionValidatorResult{
			isValid:               false,
			failureReasonTag:      "failed_fetch_history",
			failureReasonDetailed: "Failed to fetch current version history",
		}, nil, nil
	}

	branch, err = serialization.HistoryBranchFromBlob(currentVersionHistory.BranchToken,
		enumspb.ENCODING_TYPE_PROTO3.String())

	if err != nil {
		return executionValidatorResult{
			isValid:               false,
			failureReasonTag:      "failed_decode_branch_token",
			failureReasonDetailed: "Failed to decode current branch token.",
		}, nil, nil
	}

	var historyPageSize int = 1
	readHistoryBranchReq := &persistence.ReadHistoryBranchRequest{
		MinEventID:    common.FirstEventID,
		MaxEventID:    common.EndEventID,
		BranchToken:   currentVersionHistory.BranchToken,
		ShardID:       &shardID,
		PageSize:      historyPageSize,
		NextPageToken: nil,
	}

	preconditionForDBCall(totalDBRequests, limiter)
	history, err := historyDB.ReadHistoryBranch(readHistoryBranchReq)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return executionValidatorResult{
				isValid:               false,
				failureReasonTag:      "invalid_missing_corresponding_branch",
				failureReasonDetailed: "Execution misses corresponding branch entry.",
				error:                 nil,
			}, nil, nil
		}
		return executionValidatorResult{
			isValid:               false,
			failureReasonTag:      "failure_fetch_corresponding_history_branch",
			failureReasonDetailed: "Failed to fetch history branch.",
			error:                 err,
		}, nil, nil
	}

	if history == nil || len(history.HistoryEvents) == 0 {
		return executionValidatorResult{
			isValid:               false,
			failureReasonTag:      "invalid_empty_history",
			failureReasonDetailed: "History for mutableState is empty.",
			error:                 nil,
		}, nil, nil
	}
	return executionValidatorResultNoCorruption, history.HistoryEvents, branch
}

func getRateLimiter(targetRPS int) quotas.RateLimiter {
	return quotas.NewRateLimiter(float64(targetRPS), targetRPS)
}

func preconditionForDBCall(totalDBRequests *int64, limiter quotas.RateLimiter) {
	*totalDBRequests = *totalDBRequests + 1
	_ = limiter.Wait(context.Background())
}
