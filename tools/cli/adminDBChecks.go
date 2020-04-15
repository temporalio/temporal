// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cli

import (
	"fmt"

	"github.com/gocql/gocql"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
)

type (
	// CheckResultStatus is the result status of a check
	CheckResultStatus string
	// CheckType is the type of check
	CheckType string
)

const (
	// CheckResultHealthy indicates check successfully ran and detected no corruption
	CheckResultHealthy CheckResultStatus = "healthy"
	// CheckResultCorrupted indicates check successfully ran and detected corruption
	CheckResultCorrupted = "corrupted"
	// CheckResultFailed indicates check failed to run
	CheckResultFailed = "failed"

	// CheckTypeHistoryExists is the check type for history exists
	CheckTypeHistoryExists CheckType = "history_exists"
	// CheckTypeValidFirstEvent is the check type for valid first event
	CheckTypeValidFirstEvent = "valid_first_event"
	// CheckTypeOrphanExecution is the check type for orphan execution
	CheckTypeOrphanExecution = "orphan_execution"

	historyPageSize = 1
)

type (
	// AdminDBCheck is used to check database invariants
	AdminDBCheck interface {
		Check(*CheckRequest) *CheckResult
		ValidRequest(*CheckRequest) bool
	}

	historyExistsCheck struct {
		dbRateLimiter  *quotas.DynamicRateLimiter
		historyStore   persistence.HistoryStore
		executionStore persistence.ExecutionStore
	}

	firstHistoryEventCheck struct {
		payloadSerializer persistence.PayloadSerializer
	}

	orphanExecutionCheck struct {
		dbRateLimiter     *quotas.DynamicRateLimiter
		executionStore    persistence.ExecutionStore
		payloadSerializer persistence.PayloadSerializer
	}

	// CheckRequest is a request to check an execution
	CheckRequest struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
		TreeID     string
		BranchID   string
		State      int
		// PrerequisiteCheckPayload is used to take in any payloads which this check needs
		// which were created by a check ran earlier
		PrerequisiteCheckPayload interface{}
	}

	// CheckResult is the result of checking an execution
	CheckResult struct {
		CheckType             CheckType
		CheckResultStatus     CheckResultStatus
		TotalDatabaseRequests int64
		// Payload can be used to return additional data which can be used by later checks
		Payload   interface{}
		ErrorInfo *ErrorInfo
	}

	// ErrorInfo contains information about any errors that occurred
	ErrorInfo struct {
		Note    string
		Details string
	}
)

// NewHistoryExistsCheck constructs a historyExistsCheck
func NewHistoryExistsCheck(
	dbRateLimiter *quotas.DynamicRateLimiter,
	historyStore persistence.HistoryStore,
	executionStore persistence.ExecutionStore,
) AdminDBCheck {
	return &historyExistsCheck{
		dbRateLimiter:  dbRateLimiter,
		historyStore:   historyStore,
		executionStore: executionStore,
	}
}

// NewFirstHistoryEventCheck constructs a firstHistoryEventCheck
func NewFirstHistoryEventCheck(
	payloadSerializer persistence.PayloadSerializer,
) AdminDBCheck {
	return &firstHistoryEventCheck{
		payloadSerializer: payloadSerializer,
	}
}

// NewOrphanExecutionCheck constructs an orphanExecutionCheck
func NewOrphanExecutionCheck(
	dbRateLimiter *quotas.DynamicRateLimiter,
	executionStore persistence.ExecutionStore,
	payloadSerializer persistence.PayloadSerializer,
) AdminDBCheck {
	return &orphanExecutionCheck{
		dbRateLimiter:     dbRateLimiter,
		executionStore:    executionStore,
		payloadSerializer: payloadSerializer,
	}
}

// Check checks that history exists
func (c *historyExistsCheck) Check(cr *CheckRequest) *CheckResult {
	result := &CheckResult{
		CheckType: CheckTypeHistoryExists,
	}
	readHistoryBranchReq := &persistence.InternalReadHistoryBranchRequest{
		TreeID:    cr.TreeID,
		BranchID:  cr.BranchID,
		MinNodeID: common.FirstEventID,
		MaxNodeID: common.EndEventID,
		ShardID:   cr.ShardID,
		PageSize:  historyPageSize,
	}
	history, historyErr := retryReadHistoryBranch(
		c.dbRateLimiter,
		&result.TotalDatabaseRequests,
		c.historyStore,
		readHistoryBranchReq)

	stillExists, executionErr := concreteExecutionStillExists(
		cr,
		c.executionStore,
		c.dbRateLimiter,
		&result.TotalDatabaseRequests)

	if executionErr != nil {
		result.CheckResultStatus = CheckResultFailed
		result.ErrorInfo = &ErrorInfo{
			Note:    "failed to verify if concrete execution still exists",
			Details: executionErr.Error(),
		}
		return result
	}
	if !stillExists {
		result.CheckResultStatus = CheckResultHealthy
		return result
	}

	if historyErr != nil {
		if historyErr == gocql.ErrNotFound {
			result.CheckResultStatus = CheckResultCorrupted
			result.ErrorInfo = &ErrorInfo{
				Note:    "concrete execution exists but history does not",
				Details: historyErr.Error(),
			}
			return result
		}
		result.CheckResultStatus = CheckResultFailed
		result.ErrorInfo = &ErrorInfo{
			Note:    "failed to verify if history exists",
			Details: historyErr.Error(),
		}
		return result
	}
	if history == nil || len(history.History) == 0 {
		result.CheckResultStatus = CheckResultCorrupted
		result.ErrorInfo = &ErrorInfo{
			Note: "got empty history",
		}
		return result
	}
	result.CheckResultStatus = CheckResultHealthy
	result.Payload = history
	return result
}

// ValidRequest returns true if the request is valid, false otherwise
func (c *historyExistsCheck) ValidRequest(cr *CheckRequest) bool {
	return validRequestHelper(cr)
}

// Check checks that first event in history is valid
// CheckRequest must contain a payload of type *persistence.InternalReadHistoryBranchResponse
func (c *firstHistoryEventCheck) Check(cr *CheckRequest) *CheckResult {
	result := &CheckResult{
		CheckType: CheckTypeValidFirstEvent,
	}
	history := cr.PrerequisiteCheckPayload.(*persistence.InternalReadHistoryBranchResponse)
	firstBatch, err := c.payloadSerializer.DeserializeBatchEvents(history.History[0])
	if err != nil || len(firstBatch) == 0 {
		result.CheckResultStatus = CheckResultFailed
		result.ErrorInfo = &ErrorInfo{
			Note:    "failed to deserialize batch events",
			Details: err.Error(),
		}
		return result
	}
	if firstBatch[0].GetEventId() != common.FirstEventID {
		result.CheckResultStatus = CheckResultCorrupted
		result.ErrorInfo = &ErrorInfo{
			Note:    "got unexpected first eventID",
			Details: fmt.Sprintf("expected: %v but got %v", common.FirstEventID, firstBatch[0].GetEventId()),
		}
		return result
	}
	if firstBatch[0].GetEventType() != shared.EventTypeWorkflowExecutionStarted {
		result.CheckResultStatus = CheckResultCorrupted
		result.ErrorInfo = &ErrorInfo{
			Note:    "got unexpected first eventType",
			Details: fmt.Sprintf("expected: %v but got %v", shared.EventTypeWorkflowExecutionStarted.String(), firstBatch[0].GetEventType().String()),
		}
		return result
	}
	result.CheckResultStatus = CheckResultHealthy
	return result
}

// ValidRequest returns true if the request is valid, false otherwise
func (c *firstHistoryEventCheck) ValidRequest(cr *CheckRequest) bool {
	if cr.PrerequisiteCheckPayload == nil {
		return false
	}
	history, ok := cr.PrerequisiteCheckPayload.(*persistence.InternalReadHistoryBranchResponse)
	if !ok {
		return false
	}
	if history.History == nil || len(history.History) == 0 {
		return false
	}
	return validRequestHelper(cr)
}

func (c *orphanExecutionCheck) Check(cr *CheckRequest) *CheckResult {
	result := &CheckResult{
		CheckType: CheckTypeOrphanExecution,
	}
	if !executionOpen(cr) {
		result.CheckResultStatus = CheckResultHealthy
		return result
	}
	getCurrentExecutionRequest := &persistence.GetCurrentExecutionRequest{
		DomainID:   cr.DomainID,
		WorkflowID: cr.WorkflowID,
	}
	currentExecution, currentErr := retryGetCurrentExecution(
		c.dbRateLimiter,
		&result.TotalDatabaseRequests,
		c.executionStore,
		getCurrentExecutionRequest)

	stillOpen, concreteErr := concreteExecutionStillOpen(cr, c.executionStore, c.dbRateLimiter, &result.TotalDatabaseRequests)
	if concreteErr != nil {
		result.CheckResultStatus = CheckResultFailed
		result.ErrorInfo = &ErrorInfo{
			Note:    "failed to check if concrete execution is still open",
			Details: concreteErr.Error(),
		}
		return result
	}
	if !stillOpen {
		result.CheckResultStatus = CheckResultHealthy
		return result
	}

	if currentErr != nil {
		switch currentErr.(type) {
		case *shared.EntityNotExistsError:
			result.CheckResultStatus = CheckResultCorrupted
			result.ErrorInfo = &ErrorInfo{
				Note:    "execution is open without having current execution",
				Details: currentErr.Error(),
			}
			return result
		}
		result.CheckResultStatus = CheckResultFailed
		result.ErrorInfo = &ErrorInfo{
			Note:    "failed to check if current execution exists",
			Details: currentErr.Error(),
		}
		return result
	}
	if currentExecution.RunID != cr.RunID {
		result.CheckResultStatus = CheckResultCorrupted
		result.ErrorInfo = &ErrorInfo{
			Note: "execution is open but current points at a different execution",
		}
		return result
	}
	result.CheckResultStatus = CheckResultHealthy
	return result
}

// ValidRequest returns true if the request is valid, false otherwise
func (c *orphanExecutionCheck) ValidRequest(cr *CheckRequest) bool {
	return validRequestHelper(cr)
}

func concreteExecutionStillOpen(
	cr *CheckRequest,
	execStore persistence.ExecutionStore,
	limiter *quotas.DynamicRateLimiter,
	totalDBRequests *int64,
) (bool, error) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		DomainID: cr.DomainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: &cr.WorkflowID,
			RunId:      &cr.RunID,
		},
	}
	_, err := retryGetWorkflowExecution(limiter, totalDBRequests, execStore, getConcreteExecution)

	if err != nil {
		switch err.(type) {
		case *shared.EntityNotExistsError:
			return false, nil
		default:
			return false, err
		}
	}
	return executionOpen(cr), nil
}

func concreteExecutionStillExists(
	cr *CheckRequest,
	execStore persistence.ExecutionStore,
	limiter *quotas.DynamicRateLimiter,
	totalDBRequests *int64,
) (bool, error) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		DomainID: cr.DomainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: &cr.WorkflowID,
			RunId:      &cr.RunID,
		},
	}
	_, err := retryGetWorkflowExecution(limiter, totalDBRequests, execStore, getConcreteExecution)
	if err == nil {
		return true, nil
	}

	switch err.(type) {
	case *shared.EntityNotExistsError:
		return false, nil
	default:
		return false, err
	}
}

func executionOpen(cr *CheckRequest) bool {
	return cr.State == persistence.WorkflowStateCreated ||
		cr.State == persistence.WorkflowStateRunning
}

func validRequestHelper(cr *CheckRequest) bool {
	return len(cr.DomainID) > 0 &&
		len(cr.WorkflowID) > 0 &&
		len(cr.RunID) > 0 &&
		len(cr.TreeID) > 0 &&
		len(cr.BranchID) > 0
}
