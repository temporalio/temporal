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

package canary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

type (
	visibilityArchivalValidator interface {
		shouldRun() bool
		getQuery(workflowID, runID, workflowType string, startTime, closeTime time.Time) string
		validateExecutions([]*shared.WorkflowExecutionInfo) error
	}

	filestoreVisibilityArchivalValidator struct {
		expectedRunID string
	}
)

const (
	schemeFilestore = "file"

	queryPageSize = 100
)

func init() {
	registerWorkflow(visibilityArchivalWorkflow, wfTypeVisibilityArchival)
	registerActivity(visibilityArchivalActivity, activityTypeVisibilityArchival)
}

func visibilityArchivalWorkflow(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeHistoryArchival, scheduledTimeNanos)
	if err != nil {
		return err
	}

	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, newActivityOptions()),
		activityTypeVisibilityArchival,
		workflow.Now(ctx).UnixNano(),
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("failed to list archived workflows", zap.Error(err))
		return profile.end(err)
	}

	return profile.end(nil)
}

func visibilityArchivalActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeVisibilityArchival, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityArchivalContext(ctx).cadence
	resp, err := client.Describe(ctx, archivalDomain)
	if err != nil {
		return err
	}

	if resp.Configuration != nil &&
		resp.Configuration.GetVisibilityArchivalStatus() == shared.ArchivalStatusDisabled {
		return errors.New("domain not configured for visibility archival")
	}

	visArchivalURI := ""
	if resp.Configuration != nil {
		visArchivalURI = resp.Configuration.GetVisibilityArchivalURI()
	}

	var validator visibilityArchivalValidator
	scheme := getURIScheme(visArchivalURI)
	switch scheme {
	case schemeFilestore:
		validator = newFilestoreVisibilityArchivalValidator()
	default:
		return fmt.Errorf("unknown visibility archival scheme: %s", scheme)
	}

	if !validator.shouldRun() {
		return nil
	}

	startTime := time.Now()
	execution, err := executeArchivalExeternalWorkflow(ctx, client, startTime.UnixNano())
	if err != nil {
		return err
	}

	query := validator.getQuery(
		execution.GetWorkflowId(),
		execution.GetRunId(),
		wfTypeArchivalExternal,
		startTime,
		time.Now(),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		executions, err := listarchivedWorkflow(
			ctx,
			scope,
			client,
			&shared.ListArchivedWorkflowExecutionsRequest{
				Domain:   stringPtr(archivalDomain),
				PageSize: int32Ptr(queryPageSize),
				Query:    stringPtr(query),
			},
		)
		if err != nil && isBadRequestError(err) {
			return err
		}

		if err == nil && validator.validateExecutions(executions) == nil {
			return nil
		}

		<-time.After(5 * time.Second)
	}
}

func listarchivedWorkflow(
	ctx context.Context,
	scope tally.Scope,
	client cadenceClient,
	request *shared.ListArchivedWorkflowExecutionsRequest,
) ([]*shared.WorkflowExecutionInfo, error) {
	var executions []*shared.WorkflowExecutionInfo
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		scope.Counter(listArchivedWorkflowCount).Inc(1)
		sw := scope.Timer(listArchivedWorkflowsLatency).Start()
		response, err := client.ListArchivedWorkflow(ctx, request)
		sw.Stop()
		if err != nil {
			scope.Counter(listArchivedWorkflowFailureCount).Inc(1)
			return nil, err
		}

		if len(response.Executions) != 0 {
			executions = append(executions, response.Executions...)
		}

		if response.NextPageToken == nil {
			break
		}

		request.NextPageToken = response.NextPageToken
	}

	return executions, nil
}

func newFilestoreVisibilityArchivalValidator() visibilityArchivalValidator {
	return &filestoreVisibilityArchivalValidator{}
}

func (v *filestoreVisibilityArchivalValidator) shouldRun() bool {
	return true
}

func (v *filestoreVisibilityArchivalValidator) getQuery(
	workflowID, runID, workflowType string,
	startTime, closeTime time.Time,
) string {
	v.expectedRunID = runID
	return fmt.Sprintf(
		"WorkflowType = '%s' and WorkflowID = '%s' and CloseTime >= %v and CloseTime <= %v",
		workflowType,
		workflowID,
		startTime.UnixNano(),
		closeTime.UnixNano(),
	)
}

func (v *filestoreVisibilityArchivalValidator) validateExecutions(
	executions []*shared.WorkflowExecutionInfo,
) error {
	if len(executions) != 1 {
		return fmt.Errorf("listarchivedWorkflow returned %d executions, expecting 1", len(executions))
	}

	runID := executions[0].Execution.GetRunId()
	if runID != v.expectedRunID {
		return fmt.Errorf("listarchivedWorkflow returned wrong runID %v, expecting %s", runID, v.expectedRunID)
	}

	return nil
}

func getURIScheme(URI string) string {
	if idx := strings.Index(URI, "://"); idx != -1 {
		return URI[:idx]
	}
	return ""
}

func isBadRequestError(
	err error,
) bool {
	_, ok := err.(*shared.BadRequestError)
	return ok
}
