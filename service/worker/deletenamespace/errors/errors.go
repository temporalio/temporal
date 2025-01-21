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

package errors

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

const (
	InvalidArgumentErrType                = "InvalidArgument"
	FailedPreconditionErrType             = "FailedPrecondition"
	ExecutionsStillExistErrType           = "ExecutionsStillExist"
	NoProgressErrType                     = "NoProgress"
	NotDeletedExecutionsStillExistErrType = "NotDeletedExecutionsStillExist"
)

func NewInvalidArgument(message string, cause error) error {
	return temporal.NewNonRetryableApplicationError(message, InvalidArgumentErrType, cause, nil)
}

func NewFailedPrecondition(message string, cause error) error {
	return temporal.NewNonRetryableApplicationError(message, FailedPreconditionErrType, cause, nil)
}

func NewExecutionsStillExist(count int) error {
	return temporal.NewApplicationError(fmt.Sprintf("%d executions are still exist", count), ExecutionsStillExistErrType, count)
}

func NewNoProgress(count int) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("no progress was made: %d executions are still exist", count), NoProgressErrType, nil, count)
}

func NewNotDeletedExecutionsStillExist(count int) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("%d not deleted executions are still exist", count), NotDeletedExecutionsStillExistErrType, nil, count)
}

func ToServiceError(err error, workflowID, runID string) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case InvalidArgumentErrType:
			return serviceerror.NewInvalidArgument(appErr.Message())
		case FailedPreconditionErrType:
			return serviceerror.NewFailedPrecondition(appErr.Message())
		}
	}
	return serviceerror.NewSystemWorkflow(
		&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		err,
	)
}
