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

	"go.temporal.io/sdk/temporal"
)

const (
	ExecutionsStillExistErrType           = "ExecutionsStillExist"
	NoProgressErrType                     = "NoProgress"
	NotDeletedExecutionsStillExistErrType = "NotDeletedExecutionsStillExist"
)

var (
	ErrUnableToExecuteActivity      = errors.New("unable to execute activity")
	ErrUnableToExecuteChildWorkflow = errors.New("unable to execute child workflow")
)

func NewExecutionsStillExistError(count int) error {
	return temporal.NewApplicationError(fmt.Sprintf("%d executions are still exist", count), ExecutionsStillExistErrType, count)
}

func NewSomeExecutionsStillExistError() error {
	return temporal.NewApplicationError("some executions are still exist", ExecutionsStillExistErrType)
}

func NewNoProgressError(count int) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("no progress were made: %d executions are still exist", count), NoProgressErrType, nil, count)
}

func NewNotDeletedExecutionsStillExistError(count int) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("%d not deleted executions are still exist", count), NotDeletedExecutionsStillExistErrType, nil, count)
}
