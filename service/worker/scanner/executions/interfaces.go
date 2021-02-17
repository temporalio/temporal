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
	historypb "go.temporal.io/api/history/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	// executionData wraps data on execution that is suitable for running validations.
	// Extend this structure if more data is required for new checks.
	executionData struct {
		mutableState  *persistencespb.WorkflowMutableState
		history       []*historypb.HistoryEvent
		historyBranch *persistencespb.HistoryBranch
	}

	// executionValidatorResult provides a summary of validation results
	executionValidatorResult struct {
		// true if provided executionData is valid.
		isValid bool
		// This would be put into metric label.
		failureReasonTag string
		// Detailed description. Will be added to failure long.
		failureReasonDetailed string
		// Extra error information
		error error
	}

	// executionValidator Implement this interface and provide it to executions scavenger to add extra checks
	// on executions table.
	executionValidator interface {
		validatorTag() string
		validate(execData *executionData) executionValidatorResult
	}
)
