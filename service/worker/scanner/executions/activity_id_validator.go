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

import "fmt"

type (
	// activityIDValidator is a validator that checks for validity of activity ids in mutable state.
	// This refers to an accident when DB or our code wrote incorrect values that looked like some huge int64s.
	// implements executionValidator
	activityIDValidator struct{}
)

// newActivityIDValidator returns new instance.
func newActivityIDValidator() activityIDValidator {
	return activityIDValidator{}
}

// ValidatorTag returns validator name tag
func (activityIDValidator) validatorTag() string {
	return "activity_id_validator"
}

// Validate Checks correctness of activity ids in mutable state.
func (activityIDValidator) validate(src *executionData) executionValidatorResult {
	activityInfos := src.mutableState.ActivityInfos

	if len(activityInfos) == 0 {
		return executionValidatorResultNoCorruption
	}

	nextEventID := src.mutableState.NextEventId
	for activityID := range activityInfos {
		if activityID >= nextEventID || activityID < 0 {
			return executionValidatorResult{
				isValid:          false,
				failureReasonTag: "corrupted_activity_id",
				failureReasonDetailed: fmt.Sprintf(
					"ActivityID: %d is not less than NextEventID: %d", activityID, nextEventID,
				),
				error: nil,
			}
		}
	}
	return executionValidatorResultNoCorruption
}
