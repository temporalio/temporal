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

package enums

import (
	"fmt"
)

var (
	WorkflowExecutionState_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Created":     1,
		"Running":     2,
		"Completed":   3,
		"Zombie":      4,
		"Void":        5,
		"Corrupted":   6,
	}
)

// WorkflowExecutionStateFromString parses a WorkflowExecutionState value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to WorkflowExecutionState
func WorkflowExecutionStateFromString(s string) (WorkflowExecutionState, error) {
	if v, ok := WorkflowExecutionState_value[s]; ok {
		return WorkflowExecutionState(v), nil
	} else if v, ok := WorkflowExecutionState_shorthandValue[s]; ok {
		return WorkflowExecutionState(v), nil
	}
	return WorkflowExecutionState(0), fmt.Errorf("%s is not a valid WorkflowExecutionState", s)
}

var (
	WorkflowBackoffType_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Retry":       1,
		"Cron":        2,
		"DelayStart":  3,
	}
)

// WorkflowBackoffTypeFromString parses a WorkflowBackoffType value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to WorkflowBackoffType
func WorkflowBackoffTypeFromString(s string) (WorkflowBackoffType, error) {
	if v, ok := WorkflowBackoffType_value[s]; ok {
		return WorkflowBackoffType(v), nil
	} else if v, ok := WorkflowBackoffType_shorthandValue[s]; ok {
		return WorkflowBackoffType(v), nil
	}
	return WorkflowBackoffType(0), fmt.Errorf("%s is not a valid WorkflowBackoffType", s)
}
