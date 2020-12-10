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

package common

import (
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/dynamicconfig"
)

// TODO remove everything below:
// TODO remove after 1.5
//  DefaultWorkflowExecutionTimeout
//  DefaultWorkflowRunTimeout
//  GetWorkflowExecutionTimeout
//  GetWorkflowRunTimeout
//  GetWorkflowTaskTimeout
const (
	// DefaultWorkflowExecutionTimeout is the Default Workflow Execution timeout applied to a Workflow when
	// this value is not explicitly set by the user on a Start Workflow request
	// Intention is 10 years
	DefaultWorkflowExecutionTimeout = 24 * 365 * 10 * time.Hour

	// DefaultWorkflowRunTimeout is the Default Workflow Run timeout applied to a Workflow when
	// this value is not explicitly set by the user on a Start Workflow request
	// Intention is 10 years
	DefaultWorkflowRunTimeout = 24 * 365 * 10 * time.Hour
)

// GetWorkflowExecutionTimeout gets the default allowed execution timeout or truncates the requested value to the maximum allowed timeout
func GetWorkflowExecutionTimeout(
	requestedTimeout time.Duration,
) time.Duration {
	if requestedTimeout == 0 {
		requestedTimeout = DefaultWorkflowExecutionTimeout
	}
	return timestamp.MinDuration(
		requestedTimeout,
		DefaultWorkflowExecutionTimeout,
	)
}

// GetWorkflowRunTimeout gets the default allowed run timeout or truncates the requested value to the maximum allowed timeout
func GetWorkflowRunTimeout(
	requestedTimeout time.Duration,
	executionTimeout time.Duration,
) time.Duration {
	if requestedTimeout == 0 {
		requestedTimeout = DefaultWorkflowRunTimeout
	}

	return timestamp.MinDuration(
		timestamp.MinDuration(
			requestedTimeout,
			executionTimeout,
		),
		DefaultWorkflowRunTimeout,
	)
}

// GetWorkflowTaskTimeout gets the default allowed execution timeout or truncates the requested value to the maximum allowed timeout
func GetWorkflowTaskTimeout(
	namespace string,
	requestedTimeout time.Duration,
	runTimeout time.Duration,
	getDefaultTimeoutFunc dynamicconfig.DurationPropertyFnWithNamespaceFilter,
) time.Duration {
	if requestedTimeout == 0 {
		requestedTimeout = getDefaultTimeoutFunc(namespace)
	}
	return timestamp.MinDuration(requestedTimeout, runTimeout)
}
