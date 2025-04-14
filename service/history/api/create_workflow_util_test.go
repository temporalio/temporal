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

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestOverrideWorkflowRunTimeout_InfiniteRunTimeout_InfiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(0)
	executionTimeout := time.Duration(0)
	require.Equal(t, time.Duration(0), overrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_FiniteRunTimeout_InfiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(10)
	executionTimeout := time.Duration(0)
	require.Equal(t, time.Duration(10), overrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_InfiniteRunTimeout_FiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(0)
	executionTimeout := time.Duration(10)
	require.Equal(t, time.Duration(10), overrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_FiniteRunTimeout_FiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(100)
	executionTimeout := time.Duration(10)
	require.Equal(t, time.Duration(10), overrideWorkflowRunTimeout(runTimeout, executionTimeout))

	runTimeout = time.Duration(10)
	executionTimeout = time.Duration(100)
	require.Equal(t, time.Duration(10), overrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowTaskTimeout_Infinite(t *testing.T) {
	taskTimeout := time.Duration(0)
	runTimeout := time.Duration(100)
	defaultTimeout := time.Duration(20)
	defaultTimeoutFn := dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(20), overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(10)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(0)
	defaultTimeout = time.Duration(30)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(30), overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(0)
	defaultTimeout = maxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, maxWorkflowTaskStartToCloseTimeout, overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))
}

func TestOverrideWorkflowTaskTimeout_Finite(t *testing.T) {
	taskTimeout := time.Duration(10)
	runTimeout := maxWorkflowTaskStartToCloseTimeout - time.Duration(1)
	defaultTimeout := time.Duration(20)
	defaultTimeoutFn := dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = maxWorkflowTaskStartToCloseTimeout - time.Duration(1)
	runTimeout = time.Duration(10)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(10)
	runTimeout = maxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = maxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	runTimeout = maxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, maxWorkflowTaskStartToCloseTimeout, overrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))
}
