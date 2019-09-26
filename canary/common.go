// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"time"

	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func absDurationDiff(d1, d2 time.Duration) time.Duration {
	if d1 > d2 {
		return d1 - d2
	}
	return d2 - d1
}

// getContextValue retrieves and returns the value corresponding
// to the given key - panics if the key does not exist
func getContextValue(ctx context.Context, key string) interface{} {
	value := ctx.Value(key)
	if value == nil {
		panic("ctx.Value(" + key + ") returned nil")
	}
	return value
}

// getActivityContext retrieves and returns the activity context from the
// global context passed to the activity
func getActivityContext(ctx context.Context) *activityContext {
	return getContextValue(ctx, ctxKeyActivityRuntime).(*activityContext)
}

// getActivityArchivalContext retrieves and returns the activity archival context from the
// global context passed to the activity
func getActivityArchivalContext(ctx context.Context) *activityContext {
	return getContextValue(ctx, ctxKeyActivityArchivalRuntime).(*activityContext)
}

// checkWFVersionCompatibility takes a workflow.Context param and
// validates that the workflow task currently being handled
// is compatible with this version of the canary - this method
// MUST only be called within a workflow function and it MUST
// be the first line in the workflow function
// Returns an error if the version is incompatible
func checkWFVersionCompatibility(ctx workflow.Context) error {
	version := workflow.GetVersion(ctx, workflowChangeID, workflowVersion, workflowVersion)
	if version != workflowVersion {
		workflow.GetLogger(ctx).Error("workflow version mismatch",
			zap.Int("want", int(workflowVersion)), zap.Int("got", int(version)))
		return fmt.Errorf("workflow version mismatch, want=%v, got=%v", workflowVersion, version)
	}
	return nil
}

// beginWorkflow executes the common steps involved in all the workflow functions
// It checks for workflow task version compatibility and also records the execution
// in m3. This function must be the first call in every workflow function
// Returns metrics scope on success, error on failure
func beginWorkflow(ctx workflow.Context, wfType string, scheduledTimeNanos int64) (*workflowMetricsProfile, error) {
	profile := recordWorkflowStart(ctx, wfType, scheduledTimeNanos)
	if err := checkWFVersionCompatibility(ctx); err != nil {
		profile.scope.Counter(errIncompatibleVersion).Inc(1)
		return nil, err
	}
	return profile, nil
}

func concat(first string, second string) string {
	return first + "/" + second
}
