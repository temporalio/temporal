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

package frontend

import (
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	filterpb "go.temporal.io/temporal-proto/filter"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	querypb "go.temporal.io/temporal-proto/query"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	versionpb "go.temporal.io/temporal-proto/version"
)

func validateExecution(w *executionpb.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}
