// Copyright (c) 2019 Temporal Technologies, Inc.
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

package adapter

import (
	"github.com/temporalio/temporal-proto/enums"
	"github.com/temporalio/temporal/.gen/go/shared"
)

func toThriftArchivalStatus(in enums.ArchivalStatus) *shared.ArchivalStatus {
	if in == enums.ArchivalStatusDefault {
		return nil
	}

	ret := shared.ArchivalStatus(in)
	return &ret
}

func toProtoArchivalStatus(in *shared.ArchivalStatus) enums.ArchivalStatus {
	if in == nil {
		return enums.ArchivalStatusDefault
	}
	return enums.ArchivalStatus(*in)
}

func toThriftTaskListKind(in enums.TaskListKind) *shared.TaskListKind {
	ret := shared.TaskListKind(in)
	return &ret
}
func toThriftWorkflowIDReusePolicy(in enums.WorkflowIdReusePolicy) *shared.WorkflowIdReusePolicy {
	ret := shared.WorkflowIdReusePolicy(in)
	return &ret
}
func toThriftHistoryEventFilterType(in enums.HistoryEventFilterType) *shared.HistoryEventFilterType {
	ret := shared.HistoryEventFilterType(in)
	return &ret
}

func toThriftWorkflowExecutionCloseStatus(in enums.WorkflowExecutionCloseStatus) *shared.WorkflowExecutionCloseStatus {
	ret := shared.WorkflowExecutionCloseStatus(in)
	return &ret
}

func toThriftDecisionType(in enums.DecisionType) *shared.DecisionType {
	ret := shared.DecisionType(in)
	return &ret
}

func toThriftContinueAsNewInitiator(in enums.ContinueAsNewInitiator) *shared.ContinueAsNewInitiator {
	ret := shared.ContinueAsNewInitiator(in)
	return &ret
}

func toThriftParentClosePolicy(in enums.ParentClosePolicy) *shared.ParentClosePolicy {
	ret := shared.ParentClosePolicy(in)
	return &ret
}

func toThriftQueryResultType(in enums.QueryResultType) *shared.QueryResultType {
	ret := shared.QueryResultType(in)
	return &ret
}
func toThriftDecisionTaskFailedCause(in enums.DecisionTaskFailedCause) *shared.DecisionTaskFailedCause {
	ret := shared.DecisionTaskFailedCause(in)
	return &ret
}
