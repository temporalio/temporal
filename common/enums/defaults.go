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
	enumspb "go.temporal.io/api/enums/v1"
)

func SetDefaultWorkflowIdReusePolicy(f *enumspb.WorkflowIdReusePolicy) {
	if *f == enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED {
		*f = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
}

func SetDefaultHistoryEventFilterType(f *enumspb.HistoryEventFilterType) {
	if *f == enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED {
		*f = enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
	}
}

func SetDefaultTaskQueueKind(f *enumspb.TaskQueueKind) {
	if *f == enumspb.TASK_QUEUE_KIND_UNSPECIFIED {
		*f = enumspb.TASK_QUEUE_KIND_NORMAL
	}
}

func SetDefaultParentClosePolicy(f *enumspb.ParentClosePolicy) {
	if *f == enumspb.PARENT_CLOSE_POLICY_UNSPECIFIED {
		*f = enumspb.PARENT_CLOSE_POLICY_TERMINATE
	}
}

func SetDefaultQueryRejectCondition(f *enumspb.QueryRejectCondition) {
	if *f == enumspb.QUERY_REJECT_CONDITION_UNSPECIFIED {
		*f = enumspb.QUERY_REJECT_CONDITION_NONE
	}
}

func SetDefaultContinueAsNewInitiator(f *enumspb.ContinueAsNewInitiator) {
	if *f == enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED {
		*f = enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW
	}
}

func SetDefaultResetReapplyType(f *enumspb.ResetReapplyType) {
	if *f == enumspb.RESET_REAPPLY_TYPE_UNSPECIFIED {
		*f = enumspb.RESET_REAPPLY_TYPE_SIGNAL
	}
}
