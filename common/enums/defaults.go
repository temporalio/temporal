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
	commonpb "go.temporal.io/temporal-proto/common"
	filterpb "go.temporal.io/temporal-proto/filter"
	querypb "go.temporal.io/temporal-proto/query"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
)

func SetDefaultWorkflowIdReusePolicy(f *commonpb.WorkflowIdReusePolicy){
	if *f == commonpb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED{
		*f = commonpb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
}

func SetDefaultContinueAsNewInitiator(f *commonpb.ContinueAsNewInitiator){
	if *f == commonpb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED{
		*f = commonpb.CONTINUE_AS_NEW_INITIATOR_DECIDER
	}
}

func SetDefaultQueryConsistencyLevel(f *querypb.QueryConsistencyLevel){
	if *f == querypb.QUERY_CONSISTENCY_LEVEL_UNSPECIFIED{
		*f = querypb.QUERY_CONSISTENCY_LEVEL_EVENTUAL
	}
}

func SetDefaultHistoryEventFilterType(f *filterpb.HistoryEventFilterType){
	if *f == filterpb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED{
		*f = filterpb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
	}
}

func SetDefaultTasklListType(f *tasklistpb.TaskListType){
	if *f == tasklistpb.TASK_LIST_TYPE_UNSPECIFIED{
		*f = tasklistpb.TASK_LIST_TYPE_DECISION
	}
}

func SetDefaultTasklListKind(f *tasklistpb.TaskListKind){
	if *f == tasklistpb.TASK_LIST_KIND_UNSPECIFIED{
		*f = tasklistpb.TASK_LIST_KIND_NORMAL
	}
}
