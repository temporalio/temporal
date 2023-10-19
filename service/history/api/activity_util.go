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
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"

	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/service/history/workflow"
)

func SetActivityTaskRunID(
	ctx context.Context,
	token *tokenspb.Task,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) error {
	// TODO when the following APIs are deprecated
	//  remove this function since run ID will always be set
	//  * RecordActivityTaskHeartbeatById
	//  * RespondActivityTaskCanceledById
	//  * RespondActivityTaskFailedById
	//  * RespondActivityTaskCompletedById

	if len(token.RunId) != 0 {
		return nil
	}

	runID, err := workflowConsistencyChecker.GetCurrentRunID(
		ctx,
		token.NamespaceId,
		token.WorkflowId,
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return err
	}
	token.RunId = runID
	return nil
}

func GetActivityScheduledEventID(
	activityID string,
	mutableState workflow.MutableState,
) (int64, error) {

	if activityID == "" {
		return 0, serviceerror.NewInvalidArgument("activityID cannot be empty")
	}
	activityInfo, ok := mutableState.GetActivityByActivityID(activityID)
	if !ok {
		return 0, serviceerror.NewNotFound(fmt.Sprintf("cannot find pending activity with ActivityID %s, check workflow execution history for more details", activityID))
	}
	return activityInfo.ScheduledEventId, nil
}
