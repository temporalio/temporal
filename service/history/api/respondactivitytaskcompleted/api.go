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

package respondactivitytaskcompleted

import (
	"context"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCompletedRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RespondActivityTaskCompletedResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespace := namespaceEntry.Name()

	tokenSerializer := common.NewProtoTaskTokenSerializer()
	request := req.CompleteRequest
	token, err0 := tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}
	if err := api.SetActivityTaskRunID(ctx, token, workflowConsistencyChecker); err != nil {
		return nil, err
	}

	var activityStartedTime time.Time
	var taskQueue string
	var workflowTypeName string
	var fabricateStartedEvent bool
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		token.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			token.NamespaceId,
			token.WorkflowId,
			token.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			workflowTypeName = mutableState.GetWorkflowType().GetName()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduledEventID := token.GetScheduledEventId()
			isCompletedByID := false
			if scheduledEventID == common.EmptyEventID { // client call CompleteActivityById, so get scheduledEventID by activityID
				isCompletedByID = true
				scheduledEventID, err0 = api.GetActivityScheduledEventID(token.GetActivityId(), mutableState)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduledEventID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(shard.GetMetricsHandler()).Record(
					1,
					metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope))
				return nil, consts.ErrStaleState
			}

			if !isRunning ||
				(!isCompletedByID && ai.StartedEventId == common.EmptyEventID) ||
				(token.GetScheduledEventId() != common.EmptyEventID && token.Attempt != ai.Attempt) ||
				(token.GetVersion() != common.EmptyVersion && token.Version != ai.Version) {
				return nil, consts.ErrActivityTaskNotFound
			}

			// We fabricate a started event only when the activity is not started yet and
			// we need to force complete an activity
			fabricateStartedEvent = ai.StartedEventId == common.EmptyEventID
			if fabricateStartedEvent {
				_, err := mutableState.AddActivityTaskStartedEvent(ai, scheduledEventID,
					"",
					req.GetCompleteRequest().GetIdentity(),
					nil,
					nil,
				)
				if err != nil {
					return nil, err
				}
			}

			ai, _ = mutableState.GetActivityInfo(scheduledEventID)
			if _, err = mutableState.AddActivityTaskCompletedEvent(scheduledEventID, ai.StartedEventId, request); err != nil {
				// Unable to add ActivityTaskCompleted event to history
				return nil, err
			}
			activityStartedTime = ai.StartedTime.AsTime()
			taskQueue = ai.TaskQueue
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)

	if err == nil && !activityStartedTime.IsZero() && !fabricateStartedEvent {
		metrics.ActivityE2ELatency.With(shard.GetMetricsHandler()).Record(
			time.Since(activityStartedTime),
			metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope),
			metrics.NamespaceTag(namespace.String()),
			metrics.WorkflowTypeTag(workflowTypeName),
			metrics.ActivityTypeTag(token.ActivityType),
			metrics.TaskQueueTag(taskQueue),
		)
	}
	return &historyservice.RespondActivityTaskCompletedResponse{}, err
}
