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

package configs

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

const (
	// OperatorPriority is used to give precedence to calls coming from web UI or tctl
	OperatorPriority = 0
)

var (
	APIToPriority = map[string]int{
		"/temporal.server.api.historyservice.v1.HistoryService/CloseShard":                             1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetShard":                               1,
		"/temporal.server.api.historyservice.v1.HistoryService/DeleteWorkflowExecution":                1,
		"/temporal.server.api.historyservice.v1.HistoryService/DescribeHistoryHost":                    1,
		"/temporal.server.api.historyservice.v1.HistoryService/DescribeMutableState":                   1,
		"/temporal.server.api.historyservice.v1.HistoryService/DescribeWorkflowExecution":              1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetDLQMessages":                         1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetDLQReplicationMessages":              1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetMutableState":                        1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetReplicationMessages":                 1,
		"/temporal.server.api.historyservice.v1.HistoryService/ImportWorkflowExecution":                1,
		"/temporal.server.api.historyservice.v1.HistoryService/IsActivityTaskValid":                    1,
		"/temporal.server.api.historyservice.v1.HistoryService/IsWorkflowTaskValid":                    1,
		"/temporal.server.api.historyservice.v1.HistoryService/MergeDLQMessages":                       1,
		"/temporal.server.api.historyservice.v1.HistoryService/PollMutableState":                       1,
		"/temporal.server.api.historyservice.v1.HistoryService/PurgeDLQMessages":                       1,
		"/temporal.server.api.historyservice.v1.HistoryService/QueryWorkflow":                          1,
		"/temporal.server.api.historyservice.v1.HistoryService/ReapplyEvents":                          1,
		"/temporal.server.api.historyservice.v1.HistoryService/RebuildMutableState":                    1,
		"/temporal.server.api.historyservice.v1.HistoryService/RecordActivityTaskHeartbeat":            1,
		"/temporal.server.api.historyservice.v1.HistoryService/RecordActivityTaskStarted":              1,
		"/temporal.server.api.historyservice.v1.HistoryService/RecordChildExecutionCompleted":          1,
		"/temporal.server.api.historyservice.v1.HistoryService/VerifyChildExecutionCompletionRecorded": 1,
		"/temporal.server.api.historyservice.v1.HistoryService/RecordWorkflowTaskStarted":              1,
		"/temporal.server.api.historyservice.v1.HistoryService/RefreshWorkflowTasks":                   1,
		"/temporal.server.api.historyservice.v1.HistoryService/RemoveSignalMutableState":               1,
		"/temporal.server.api.historyservice.v1.HistoryService/RemoveTask":                             1,
		"/temporal.server.api.historyservice.v1.HistoryService/ReplicateEventsV2":                      1,
		"/temporal.server.api.historyservice.v1.HistoryService/ReplicateWorkflowState":                 1,
		"/temporal.server.api.historyservice.v1.HistoryService/RequestCancelWorkflowExecution":         1,
		"/temporal.server.api.historyservice.v1.HistoryService/ResetStickyTaskQueue":                   1,
		"/temporal.server.api.historyservice.v1.HistoryService/ResetWorkflowExecution":                 1,
		"/temporal.server.api.historyservice.v1.HistoryService/RespondActivityTaskCanceled":            1,
		"/temporal.server.api.historyservice.v1.HistoryService/RespondActivityTaskCompleted":           1,
		"/temporal.server.api.historyservice.v1.HistoryService/RespondActivityTaskFailed":              1,
		"/temporal.server.api.historyservice.v1.HistoryService/RespondWorkflowTaskCompleted":           1,
		"/temporal.server.api.historyservice.v1.HistoryService/RespondWorkflowTaskFailed":              1,
		"/temporal.server.api.historyservice.v1.HistoryService/ScheduleWorkflowTask":                   1,
		"/temporal.server.api.historyservice.v1.HistoryService/VerifyFirstWorkflowTaskScheduled":       1,
		"/temporal.server.api.historyservice.v1.HistoryService/SignalWithStartWorkflowExecution":       1,
		"/temporal.server.api.historyservice.v1.HistoryService/SignalWorkflowExecution":                1,
		"/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution":                 1,
		"/temporal.server.api.historyservice.v1.HistoryService/SyncActivity":                           1,
		"/temporal.server.api.historyservice.v1.HistoryService/SyncShardStatus":                        1,
		"/temporal.server.api.historyservice.v1.HistoryService/TerminateWorkflowExecution":             1,
		"/temporal.server.api.historyservice.v1.HistoryService/GenerateLastHistoryReplicationTasks":    1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetReplicationStatus":                   1,
		"/temporal.server.api.historyservice.v1.HistoryService/DeleteWorkflowVisibilityRecord":         1,
		"/temporal.server.api.historyservice.v1.HistoryService/UpdateWorkflowExecution":                1,
		"/temporal.server.api.historyservice.v1.HistoryService/PollWorkflowExecutionUpdate":            1,
		"/temporal.server.api.historyservice.v1.HistoryService/ExecuteMultiOperation":                  1,
		"/temporal.server.api.historyservice.v1.HistoryService/StreamWorkflowReplicationMessages":      1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetWorkflowExecutionHistory":            1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetWorkflowExecutionHistoryReverse":     1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetWorkflowExecutionRawHistory":         1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetWorkflowExecutionRawHistoryV2":       1,
		"/temporal.server.api.historyservice.v1.HistoryService/ForceDeleteWorkflowExecution":           1,
		"/temporal.server.api.historyservice.v1.HistoryService/GetDLQTasks":                            1,
		"/temporal.server.api.historyservice.v1.HistoryService/DeleteDLQTasks":                         1,
		"/temporal.server.api.historyservice.v1.HistoryService/AddTasks":                               1,
		"/temporal.server.api.historyservice.v1.HistoryService/ListQueues":                             1,
		"/temporal.server.api.historyservice.v1.HistoryService/ListTasks":                              1,
		"/temporal.server.api.historyservice.v1.HistoryService/CompleteNexusOperation":                 1,
	}

	APIPrioritiesOrdered = []int{OperatorPriority, 1}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range APIPrioritiesOrdered {
		if priority == OperatorPriority {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultIncomingRateLimiter(operatorRateFn(rateFn, operatorRPSRatio)))
		} else {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultIncomingRateLimiter(rateFn))
		}
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if req.CallerType == headers.CallerTypeOperator {
			return OperatorPriority
		}
		if priority, ok := APIToPriority[req.API]; ok {
			return priority
		}
		return APIPrioritiesOrdered[len(APIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func operatorRateFn(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RateFn {
	return func() float64 {
		return operatorRPSRatio() * rateFn()
	}
}
