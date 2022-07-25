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

package client

import (
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

var (
	CallerTypeAndAPIToPriority = map[string]map[string]int{
		headers.CallerTypeAPI: {
			"GetOrCreateShard":     0,
			"UpdateShard":          0,
			"AssertShardOwnership": 0,

			"CreateWorkflowExecution":           0,
			"UpdateWorkflowExecution":           0,
			"ConflictResolveWorkflowExecution":  0,
			"DeleteWorkflowExecution":           0,
			"DeleteCurrentWorkflowExecution":    0,
			"GetCurrentExecution":               0,
			"GetWorkflowExecution":              0,
			"SetWorkflowExecution":              0,
			"ListConcreteExecutions":            0,
			"AddHistoryTasks":                   0,
			"GetHistoryTask":                    0,
			"GetHistoryTasks":                   0,
			"CompleteHistoryTask":               0,
			"RangeCompleteHistoryTasks":         0,
			"PutReplicationTaskToDLQ":           0,
			"GetReplicationTasksFromDLQ":        0,
			"DeleteReplicationTaskFromDLQ":      0,
			"RangeDeleteReplicationTaskFromDLQ": 0,
			"AppendHistoryNodes":                0,
			"AppendRawHistoryNodes":             0,
			"ReadHistoryBranch":                 0,
			"ReadHistoryBranchByBatch":          0,
			"ReadHistoryBranchReverse":          0,
			"ReadRawHistoryBranch":              0,
			"ForkHistoryBranch":                 0,
			"DeleteHistoryBranch":               0,
			"TrimHistoryBranch":                 0,
			"GetHistoryTree":                    0,
			"GetAllHistoryTreeBranches":         0,

			"CreateTaskQueue":       0,
			"UpdateTaskQueue":       0,
			"GetTaskQueue":          0,
			"ListTaskQueue":         0,
			"DeleteTaskQueue":       0,
			"CreateTasks":           0,
			"GetTasks":              0,
			"CompleteTask":          0,
			"CompleteTasksLessThan": 0,

			"CreateNamespace":            0,
			"GetNamespace":               0,
			"UpdateNamespace":            0,
			"RenameNamespace":            0,
			"DeleteNamespace":            0,
			"DeleteNamespaceByName":      0,
			"ListNamespaces":             0,
			"GetMetadata":                0,
			"InitializeSystemNamespaces": 0,

			"GetClusterMembers":         0,
			"UpsertClusterMembership":   0,
			"PruneClusterMembership":    0,
			"ListClusterMetadata":       0,
			"GetCurrentClusterMetadata": 0,
			"GetClusterMetadata":        0,
			"SaveClusterMetadata":       0,
			"DeleteClusterMetadata":     0,
		},
		headers.CallerTypeBackground: {
			"GetOrCreateShard":     0,
			"UpdateShard":          0,
			"AssertShardOwnership": 1,

			"CreateWorkflowExecution":           1,
			"UpdateWorkflowExecution":           1,
			"ConflictResolveWorkflowExecution":  1,
			"DeleteWorkflowExecution":           1,
			"DeleteCurrentWorkflowExecution":    1,
			"GetCurrentExecution":               1,
			"GetWorkflowExecution":              1,
			"SetWorkflowExecution":              1,
			"ListConcreteExecutions":            1,
			"AddHistoryTasks":                   1,
			"GetHistoryTask":                    1,
			"GetHistoryTasks":                   1,
			"CompleteHistoryTask":               1,
			"RangeCompleteHistoryTasks":         0, // this is a preprequisite for updating ack level
			"PutReplicationTaskToDLQ":           1,
			"GetReplicationTasksFromDLQ":        1,
			"DeleteReplicationTaskFromDLQ":      1,
			"RangeDeleteReplicationTaskFromDLQ": 1,
			"AppendHistoryNodes":                1,
			"AppendRawHistoryNodes":             1,
			"ReadHistoryBranch":                 1,
			"ReadHistoryBranchByBatch":          1,
			"ReadHistoryBranchReverse":          1,
			"ReadRawHistoryBranch":              1,
			"ForkHistoryBranch":                 1,
			"DeleteHistoryBranch":               1,
			"TrimHistoryBranch":                 1,
			"GetHistoryTree":                    1,
			"GetAllHistoryTreeBranches":         1,

			"CreateTaskQueue":       1,
			"UpdateTaskQueue":       1,
			"GetTaskQueue":          1,
			"ListTaskQueue":         1,
			"DeleteTaskQueue":       1,
			"CreateTasks":           1,
			"GetTasks":              1,
			"CompleteTask":          1,
			"CompleteTasksLessThan": 1,

			"CreateNamespace":            1,
			"GetNamespace":               1,
			"UpdateNamespace":            1,
			"RenameNamespace":            1,
			"DeleteNamespace":            1,
			"DeleteNamespaceByName":      1,
			"ListNamespaces":             1,
			"GetMetadata":                1,
			"InitializeSystemNamespaces": 1,

			"GetClusterMembers":         1,
			"UpsertClusterMembership":   1,
			"PruneClusterMembership":    1,
			"ListClusterMetadata":       1,
			"GetCurrentClusterMetadata": 1,
			"GetClusterMetadata":        1,
			"SaveClusterMetadata":       1,
			"DeleteClusterMetadata":     1,
		},
	}

	RequestPrioritiesOrdered = []int{0, 1}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range RequestPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewDefaultOutgoingRateLimiter(rateFn)
	}

	return quotas.NewPriorityRateLimiter(
		func(req quotas.Request) int {
			if priority, ok := CallerTypeAndAPIToPriority[req.CallerType][req.API]; ok {
				return priority
			}

			// default requests to high priority to be consistent with existing behavior
			return RequestPrioritiesOrdered[0]
		},
		rateLimiters,
	)
}
