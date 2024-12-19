// The MIT License
//
// Copyright (pm) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (pm) 2020 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_queue_partition_manager_mock.go

package matching

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

type (
	// TODO: adopt interface/mock combo in tests for looser coupling between MatchingEngine and PartitionManagers
	taskQueuePartitionManager interface {
		Start()
		Stop(unloadCause)
		Namespace() *namespace.Namespace
		WaitUntilInitialized(context.Context) error
		// AddTask adds a task to the task queue. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		// Returns the build ID assigned to the task according to the assignment rules (if any),
		// and a boolean indicating if sync-match happened or not.
		AddTask(ctx context.Context, params addTaskParams) (buildId string, syncMatch bool, err error)
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, bool, error)
		// ProcessSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		ProcessSpooledTask(ctx context.Context, task *internalTask, assignedBuildId string) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskId string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// DispatchNexusTask dispatches a nexus task to a local or remote poller. If forwarded then result or
		// error is returned, if dispatched to local poller then nil and nil is returned.
		DispatchNexusTask(ctx context.Context, taskId string, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error)
		GetUserDataManager() userDataManager
		// MarkAlive updates the liveness timer to keep this partition manager alive.
		MarkAlive()
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		// HasPollerAfter checks pollers on the queue associated with the given buildId, or the unversioned queue if an empty string is given
		HasPollerAfter(buildId string, accessTime time.Time) bool
		// HasAnyPollerAfter checks pollers on all versioned and unversioned queues
		HasAnyPollerAfter(accessTime time.Time) bool
		// LegacyDescribeTaskQueue returns information about all pollers of this partition and the status of its unversioned physical queue
		LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		Describe(ctx context.Context, buildIds map[string]bool, includeAllActive, reportStats, reportPollers, internalTaskQueueStatus bool) (*matchingservice.DescribeTaskQueuePartitionResponse, error)
		String() string
		Partition() tqid.Partition
		LongPollExpirationInterval() time.Duration
		// TimeSinceLastFanOut returns the time since the last DescribeTaskQueuePartition fan out
		TimeSinceLastFanOut() time.Duration
		// UpdateTimeSinceLastFanOutAndCache updates the cache and it's TTL
		UpdateTimeSinceLastFanOutAndCache(physicalInfoByBuildId map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
		// GetPhysicalTaskQueueInfoFromCache returns the cached physicalInfoByBuildId
		GetPhysicalTaskQueueInfoFromCache() map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo
	}
)
