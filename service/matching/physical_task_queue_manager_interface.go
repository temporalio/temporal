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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination physical_task_queue_manager_mock.go

package matching

import (
	"context"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/taskqueue/v1"
)

type (
	physicalTaskQueueManager interface {
		Start()
		Stop(unloadCause)
		WaitUntilInitialized(context.Context) error
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// MarkAlive updates the liveness timer to keep this physicalTaskQueueManager alive.
		MarkAlive()
		// TrySyncMatch tries to match task to a local or remote poller. If not possible, returns false.
		TrySyncMatch(ctx context.Context, task *internalTask) (bool, error)
		// SpoolTask spools a task to persistence to be matched asynchronously when a poller is available.
		SpoolTask(taskInfo *persistencespb.TaskInfo) error
		ProcessSpooledTask(ctx context.Context, task *internalTask) error
		// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchSpooledTask(ctx context.Context, task *internalTask, userDataChanged <-chan struct{}) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskId string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// DispatchNexusTask dispatches a nexus task to a local or remote poller. If forwarded then result or
		// error is returned, if dispatched to local poller then nil and nil is returned.
		DispatchNexusTask(ctx context.Context, taskId string, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error)
		UpdatePollerInfo(pollerIdentity, *pollMetadata)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// LegacyDescribeTaskQueue returns pollers info and legacy TaskQueueStatus for this physical queue
		LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		GetStats() *taskqueuepb.TaskQueueStats
		GetInternalTaskQueueStatus() *taskqueue.InternalTaskQueueStatus
		UnloadFromPartitionManager(unloadCause)
		String() string
		QueueKey() *PhysicalTaskQueueKey
		// ShouldEmitGauges determines whether the gauge metrics should be emitted or not for this particular physical
		// queue based on dynamic configs.
		ShouldEmitGauges() bool
	}
)
