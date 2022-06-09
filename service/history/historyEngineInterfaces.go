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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination historyEngineInterfaces_mock.go

package history

import (
	"context"
	"time"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

type (
	queueProcessor interface {
		common.Daemon
		notifyNewTask()
	}

	// ReplicatorQueueProcessor is the interface for replicator queue processor
	ReplicatorQueueProcessor interface {
		queueProcessor
		getTasks(
			ctx context.Context,
			pollingCluster string,
			lastReadTaskID int64,
		) (*replicationspb.ReplicationMessages, error)
		getTask(
			ctx context.Context,
			taskInfo *replicationspb.ReplicationTaskInfo,
		) (*replicationspb.ReplicationTask, error)
	}

	queueAckMgr interface {
		getFinishedChan() <-chan struct{}
		readQueueTasks() ([]queues.Executable, bool, error)
		getQueueAckLevel() int64
		getQueueReadLevel() int64
		updateQueueAckLevel() error
	}

	processor interface {
		readTasks(readLevel int64) ([]tasks.Task, bool, error)
		updateAckLevel(taskID int64) error
		queueShutdown() error
	}

	timerProcessor interface {
		notifyNewTimers(timerTask []tasks.Task)
	}

	timerQueueAckMgr interface {
		getFinishedChan() <-chan struct{}
		readTimerTasks() ([]queues.Executable, *time.Time, bool, error)
		getAckLevel() tasks.Key
		getReadLevel() tasks.Key
		updateAckLevel() error
	}

	taskExecutableInitializer func(tasks.Task) queues.Executable
)
