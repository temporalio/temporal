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

package queues

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination queue_mock.go

type (
	Queue interface {
		common.Daemon
		Category() tasks.Category
		NotifyNewTasks(clusterName string, tasks []tasks.Task)
		FailoverNamespace(namespaceIDs map[string]struct{})
		LockTaskProcessing()
		UnlockTaskProcessing()
	}

	Factory interface {
		common.Daemon

		// TODO: remove the cache parameter after workflow cache become a host level component
		// and it can be provided as a parameter when creating a QueueFactory instance.
		// Currently, workflow cache is shard level, but we can't get it from shard or engine interface,
		// as that will lead to a cycle dependency issue between shard and workflow package.
		CreateQueue(shard shard.Context, engine shard.Engine, cache workflow.Cache) Queue
	}
)

const (
	FactoryFxGroup = "queueFactory"
)

// TODO: remove QueueType after merging active and standby
// transfer/timer queue. Use tasks.Category instead
// Currently need queue active/standby information
// for assigning priority
type (
	QueueType int
)

const (
	QueueTypeUnknown QueueType = iota
	// QueueTypeTransfer is used by single cursor transfer queue, which
	// processes both active and standby task
	QueueTypeTransfer
	QueueTypeActiveTransfer
	QueueTypeStandbyTransfer
	// QueueTypeTimer is used by single cursor timer queue, which
	// processes both active and standby task
	QueueTypeTimer
	QueueTypeActiveTimer
	QueueTypeStandbyTimer
	QueueTypeVisibility
)
