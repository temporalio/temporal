// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

//go:build utest

package workflow

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type MutableStateStub struct {
	MutableStateImpl
}

func InitializeMutableState(namespace *namespace.Namespace, logger log.Logger, config *configs.Config, registry namespace.Registry, metadata cluster.Metadata, shard shard.Context) MutableState {
	ts := clock.NewRealTimeSource()
	simpleIdGenerator := func(n int) ([]int64, error) {
		ids := make([]int64, n)
		for i := 0; i < n; i++ {
			ids[i] = int64(100 + i)
		}
		return ids, nil
	}
	builder := &HistoryBuilder{timeSource: ts, taskIDGenerator: simpleIdGenerator}
	oneItemHistory := make([]*history.VersionHistory, 1)
	oneItemHistory[0] = &history.VersionHistory{}
	info := &persistencespb.WorkflowExecutionInfo{
		VersionHistories: &history.VersionHistories{
			Histories: oneItemHistory,
		},
		ExecutionStats: &persistencespb.ExecutionStats{},
	}
	state := &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED}
	insertTasks := make(map[tasks.Category][]tasks.Task)
	ms := &MutableStateStub{
		MutableStateImpl{
			config:          config,
			hBuilder:        builder,
			executionInfo:   info,
			executionState:  state,
			namespaceEntry:  namespace,
			timeSource:      ts,
			logger:          logger,
			InsertTasks:     insertTasks,
			clusterMetadata: metadata,
			shard:           shard,
			QueryRegistry:   NewQueryRegistry(),
		},
	}
	ms.workflowTaskManager = newWorkflowTaskStateMachine(&ms.MutableStateImpl)
	ms.taskGenerator = NewTaskGenerator(registry, ms, config, nil)
	return ms
}

func (ms *MutableStateStub) StartTransaction(_ *namespace.Namespace) (bool, error) {
	return false, nil
}
