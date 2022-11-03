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

package history

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/queues"
	shard "go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

func TestArchivalQueueTaskExecutor(t *testing.T) {
	startTime := time.Unix(1, 0)
	executionTime := startTime.Add(time.Second)
	closeTime := executionTime.Add(time.Minute)
	hourRetention := time.Hour
	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)
	version := 52
	for _, c := range []struct {
		Name                       string
		IsWorkflowExecutionRunning bool
		Retention                  *time.Duration
		Task                       tasks.Task
		ExpectedDeleteTime         time.Time
		ExpectedErrorSubstrings    []string
		ExpectArchive              bool
		ExpectAddTask              bool
	}{
		{
			Name:                       "Success",
			IsWorkflowExecutionRunning: false,
			Retention:                  &hourRetention,
			Task: &tasks.ArchiveExecutionTask{
				WorkflowKey: workflowKey,
				Version:     int64(version),
			},
			ExpectedDeleteTime: closeTime.Add(hourRetention),
			ExpectArchive:      true,
			ExpectAddTask:      true,
		},
		{
			Name:                       "Running execution",
			IsWorkflowExecutionRunning: true,
			Retention:                  &hourRetention,
			Task: &tasks.ArchiveExecutionTask{
				WorkflowKey: workflowKey,
				Version:     int64(version),
			},
			ExpectedDeleteTime:      closeTime.Add(hourRetention),
			ExpectedErrorSubstrings: []string{"cannot archive workflow which is running"},
			ExpectArchive:           false,
			ExpectAddTask:           false,
		},
		{
			Name:                       "Default retention",
			IsWorkflowExecutionRunning: false,
			Retention:                  nil,
			Task: &tasks.ArchiveExecutionTask{
				WorkflowKey: workflowKey,
				Version:     int64(version),
			},
			ExpectedDeleteTime: closeTime.Add(24 * time.Hour * 7),
			ExpectArchive:      true,
			ExpectAddTask:      true,
		},
		{
			Name: "Wrong task type",
			Task: &tasks.CloseExecutionTask{
				WorkflowKey: workflowKey,
			},
			ExpectedErrorSubstrings: []string{"task with invalid type"},
		},
	} {
		c := c // store c in closure to prevent loop from changing it when a parallel task is accessing it
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			namespaceRegistry := namespace.NewMockRegistry(controller)
			task := c.Task
			shardContext := shard.NewMockContext(controller)
			workflowCache := workflow.NewMockCache(controller)
			workflowContext := workflow.NewMockContext(controller)
			mutableState := workflow.NewMockMutableState(controller)
			branchToken := []byte{42}
			metricsHandler := metrics.NoopMetricsHandler
			logger := log.NewNoopLogger()
			timeSource := clock.NewRealTimeSource()
			archiver := archival.NewMockArchiver(controller)

			namespaceRegistry.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()
			mutableState.EXPECT().IsWorkflowExecutionRunning().Return(c.IsWorkflowExecutionRunning).AnyTimes()
			shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
			workflowContext.EXPECT().LoadMutableState(gomock.Any()).Return(mutableState, nil).AnyTimes()
			workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(workflowContext, workflow.ReleaseCacheFunc(func(err error) {}), nil).AnyTimes()

			if c.ExpectArchive {
				namespaceEntry := tests.GlobalNamespaceEntry.Clone(namespace.WithRetention(c.Retention))
				mutableState.EXPECT().GetCurrentBranchToken().Return(branchToken, nil)
				mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry)
				mutableState.EXPECT().GetNextEventID().Return(int64(100))
				mutableState.EXPECT().GetLastWriteVersion().Return(int64(200), nil)
				executionInfo := &persistence.WorkflowExecutionInfo{
					StartTime:     &startTime,
					ExecutionTime: &executionTime,
					CloseTime:     &closeTime,
				}
				mutableState.EXPECT().GetExecutionInfo().Return(executionInfo)
				executionState := &persistence.WorkflowExecutionState{
					State:  0,
					Status: 0,
				}
				mutableState.EXPECT().GetExecutionState().Return(executionState)
				shardContext.EXPECT().GetShardID().Return(int32(1))
				archiver.EXPECT().Archive(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context,
					request *archival.Request) (*archival.Response, error) {
					assert.Equal(t, startTime, request.StartTime)
					assert.Equal(t, executionTime, request.ExecutionTime)
					assert.Equal(t, closeTime, request.CloseTime)

					return &archival.Response{}, nil
				})
			}

			if c.ExpectAddTask {
				mutableState.EXPECT().AddTasks(&tasks.DeleteHistoryEventTask{
					WorkflowKey:                 workflowKey,
					VisibilityTimestamp:         c.ExpectedDeleteTime,
					TaskID:                      0,
					Version:                     int64(version),
					BranchToken:                 branchToken,
					WorkflowDataAlreadyArchived: true,
				})
			}

			executor := newArchivalQueueTaskExecutor(archiver, shardContext, workflowCache, metricsHandler, logger)
			executable := queues.NewExecutable(
				queues.DefaultReaderId,
				task,
				nil,
				executor,
				nil,
				nil,
				queues.NewNoopPriorityAssigner(),
				timeSource,
				namespaceRegistry,
				nil,
				metrics.NoopMetricsHandler,
				nil,
				nil,
			)
			err := executable.Execute()
			if len(c.ExpectedErrorSubstrings) > 0 {
				require.Error(t, err)
				for _, s := range c.ExpectedErrorSubstrings {
					assert.ErrorContains(t, err, s)
				}
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
