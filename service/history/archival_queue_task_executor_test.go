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

package history_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/persistence/v1"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	cpersistence "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestArchivalQueueTaskExecutor(t *testing.T) {
	for _, c := range []testCase{
		{
			Name: "success",
			Configure: func(p *params) {
			},
		},
		{
			Name: "history archival disabled for cluster",
			Configure: func(p *params) {
				p.HistoryConfig.ClusterEnabled = false
				p.ExpectedTargets = []archival.Target{
					archival.TargetVisibility,
				}
			},
		},
		{
			Name: "URIs are not read for empty targets",
			Configure: func(p *params) {
				p.HistoryConfig.ClusterEnabled = false
				p.VisibilityConfig.ClusterEnabled = false
				// we set the URIs to invalid values which would produce errors if they were read
				// we should not read these URIs because history and visibility archival are disabled
				p.HistoryURI = "invalid_uri"
				p.VisibilityURI = "invalid_uri"
				p.ExpectArchive = false
			},
		},
		{
			Name: "history archival disabled for namespace",
			Configure: func(p *params) {
				p.HistoryConfig.NamespaceArchivalState = carchiver.ArchivalDisabled
				p.ExpectedTargets = []archival.Target{
					archival.TargetVisibility,
				}
			},
		},
		{
			Name: "visibility archival disabled for cluster",
			Configure: func(p *params) {
				p.VisibilityConfig.ClusterEnabled = false
				p.ExpectedTargets = []archival.Target{
					archival.TargetHistory,
				}
			},
		},
		{
			Name: "visibility archival disabled for namespace",
			Configure: func(p *params) {
				p.VisibilityConfig.NamespaceArchivalState = carchiver.ArchivalDisabled
				p.ExpectedTargets = []archival.Target{
					archival.TargetHistory,
				}
			},
		},
		{
			Name: "both history and visibility archival disabled",
			Configure: func(p *params) {
				p.VisibilityConfig.NamespaceArchivalState = carchiver.ArchivalDisabled
				p.HistoryConfig.NamespaceArchivalState = carchiver.ArchivalDisabled
				p.ExpectArchive = false
			},
		},
		{
			Name: "running execution",
			Configure: func(p *params) {
				p.IsWorkflowExecutionRunning = true
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "nil mutable state",
			Configure: func(p *params) {
				p.MutableStateExists = false
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "namespace not found",
			Configure: func(p *params) {
				p.GetNamespaceByIDError = &serviceerror.NamespaceNotFound{}
				// namespace not found means we should use default retention
				p.ExpectedDeleteTime = p.CloseTime.Add(24 * time.Hour)
			},
		},
		{
			Name: "get namespace internal error",
			Configure: func(p *params) {
				p.GetNamespaceByIDError = serviceerror.NewInternal("get namespace error")
				p.ExpectAddTask = false
				p.ExpectedErrorSubstrings = []string{
					"get namespace error",
				}
			},
		},
		{
			Name: "wrong task type",
			Configure: func(p *params) {
				p.Task = &tasks.DeleteExecutionTask{
					WorkflowKey: p.WorkflowKey,
				}
				p.ExpectArchive = false
				p.ExpectAddTask = false
				p.ExpectedErrorSubstrings = []string{"invalid type"}
			},
		},
		{
			Name: "invalid history URI",
			Configure: func(p *params) {
				p.HistoryURI = "invalid_uri"
				p.ExpectedErrorSubstrings = []string{"history URI", "parse"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
				mockCounter := metrics.NewMockCounterIface(p.Controller)
				mockCounter.EXPECT().Record(
					int64(1),
					metrics.NamespaceTag(tests.Namespace.String()),
					metrics.FailureTag("invalid_history_uri"),
				)
				p.MetricsHandler.EXPECT().Counter("archival_task_invalid_uri").Return(mockCounter)
			},
		},
		{
			Name: "invalid visibility URI",
			Configure: func(p *params) {
				p.VisibilityURI = "invalid_uri"
				p.ExpectedErrorSubstrings = []string{"visibility URI", "parse"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
				mockCounter := metrics.NewMockCounterIface(p.Controller)
				mockCounter.EXPECT().Record(
					int64(1),
					metrics.NamespaceTag(tests.Namespace.String()),
					metrics.FailureTag("invalid_visibility_uri"),
				)
				p.MetricsHandler.EXPECT().Counter("archival_task_invalid_uri").Return(mockCounter)
			},
		},
		{
			Name: "archiver error",
			Configure: func(p *params) {
				p.ArchiveError = errors.New("archiver error")
				p.ExpectedErrorSubstrings = []string{"archiver error"}
				p.ExpectAddTask = false
			},
		},
		{
			Name: "get workflow close time error",
			Configure: func(p *params) {
				p.GetWorkflowCloseTimeError = errors.New("get workflow close time error")
				p.ExpectedErrorSubstrings = []string{"get workflow close time error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "get workflow execution duration error",
			Configure: func(p *params) {
				p.GetWorkflowExecutionDurationError = errors.New("get workflow execution duration error")
				p.ExpectedErrorSubstrings = []string{"get workflow execution duration error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "get current branch token error",
			Configure: func(p *params) {
				p.GetCurrentBranchTokenError = errors.New("get current branch token error")
				p.ExpectedErrorSubstrings = []string{"get current branch token error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "load mutable state error",
			Configure: func(p *params) {
				p.LoadMutableStateError = errors.New("load mutable state error")
				p.ExpectedErrorSubstrings = []string{"load mutable state error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "get or create workflow execution error",
			Configure: func(p *params) {
				p.GetOrCreateWorkflowExecutionError = errors.New("get or create workflow execution error")
				p.ExpectedErrorSubstrings = []string{"get or create workflow execution error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "get close version error before archiving",
			Configure: func(p *params) {
				p.GetCloseVersionBeforeArchivalError = errors.New("get close version error")
				p.ExpectedErrorSubstrings = []string{"get close version error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "get close version error after archiving",
			Configure: func(p *params) {
				p.GetCloseVersionAfterArchivalError = errors.New("get close version error")
				p.ExpectedErrorSubstrings = []string{"get close version error"}
				p.ExpectArchive = true
				p.ExpectAddTask = false
			},
		},
		{
			Name: "mutable state version does not match task version",
			Configure: func(p *params) {
				p.CloseVersionBeforeArchival = 1
				p.Task.(*tasks.ArchiveExecutionTask).Version = 2
				p.ExpectedErrorSubstrings = []string{"version mismatch"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
		{
			Name: "close version changed during archival",
			Configure: func(p *params) {
				p.CloseVersionAfterArchival = p.CloseVersionBeforeArchival + 1
				p.ExpectedErrorSubstrings = []string{"version mismatch"}
				p.ExpectArchive = true
				p.ExpectAddTask = false
			},
		},
		{
			Name: "close visibility task complete",
			Configure: func(p *params) {
				p.RelocatableAttributesRemoved = true
			},
		},
		{
			Name: "get workflow execution from visibility error",
			Configure: func(p *params) {
				p.RelocatableAttributesRemoved = true
				p.GetWorkflowExecutionError = errors.New("get workflow execution error")
				p.ExpectedErrorSubstrings = []string{"get workflow execution error"}
				p.ExpectArchive = false
				p.ExpectAddTask = false
			},
		},
	} {
		c := c // store c in closure to prevent loop from changing it when a parallel task is accessing it
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			var p params
			p.Controller = gomock.NewController(t)
			p.HistoryConfig.NamespaceArchivalState = carchiver.ArchivalEnabled
			p.VisibilityConfig.NamespaceArchivalState = carchiver.ArchivalEnabled
			p.HistoryConfig.ClusterEnabled = true
			p.VisibilityConfig.ClusterEnabled = true
			p.WorkflowKey = definition.NewWorkflowKey(
				tests.NamespaceID.String(),
				tests.WorkflowID,
				tests.RunID,
			)
			p.StartTime = time.Unix(0, 0).UTC()
			p.ExecutionTime = time.Unix(0, 0).UTC()
			p.CloseTime = time.Unix(0, 0).UTC().Add(time.Minute * 2)
			p.ExecutionDuration = p.CloseTime.Sub(p.ExecutionTime)
			p.Retention = durationpb.New(time.Hour)
			// delete time = close time + retention
			// delete time = 2 minutes + 1 hour = 1 hour 2 minutes
			p.ExpectedDeleteTime = time.Unix(0, 0).UTC().Add(time.Minute * 2).Add(time.Hour)
			p.CloseVersionBeforeArchival = 1
			p.CloseVersionAfterArchival = 1
			p.Task = &tasks.ArchiveExecutionTask{
				WorkflowKey: p.WorkflowKey,
				Version:     1,
			}
			p.HistoryURI = "test://history/archival"
			p.VisibilityURI = "test://visibility/archival"
			p.ExpectedTargets = []archival.Target{
				archival.TargetHistory,
				archival.TargetVisibility,
			}
			p.ExpectArchive = true
			p.ExpectAddTask = true
			p.MetricsHandler = metrics.NewMockHandler(p.Controller)
			p.MutableStateExists = true

			c.Configure(&p)
			namespaceRegistry := namespace.NewMockRegistry(p.Controller)
			task := p.Task
			shardContext := shard.NewMockContext(p.Controller)
			workflowCache := cache.NewMockCache(p.Controller)
			workflowContext := workflow.NewMockContext(p.Controller)
			branchToken := []byte{42}
			logger := log.NewNoopLogger()
			timeSource := clock.NewRealTimeSource()
			a := archival.NewMockArchiver(p.Controller)

			shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
			cfg := tests.NewDynamicConfig()
			cfg.RetentionTimerJitterDuration = func() time.Duration {
				return 0
			}
			shardContext.EXPECT().GetConfig().Return(cfg).AnyTimes()
			mockMetadata := cluster.NewMockMetadata(p.Controller)
			mockMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			shardContext.EXPECT().GetClusterMetadata().Return(mockMetadata).AnyTimes()

			shardID := int32(1)
			historyArchivalState := p.HistoryConfig.NamespaceArchivalState
			visibilityArchivalState := p.VisibilityConfig.NamespaceArchivalState

			namespaceEntry := namespace.NewGlobalNamespaceForTest(
				&persistence.NamespaceInfo{
					Id:   tests.NamespaceID.String(),
					Name: tests.Namespace.String(),
				},
				&persistence.NamespaceConfig{
					Retention:               p.Retention,
					HistoryArchivalState:    enumspb.ArchivalState(historyArchivalState),
					HistoryArchivalUri:      p.HistoryURI,
					VisibilityArchivalState: enumspb.ArchivalState(visibilityArchivalState),
					VisibilityArchivalUri:   p.VisibilityURI,
				},
				&persistence.NamespaceReplicationConfig{
					ActiveClusterName: cluster.TestCurrentClusterName,
					Clusters: []string{
						cluster.TestCurrentClusterName,
					},
				},
				123,
			)
			namespaceRegistry.EXPECT().GetNamespaceName(namespaceEntry.ID()).
				Return(namespaceEntry.Name(), nil).AnyTimes()
			namespaceRegistry.EXPECT().GetNamespaceByID(namespaceEntry.ID()).
				Return(namespaceEntry, p.GetNamespaceByIDError).AnyTimes()

			if p.MutableStateExists {
				mutableState := workflow.NewMockMutableState(p.Controller)
				mutableState.EXPECT().IsWorkflowExecutionRunning().Return(p.IsWorkflowExecutionRunning).AnyTimes()
				mutableState.EXPECT().GetWorkflowKey().Return(p.WorkflowKey).AnyTimes()
				workflowContext.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(
					mutableState,
					p.LoadMutableStateError,
				).AnyTimes()
				mutableState.EXPECT().GetCurrentBranchToken().Return(
					branchToken,
					p.GetCurrentBranchTokenError,
				).AnyTimes()
				mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry).AnyTimes()
				mutableState.EXPECT().GetNextEventID().Return(int64(100)).AnyTimes()
				mutableState.EXPECT().GetCloseVersion().Return(
					p.CloseVersionBeforeArchival,
					p.GetCloseVersionBeforeArchivalError,
				).MaxTimes(1)
				mutableState.EXPECT().GetCloseVersion().Return(
					p.CloseVersionAfterArchival,
					p.GetCloseVersionAfterArchivalError,
				).MaxTimes(1)
				if p.ExpectAddTask {
					mutableState.EXPECT().GetCloseVersion().Return(p.CloseVersionBeforeArchival, nil).Times(1)
				}
				mutableState.EXPECT().GetWorkflowCloseTime(gomock.Any()).Return(
					p.CloseTime,
					p.GetWorkflowCloseTimeError,
				).AnyTimes()
				mutableState.EXPECT().GetWorkflowExecutionDuration(gomock.Any()).Return(
					p.ExecutionDuration,
					p.GetWorkflowExecutionDurationError,
				).AnyTimes()
				executionInfo := &persistence.WorkflowExecutionInfo{
					NamespaceId:                  tests.NamespaceID.String(),
					ExecutionTime:                timestamppb.New(p.ExecutionTime),
					CloseTime:                    timestamppb.New(p.CloseTime),
					RelocatableAttributesRemoved: p.RelocatableAttributesRemoved,
				}
				mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
				executionState := &persistence.WorkflowExecutionState{
					State:     0,
					Status:    0,
					StartTime: timestamppb.New(p.StartTime),
				}
				mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
				if p.ExpectAddTask {
					mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...*tasks.DeleteHistoryEventTask) {
						require.Len(t, ts, 1)
						task := ts[0]
						assert.Equal(t, p.WorkflowKey, task.WorkflowKey)
						assert.Zero(t, task.TaskID)
						assert.Equal(t, p.CloseVersionBeforeArchival, task.Version)
						assert.Equal(t, branchToken, task.BranchToken)
						assert.Equal(t, p.ExpectedDeleteTime, task.VisibilityTimestamp)
						popTasks := map[tasks.Category][]tasks.Task{
							tasks.CategoryTimer: {
								task,
							},
						}
						mutableState.EXPECT().PopTasks().Return(popTasks)
						shardContext.EXPECT().AddTasks(gomock.Any(), &cpersistence.AddHistoryTasksRequest{
							ShardID:     shardID,
							NamespaceID: tests.NamespaceID.String(),
							WorkflowID:  task.WorkflowID,
							Tasks:       popTasks,
						})
					})
				}
			} else {
				workflowContext.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(
					nil,
					p.LoadMutableStateError,
				).AnyTimes()
			}
			workflowCache.EXPECT().GetOrCreateWorkflowExecution(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Return(
				workflowContext,
				cache.ReleaseCacheFunc(func(err error) {}),
				p.GetOrCreateWorkflowExecutionError,
			).AnyTimes()

			archivalMetadata := carchiver.NewMockArchivalMetadata(p.Controller)
			historyConfig := carchiver.NewMockArchivalConfig(p.Controller)
			historyConfig.EXPECT().ClusterConfiguredForArchival().Return(p.HistoryConfig.ClusterEnabled).AnyTimes()
			archivalMetadata.EXPECT().GetHistoryConfig().Return(historyConfig).AnyTimes()
			visibilityConfig := carchiver.NewMockArchivalConfig(p.Controller)
			visibilityConfig.EXPECT().ClusterConfiguredForArchival().Return(p.VisibilityConfig.ClusterEnabled).AnyTimes()
			archivalMetadata.EXPECT().GetVisibilityConfig().Return(visibilityConfig).AnyTimes()
			shardContext.EXPECT().GetArchivalMetadata().Return(archivalMetadata).AnyTimes()
			shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()

			if p.ExpectArchive {
				a.EXPECT().Archive(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context,
					request *archival.Request) (*archival.Response, error) {
					assert.Equal(t, p.StartTime, request.StartTime.AsTime())
					assert.Equal(t, p.ExecutionTime, request.ExecutionTime.AsTime())
					assert.Equal(t, p.CloseTime, request.CloseTime.AsTime())
					assert.Equal(t, p.ExecutionDuration, request.ExecutionDuration.AsDuration())
					assert.ElementsMatch(t, p.ExpectedTargets, request.Targets)

					return &archival.Response{}, p.ArchiveError
				})
			}

			visibilityManager := manager.NewMockVisibilityManager(p.Controller)
			if p.RelocatableAttributesRemoved {
				visibilityManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&manager.GetWorkflowExecutionResponse{Execution: &workflowpb.WorkflowExecutionInfo{
						Memo:             nil,
						SearchAttributes: nil,
					}},
					p.GetWorkflowExecutionError,
				)
			}

			executor := history.NewArchivalQueueTaskExecutor(
				a,
				shardContext,
				workflowCache,
				workflow.RelocatableAttributesFetcherProvider(shardContext.GetConfig(), visibilityManager),
				p.MetricsHandler,
				logger,
			)
			executable := queues.NewExecutable(
				queues.DefaultReaderId,
				task,
				executor,
				nil,
				nil,
				queues.NewNoopPriorityAssigner(),
				timeSource,
				namespaceRegistry,
				mockMetadata,
				logger,
				metrics.NoopMetricsHandler,
			)
			err := executable.Execute()
			if len(p.ExpectedErrorSubstrings) > 0 {
				require.Error(t, err)
				for _, s := range p.ExpectedErrorSubstrings {
					assert.ErrorContains(t, err, s)
				}
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

// testCase represents a single test case for TestArchivalQueueTaskExecutor
type testCase struct {
	// Name is the name of the test case
	Name string
	// Configure is a function that takes the default params and modifies them for the test case
	Configure func(*params)
}

// params represents the parameters for a test within TestArchivalQueueTaskExecutor
type params struct {
	Controller                         *gomock.Controller
	IsWorkflowExecutionRunning         bool
	Retention                          *durationpb.Duration
	Task                               tasks.Task
	ExpectedDeleteTime                 time.Time
	ExpectedErrorSubstrings            []string
	ExpectArchive                      bool
	ExpectAddTask                      bool
	ExpectedTargets                    []archival.Target
	HistoryConfig                      archivalConfig
	VisibilityConfig                   archivalConfig
	WorkflowKey                        definition.WorkflowKey
	StartTime                          time.Time
	ExecutionTime                      time.Time
	CloseTime                          time.Time
	ExecutionDuration                  time.Duration
	GetNamespaceByIDError              error
	HistoryURI                         string
	VisibilityURI                      string
	MetricsHandler                     *metrics.MockHandler
	MutableStateExists                 bool
	ArchiveError                       error
	GetWorkflowCloseTimeError          error
	GetWorkflowExecutionDurationError  error
	GetCurrentBranchTokenError         error
	RelocatableAttributesRemoved       bool
	ExpectGetWorkflowExecution         bool
	GetWorkflowExecutionError          error
	LoadMutableStateError              error
	GetOrCreateWorkflowExecutionError  error
	CloseVersionBeforeArchival         int64
	GetCloseVersionBeforeArchivalError error
	CloseVersionAfterArchival          int64
	GetCloseVersionAfterArchivalError  error
}

// archivalConfig represents the user configuration of archival for the cluster and namespace
type archivalConfig struct {
	ClusterEnabled         bool
	NamespaceArchivalState carchiver.ArchivalState
}
