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

//go:build !race

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/tests"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This suite contains tests of scenarios in which conflicting histories arise during history replication. To do that we
// need to create "split-brain" sitauations in which both clusters believe they are active, and to do that, we need to
// control when history and namespace event replication tasks are executed. This is achieved using injection approaches
// based on those in tests/xdc/history_replication_dlq_test.go.

type (
	historyReplicationConflictTestSuite struct {
		xdcBaseSuite
		namespaceReplicationTasks chan *replicationspb.NamespaceTaskAttributes
		namespaceTaskExecutor     namespace.ReplicationTaskExecutor
		historyReplicationTasks   map[string]chan *hrcTestExecutableTask
		tv                        *testvars.TestVars
	}
	hrcTestNamespaceReplicationTaskExecutor struct {
		replicationTaskExecutor namespace.ReplicationTaskExecutor
		s                       *historyReplicationConflictTestSuite
	}
	hrcTestExecutableTaskConverter struct {
		converter replication.ExecutableTaskConverter
		s         *historyReplicationConflictTestSuite
	}
	hrcTestExecutableTask struct {
		s *historyReplicationConflictTestSuite
		replication.TrackableExecutableTask
		replicationTask *replicationspb.ReplicationTask
		taskClusterName string
		result          chan error
	}
)

const (
	taskBufferCapacity = 100
)

func TestHistoryReplicationConflictTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(historyReplicationConflictTestSuite))
}

func (s *historyReplicationConflictTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.EnableReplicationStream: true,
	}
	s.logger = log.NewNoopLogger()
	s.setupSuite(
		[]string{"cluster1", "cluster2"},
		tests.WithFxOptionsForService(primitives.WorkerService,
			fx.Decorate(
				func(executor namespace.ReplicationTaskExecutor) namespace.ReplicationTaskExecutor {
					s.namespaceTaskExecutor = executor
					return &hrcTestNamespaceReplicationTaskExecutor{
						replicationTaskExecutor: executor,
						s:                       s,
					}
				},
			),
		),
		tests.WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(
				func(converter replication.ExecutableTaskConverter) replication.ExecutableTaskConverter {
					return &hrcTestExecutableTaskConverter{
						converter: converter,
						s:         s,
					}
				},
			),
		),
	)
	s.namespaceReplicationTasks = make(chan *replicationspb.NamespaceTaskAttributes, taskBufferCapacity)
	s.historyReplicationTasks = map[string]chan *hrcTestExecutableTask{}
	for _, c := range s.clusterNames {
		s.historyReplicationTasks[c] = make(chan *hrcTestExecutableTask, taskBufferCapacity)
	}
}

func (s *historyReplicationConflictTestSuite) SetupTest() {
	s.setupTest()
}

func (s *historyReplicationConflictTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestConflictResolutionReappliesSignals creates a split-brain scenario in which both clusters believe they are active.
// Both clusters then accept a signal and write it to their own history, and the test confirms that the signal is
// reapplied during the resulting conflict resolution process.
func (s *historyReplicationConflictTestSuite) TestConflictResolutionReappliesSignals() {
	s.tv = testvars.New(s.T().Name())
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()
	sdkClient1, sdkClient2 := s.createSdkClients()

	s.registerMultiRegionNamespace(ctx)
	runId := s.startWorkflow(ctx, sdkClient1)
	s.enterSplitBrainState(ctx)

	// Both clusters now believe they are active and hence both will accept a signal.

	// Send signals
	s.NoError(sdkClient1.SignalWorkflow(ctx, s.tv.WorkflowID(), runId, "my-signal", "cluster1-signal"))
	s.NoError(sdkClient2.SignalWorkflow(ctx, s.tv.WorkflowID(), runId, "my-signal", "cluster2-signal"))

	// cluster1 has accepted a signal
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster1-signal\""}]}}
	`, s.getHistory(ctx, s.cluster1, runId), []int{1, 1, 1})

	// cluster2 has also accepted a signal (with failover version 2 since it is endogenous to cluster 2)
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	`, s.getHistory(ctx, s.cluster2, runId), []int{1, 1, 2})

	// Execute pending history replication tasks. Both clusters believe they are active, therefore each cluster sends
	// its signal to the other, triggering conflict resolution.

	// cluster2 sends its signal to cluster1. Since it has a higher failover version, it supersedes the endogenous
	// signal in cluster1.
	s.executeHistoryReplicationTasksFromClusterUntil("cluster2", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	`, s.getHistory(ctx, s.cluster1, runId), []int{1, 1, 2})

	// cluster1 sends its signal to cluster1. Since it has a lower failover version, it is reapplied after the
	// endogenous cluster 2 signal.
	s.executeHistoryReplicationTasksFromClusterUntil("cluster1", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster2-signal\""}]}}
	4 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster1-signal\""}]}}
	`, s.getHistory(ctx, s.cluster2, runId), []int{1, 1, 2, 2})

	// Cluster2 sends the reapplied signal to cluster1, bringing the cluster histories into agreement.
	s.executeHistoryReplicationTasksFromClusterUntil("cluster2", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.EqualValues(
		s.getHistory(ctx, s.cluster1, runId),
		s.getHistory(ctx, s.cluster2, runId),
	)
}

func (s *historyReplicationConflictTestSuite) enterSplitBrainState(ctx context.Context) {
	// We now create a "split brain" state by setting cluster2 to active. We do not execute namespace replication tasks
	// afterward, so cluster1 does not learn of the change.
	s.Equal([]string{"cluster1", "cluster1"}, s.getActiveClusters(ctx))
	s.setActive(ctx, s.cluster2, "cluster2")
	s.Equal([]string{"cluster1", "cluster2"}, s.getActiveClusters(ctx))

	// TODO (dan) Why do the tests still pass with this? Does this not remove the split-brain?
	// s.executeNamespaceReplicationTasksUntil(ctx, enumsspb.NAMESPACE_OPERATION_UPDATE, 2)

	// Wait for active cluster to be changed in namespace registry entry.
	// TODO (dan) It would be nice to find a better approach.
	time.Sleep(tests.NamespaceCacheRefreshInterval)
}

// executeNamespaceReplicationTasksUntil executes buffered namespace event replication tasks until the specified event
// type is encountered with the specified failover version.
func (s *historyReplicationConflictTestSuite) executeNamespaceReplicationTasksUntil(ctx context.Context, operation enumsspb.NamespaceOperation, version int64) {
	for {
		task := <-s.namespaceReplicationTasks
		err := s.namespaceTaskExecutor.Execute(ctx, task)
		s.NoError(err)
		if task.NamespaceOperation == operation && task.FailoverVersion == version {
			return
		}
	}
}

// executeHistoryReplicationTasksFromClusterUntil executes buffered history event replication tasks until the specified event type
// is encountered for the workflowId.
func (s *historyReplicationConflictTestSuite) executeHistoryReplicationTasksFromClusterUntil(
	sourceCluster string,
	eventType enumspb.EventType,
) {
	for {
		t := <-s.historyReplicationTasks[sourceCluster]
		events := s.executeHistoryReplicationTask(t)
		for _, event := range events {
			if event.GetEventType() == eventType {
				return
			}
		}
	}
}

func (s *historyReplicationConflictTestSuite) executeHistoryReplicationTask(task *hrcTestExecutableTask) []*historypb.HistoryEvent {
	serializer := serialization.NewSerializer()
	trackableTask := (*task).TrackableExecutableTask
	err := trackableTask.Execute()
	s.NoError(err)
	task.result <- err
	attrs := (*task).replicationTask.GetHistoryTaskAttributes()
	s.NotNil(attrs)
	s.Equal(s.tv.WorkflowID(), attrs.WorkflowId)
	events, err := serializer.DeserializeEvents(attrs.Events)
	s.NoError(err)
	return events
}

func (c *hrcTestNamespaceReplicationTaskExecutor) Execute(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) error {
	c.s.namespaceReplicationTasks <- task
	// Report success, although we have merely buffered the task and will execute it later.
	return nil
}

// Convert the replication tasks using the base converter, and wrap them in our own executable tasks.
func (t *hrcTestExecutableTaskConverter) Convert(
	taskClusterName string,
	clientShardKey replication.ClusterShardKey,
	serverShardKey replication.ClusterShardKey,
	replicationTasks ...*replicationspb.ReplicationTask,
) []replication.TrackableExecutableTask {
	convertedTasks := t.converter.Convert(taskClusterName, clientShardKey, serverShardKey, replicationTasks...)
	testExecutableTasks := make([]replication.TrackableExecutableTask, len(convertedTasks))
	for i, task := range convertedTasks {
		testExecutableTasks[i] = &hrcTestExecutableTask{
			taskClusterName:         taskClusterName,
			s:                       t.s,
			TrackableExecutableTask: task,
			replicationTask:         replicationTasks[i],
			result:                  make(chan error),
		}
	}
	return testExecutableTasks
}

// Execute pushes the task to a buffer and waits for it to be executed.
func (t *hrcTestExecutableTask) Execute() error {
	t.s.historyReplicationTasks[t.taskClusterName] <- t
	return <-t.result
}

// gRPC utilities

func (s *historyReplicationConflictTestSuite) createSdkClients() (sdkclient.Client, sdkclient.Client) {
	c1, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: s.tv.NamespaceName().String(),
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	c2, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.GetHost().FrontendGRPCAddress(),
		Namespace: s.tv.NamespaceName().String(),
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	return c1, c2
}

func (s *historyReplicationConflictTestSuite) startWorkflow(ctx context.Context, cluster1Client sdkclient.Client) string {
	myWorkflow := func(ctx workflow.Context) error { return nil }
	run, err := cluster1Client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: s.tv.TaskQueue().Name,
		ID:        s.tv.WorkflowID(),
	}, myWorkflow)
	s.NoError(err)
	runId := run.GetRunID()

	// Process history replication tasks until a task from cluster1 => cluster2 containing the initial workflow events
	// is encountered.
	s.executeHistoryReplicationTasksFromClusterUntil("cluster1", enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)

	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
  	2 WorkflowTaskScheduled
  	`, s.getHistory(ctx, s.cluster1, runId), []int{1, 1})
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	`, s.getHistory(ctx, s.cluster2, runId), []int{1, 1})

	return runId
}

func (s *historyReplicationConflictTestSuite) registerMultiRegionNamespace(ctx context.Context) {
	_, err := s.cluster1.GetFrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        s.tv.NamespaceName().String(),
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		IsGlobalNamespace:                true,                           // Needed so that the namespace is replicated
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24), // Required parameter
	})
	s.NoError(err)
	// Namespace event replication tasks are being captured; we need to execute the pending ones now to propagate the
	// new namespace to cluster 2.
	s.executeNamespaceReplicationTasksUntil(ctx, enumsspb.NAMESPACE_OPERATION_CREATE, 1)
	s.Equal([]string{"cluster1", "cluster1"}, s.getActiveClusters(ctx))
}

func (s *historyReplicationConflictTestSuite) setActive(ctx context.Context, cluster *tests.TestCluster, clusterName string) {
	_, err := cluster.GetFrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: s.tv.NamespaceName().String(),
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName,
		},
	})
	s.NoError(err)
}

func (s *historyReplicationConflictTestSuite) getHistory(ctx context.Context, cluster *tests.TestCluster, rid string) []*historypb.HistoryEvent {
	historyResponse, err := cluster.GetFrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.tv.NamespaceName().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: s.tv.WorkflowID(),
			RunId:      rid,
		},
	})
	s.NoError(err)
	return historyResponse.History.Events
}

func (s *historyReplicationConflictTestSuite) getActiveClusters(ctx context.Context) []string {
	return []string{
		s.getActiveCluster(ctx, s.cluster1),
		s.getActiveCluster(ctx, s.cluster2),
	}
}

func (s *historyReplicationConflictTestSuite) getActiveCluster(ctx context.Context, cluster *tests.TestCluster) string {
	resp, err := cluster.GetFrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: s.tv.NamespaceName().String()})
	s.NoError(err)
	return resp.ReplicationConfig.ActiveClusterName
}

func (s *historyReplicationConflictTestSuite) decodePayloadsString(ps *commonpb.Payloads) (r string) {
	s.NoError(payloads.Decode(ps, &r))
	return
}
