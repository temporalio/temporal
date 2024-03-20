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
	"errors"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/tests"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This file contains tests of scenarios in which conflicting histories arise during history replication.
// To do this we need to be able to disable, and then re-enable, namespace and history replication.

type (
	historyReplicationConflictTestSuite struct {
		xdcBaseSuite
		namespaceReplicationTasks chan *replicationspb.NamespaceTaskAttributes
		namespaceTaskExecutor     namespace.ReplicationTaskExecutor
		historyReplicationTasks   chan *hrcTestExecutableTask
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
	}
)

const (
	waitDuration       = 2 * time.Second
	taskBufferCapacity = 100
)

var (
	cluster1Count = 0
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
	s.namespaceReplicationTasks = make(chan *replicationspb.NamespaceTaskAttributes, taskBufferCapacity)
	s.historyReplicationTasks = make(chan *hrcTestExecutableTask, taskBufferCapacity)
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
}

func (s *historyReplicationConflictTestSuite) SetupTest() {
	s.setupTest()
}

func (s *historyReplicationConflictTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *historyReplicationConflictTestSuite) TestConflictResolutionReappliesSignals() {
	ns := "history-replication-conflict-test-namespace"
	id := "history-replication-conflict-test-workflow-id"
	tq := "history-replication-conflict-test-task-queue"

	ctx := context.Background()
	sdkClient1, sdkClient2 := s.createSdkClients(ns)

	fmt.Println("----------- Register global namespace")
	s.registerGlobalNamespace(ctx, ns)

	fmt.Println("----------- Execute namespace replication tasks")
	s.executeNamespaceReplicationTasks(ctx)

	fmt.Println("----------- Start workflow")
	runId := s.startWorkflow(ctx, sdkClient1, tq, id)
	time.Sleep(waitDuration)
	// s.executeHistoryReplicationTasksUntilXXX(ctx, id, enums.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)
	s.describeNamespaces(ctx, ns)
	s.printEvents(ctx, ns, id, runId)

	s.HistoryRequire.EqualHistoryEventsAndVersions(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  `, s.getHistory(ctx, s.cluster1, ns, id, runId), []int{1, 1})
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  `, s.getHistory(ctx, s.cluster2, ns, id, runId), []int{1, 1})

	// We now create a "split brain" state by setting cluster2 to active. We do not execute replication tasks afterward,
	// so cluster1 does not learn of the change.
	fmt.Println("----------- Set cluster2 active")
	s.setActive(ctx, s.cluster2, "cluster2", ns)
	s.describeNamespaces(ctx, ns)

	// Both clusters now believe they are active and hence both will accept a signal.
	fmt.Println("----------- Send signals")
	err := sdkClient1.SignalWorkflow(ctx, id, runId, "my-signal", "cluster-1-signal")
	s.NoError(err)
	err = sdkClient2.SignalWorkflow(ctx, id, runId, "my-signal", "cluster-2-signal")
	s.NoError(err)
	s.printEvents(ctx, ns, id, runId)
	time.Sleep(waitDuration)

	// cluster1 has accepted a signal
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster-1-signal\""}]}}
	`, s.getHistory(ctx, s.cluster1, ns, id, runId), []int{1, 1, 1})

	// cluster2: notice that the signal it accepted has failover version 2
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster-2-signal\""}]}}
	`, s.getHistory(ctx, s.cluster2, ns, id, runId), []int{1, 1, 2})

	// Execute pending history replication tasks. Recall that both clusters believe they are active.
	// Each cluster sends its signal to the other.
	fmt.Println("----------- Execute history replication tasks")
	s.executeHistoryReplicationTasksUntil(ctx, id, enums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	fmt.Println("----------- Execute namespace replication tasks")
	s.executeNamespaceReplicationTasks(ctx)
	s.describeNamespaces(ctx, ns)
	s.printEvents(ctx, ns, id, runId)

	// cluster1:
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster-2-signal\""}]}}
	`, s.getHistory(ctx, s.cluster1, ns, id, runId), []int{1, 1, 2})

	// cluster2: history has not changed
	s.HistoryRequire.EqualHistoryEventsAndVersions(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster-2-signal\""}]}}
	4 WorkflowExecutionSignaled {"Input": {"Payloads": [{"Data": "\"cluster-1-signal\""}]}}
	`, s.getHistory(ctx, s.cluster2, ns, id, runId), []int{1, 1, 2, 2})

	time.Sleep(waitDuration)
	s.executeHistoryReplicationTasksUntil(ctx, id, enums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.printEvents(ctx, ns, id, runId)
	time.Sleep(waitDuration)
	s.executeHistoryReplicationTasksUntil(ctx, id, enums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	s.printEvents(ctx, ns, id, runId)

	s.describeNamespaces(ctx, ns)
	fmt.Printf("cluster1 describe workflow")
	s.describeWorkflow(ctx, s.cluster1, ns, id, runId)

	fmt.Printf("cluster2 describe workflow")
	s.describeWorkflow(ctx, s.cluster2, ns, id, runId)
}

func (s *historyReplicationConflictTestSuite) executeNamespaceReplicationTasks(ctx context.Context) {
	for {
		select {
		case task := <-s.namespaceReplicationTasks:
			err := s.namespaceTaskExecutor.Execute(ctx, task)
			s.NoError(err)
			fmt.Println("Executed namespace replication task:", task)
		default:
			return
		}
	}
}

// executeHistoryReplicationTasksUntil executes buffered history replication tasks until the requested event type is
// encountered for the workflowId.
func (s *historyReplicationConflictTestSuite) executeHistoryReplicationTasksUntilXXX(
	ctx context.Context,
	workflowId string,
	eventType enums.EventType,
) {
	serializer := serialization.NewSerializer()
	seen := false
	for {
		select {
		case task := <-s.historyReplicationTasks:
			trackableTask := (*task).TrackableExecutableTask
			err := trackableTask.Execute()
			s.NoError(err)
			trackableTask.Ack()
			attr := (*task).replicationTask.GetHistoryTaskAttributes()
			if attr == nil {
				s.logger.Warn("task has no history task attributes")
				continue
			}
			if attr.WorkflowId != workflowId {
				continue
			}
			events, err := serializer.DeserializeEvents(attr.Events)
			s.NoError(err)
			for _, event := range events {
				fmt.Println("history replication task event:", event.EventType)
				if event.GetEventType() == eventType {
					seen = true
				}
			}
			if seen {
				return
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for replication task to be processed")
		}
	}
}

// executeHistoryReplicationTasksUntil executes buffered history replication tasks until the requested event type is
// encountered for the workflowId.
func (s *historyReplicationConflictTestSuite) executeHistoryReplicationTasksUntil(
	ctx context.Context,
	workflowId string,
	eventType enums.EventType,
) {
	serializer := serialization.NewSerializer()
	cluster1Count := 0
	cluster2Count := 0
	for {
		select {
		case task := <-s.historyReplicationTasks:
			trackableTask := (*task).TrackableExecutableTask
			err := trackableTask.Execute()
			s.NoError(err)
			trackableTask.Ack()
			attrs := (*task).replicationTask.GetHistoryTaskAttributes()
			s.NotNil(attrs)
			fmt.Printf("Lazily executed %s history replication task: %v\n%v\n", task.taskClusterName, trackableTask, attrs)
			if attrs.WorkflowId != workflowId {
				continue
			}
			events, err := serializer.DeserializeEvents(attrs.Events)
			s.NoError(err)
			for _, event := range events {
				fmt.Println("history replication task event:", event.EventType)
				if event.GetEventType() == eventType {
					fmt.Println(task.taskClusterName)
					if task.taskClusterName == "cluster1" {
						cluster1Count++
					}
					if task.taskClusterName == "cluster2" {
						cluster2Count++
					}
					if cluster1Count >= 1 && cluster2Count >= 1 {
						return
					}
				}
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for replication task to be processed")
		}
	}
}

func (c *hrcTestNamespaceReplicationTaskExecutor) Execute(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) error {
	c.s.namespaceReplicationTasks <- task
	return errors.New("Prevent Ack")
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
		}
	}
	return testExecutableTasks
}

// Execute buffers the task instead of executing it.
func (t *hrcTestExecutableTask) Execute() error {
	if t.taskClusterName == "cluster1" && cluster1Count == 0 {
		// The first history replication task is replicating the initial workflow events from cluster 1 to cluster 2.
		// Execute this eagerly.
		cluster1Count++
		err := t.TrackableExecutableTask.Execute()

		attrs := t.replicationTask.GetHistoryTaskAttributes()
		t.s.NotNil(attrs)
		fmt.Printf("Eagerly executed cluster1 history replication task: %v\n%v\n", t.TrackableExecutableTask, attrs)

		return err
	}
	t.s.historyReplicationTasks <- t
	return errors.New("Prevent Ack")
}

// gRPC utilities

func (s *historyReplicationConflictTestSuite) createSdkClients(ns string) (sdkclient.Client, sdkclient.Client) {
	c1, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	c2, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.GetHost().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	return c1, c2
}

func (s *historyReplicationConflictTestSuite) startWorkflow(ctx context.Context, client sdkclient.Client, tq, id string) string {
	myWorkflow := func(ctx workflow.Context) error { return nil }
	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		ID:        id,
	}, myWorkflow)
	s.NoError(err)
	time.Sleep(waitDuration)
	return run.GetRunID()
}

func (s *historyReplicationConflictTestSuite) registerGlobalNamespace(ctx context.Context, ns string) {
	_, err := s.cluster1.GetFrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		IsGlobalNamespace:                true,                           // Needed so that the namespace is replicated
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24), // Required parameter
	})
	s.NoError(err)
	time.Sleep(waitDuration)
}

func (s *historyReplicationConflictTestSuite) setActive(ctx context.Context, cluster *tests.TestCluster, clusterName string, ns string) {
	_, err := cluster.GetFrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			// Clusters:          s.clusterReplicationConfig(),
			ActiveClusterName: clusterName,
		},
	})
	s.NoError(err)
	time.Sleep(waitDuration)
}

func (s *historyReplicationConflictTestSuite) getHistory(ctx context.Context, cluster *tests.TestCluster, ns string, id string, rid string) []*historypb.HistoryEvent {
	historyResponse, err := cluster.GetFrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      rid,
		},
	})
	s.NoError(err)
	return historyResponse.History.Events
}

// NOT FOR MERGE: Debugging utilities

func (s *historyReplicationConflictTestSuite) describeWorkflow(ctx context.Context, cluster *tests.TestCluster, ns string, id string, rid string) error {
	response, err := cluster.GetAdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      rid,
		},
	})
	s.NoError(err)
	for _, h := range response.CacheMutableState.ExecutionInfo.VersionHistories.Histories {
		fmt.Printf("history: \n%v\n", h)
	}
	return nil
}
func (s *historyReplicationConflictTestSuite) describeNamespaces(ctx context.Context, ns string) {
	for i, cluster := range []*tests.TestCluster{s.cluster1, s.cluster2} {
		fmt.Println("cluster:", i+1)
		s.describeClusterNamespaces(ctx, cluster, ns)
	}
}

func (s *historyReplicationConflictTestSuite) describeClusterNamespaces(ctx context.Context, cluster *tests.TestCluster, ns string) {

	r, err := cluster.GetFrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("    namespace", r.NamespaceInfo.Name)
	fmt.Println("    isGlobal:     ", r.IsGlobalNamespace)
	fmt.Println("    activeCluster:", r.ReplicationConfig.ActiveClusterName)
	fmt.Println("    clusters:     ", r.ReplicationConfig.Clusters)
}

func (s *historyReplicationConflictTestSuite) decodePayloadsString(ps *commonpb.Payloads) (r string) {
	s.NoError(payloads.Decode(ps, &r))
	return
}

func (s *historyReplicationConflictTestSuite) printEvents(ctx context.Context, ns string, id string, rid string) {
	for i, cluster := range []*tests.TestCluster{s.cluster1, s.cluster2} {
		fmt.Printf("cluster%d\n", +i+1)
		for _, e := range s.getHistory(ctx, cluster, ns, id, rid) {
			input := ""
			if attr := e.GetWorkflowExecutionSignaledEventAttributes(); attr != nil {
				input = s.decodePayloadsString(attr.Input)
			}
			fmt.Printf("(%d, %d) %s %s\n", e.EventId, e.Version, e.EventType.String(), input)
		}
	}
	fmt.Println()
}
