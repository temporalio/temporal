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

package xdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	enumspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This file contains tests for the history replication DLQ feature. It uses a faulty replication task executor to force
// replication tasks to fail, causing them to be DLQ'd. It then uses the tdbg CLI to read the replication tasks from the
// DLQ and verify that they are correct.

type (
	historyReplicationDLQSuite struct {
		xdcBaseSuite
		// This test is parameterized on whether to use streaming for replication or not. Previously, replication was
		// based on a pull-based architecture with standby clusters polling the active cluster for a given namespace
		// whenever they wanted to process tasks. There's now a push-based, or "streaming", option based on a dynamic
		// config flag. We want to test both code paths, so we run the test suite twice, once with streaming enabled and
		// once with it disabled.
		enableReplicationStream bool
		// We also parameterize on whether we should use the new queue implementation or not. See more details about
		// this migration in [persistence.QueueV2].
		enableQueueV2 bool

		// The below "params" objects are used to propagate parameters to the dependencies that we inject into the
		// Temporal server. Mainly, we use this to share channels between the main test goroutine and the background
		// task processing goroutines so that we can synchronize on them.
		replicationTaskExecutors          replicationTaskExecutorParams
		namespaceReplicationTaskExecutors namespaceReplicationTaskExecutorParams
		dlqWriters                        dlqWriterParams
	}

	// The below types are used to inject our own implementations of replication dependencies, so that we can both
	// observe events like namespace replication tasks being processed and inject our own faulty code to cause tasks
	// to fail.

	replicationTaskExecutorParams struct {
		executedTasks       chan *replicationspb.ReplicationTask
		workflowIDToFail    atomic.Pointer[string]
		workflowIDToObserve atomic.Pointer[string]
	}
	testReplicationTaskExecutor struct {
		*replicationTaskExecutorParams
		taskExecutor replication.TaskExecutor
	}
	dlqWriterParams struct {
		// This channel is sent to once we're done processing a request to add a message to the DLQ.
		processedDLQRequests chan replication.DLQWriteRequest
	}
	testDLQWriter struct {
		*dlqWriterParams
		replication.DLQWriter
	}
	namespaceReplicationTaskExecutorParams struct {
		// This channel is sent to once we're done processing a namespace replication task.
		tasks chan *replicationspb.NamespaceTaskAttributes
	}
	testNamespaceReplicationTaskExecutor struct {
		*namespaceReplicationTaskExecutorParams
		replicationTaskExecutor namespace.ReplicationTaskExecutor
	}
	testExecutableTaskConverter struct {
		*replicationTaskExecutorParams
		converter replication.ExecutableTaskConverter
	}
	testExecutableTask struct {
		*replicationTaskExecutorParams
		replication.TrackableExecutableTask
		replicationTask *replicationspb.ReplicationTask
	}
)

func TestHistoryReplicationDLQSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableQueueV2           bool
		enableReplicationStream bool
	}{
		{
			name:                    "QueueV1ReplicationStreamEnabled",
			enableQueueV2:           false,
			enableReplicationStream: true,
		},
		{
			name:                    "QueueV1ReplicationStreamDisabled",
			enableQueueV2:           false,
			enableReplicationStream: false,
		},
		{
			name:                    "QueueV2ReplicationStreamEnabled",
			enableQueueV2:           true,
			enableReplicationStream: true,
		},
		{
			name:                    "QueueV2ReplicationStreamDisabled",
			enableQueueV2:           true,
			enableReplicationStream: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &historyReplicationDLQSuite{
				enableReplicationStream: tc.enableReplicationStream,
				enableQueueV2:           tc.enableQueueV2,
			}
			suite.Run(t, s)
		})
	}
}

func (s *historyReplicationDLQSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():       s.enableReplicationStream,
		dynamicconfig.EnableHistoryReplicationDLQV2.Key(): s.enableQueueV2,
	}

	// We don't know how many messages these channels are actually going to produce, and we may not read them all, so we
	// need to buffer them by a good amount.
	s.namespaceReplicationTaskExecutors.tasks = make(chan *replicationspb.NamespaceTaskAttributes, 100)
	s.replicationTaskExecutors.executedTasks = make(chan *replicationspb.ReplicationTask, 100)
	s.dlqWriters.processedDLQRequests = make(chan replication.DLQWriteRequest, 100)
	workflowIDToFail := uuid.New()
	s.replicationTaskExecutors.workflowIDToFail.Store(&workflowIDToFail)
	s.replicationTaskExecutors.workflowIDToObserve.Store(&workflowIDToFail)

	// This can't be very long, so we just use a UUID instead of a more descriptive name.
	// We also don't escape this string in many places, so it can't contain any dashes.
	format := strings.Replace(uuid.New(), "-", "", -1) + "_%s"
	taskExecutorDecorator := s.getTaskExecutorDecorator()
	s.logger = log.NewTestLogger()
	s.setupSuite(
		[]string{
			fmt.Sprintf(format, "active"),
			fmt.Sprintf(format, "standby"),
		},
		testcore.WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(
				taskExecutorDecorator,
				func(dlqWriter replication.DLQWriter) replication.DLQWriter {
					// Replace the dlq writer with one that records DLQ requests so that we can wait until a task is
					// added to the DLQ before querying tdbg.
					return &testDLQWriter{
						dlqWriterParams: &s.dlqWriters,
						DLQWriter:       dlqWriter,
					}
				},
			),
		),
		testcore.WithFxOptionsForService(primitives.WorkerService,
			fx.Decorate(
				func(executor namespace.ReplicationTaskExecutor) namespace.ReplicationTaskExecutor {
					return &testNamespaceReplicationTaskExecutor{
						replicationTaskExecutor:                executor,
						namespaceReplicationTaskExecutorParams: &s.namespaceReplicationTaskExecutors,
					}
				},
			),
		),
	)
}

func (s *historyReplicationDLQSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *historyReplicationDLQSuite) SetupTest() {
	s.setupTest()
}

// This test executes a workflow on the active cluster, verifies that its replication tasks all appear in the DLQ, re-
// enqueues them, and then verifies that the replication task executor re-executes them on the standby cluster,
// completing the previously-failed replication attempt.
func (s *historyReplicationDLQSuite) TestWorkflowReplicationTaskFailure() {
	// This test uses channels to synchronize between the main test goroutine and the background task processing for
	// replication, so we use a context with a timeout to ensure that the test doesn't hang forever when we try to
	// receive from a channel.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Register a namespace.
	ns := "history-replication-dlq-test-namespace"
	_, err := s.cluster1.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: ns,
		Clusters:  s.clusterReplicationConfig(),
		// The first cluster is the active cluster.
		ActiveClusterName: s.clusterNames[0],
		// Needed so that the namespace is replicated.
		IsGlobalNamespace: true,
		// This is a required parameter.
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	// Create a worker and register a workflow on the active cluster.
	activeClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	tq := "history-replication-dlq-test-task-queue"
	worker := sdkworker.New(activeClient, tq, sdkworker.Options{})
	myWorkflow := func(ctx workflow.Context) (string, error) {
		return "hello", nil
	}
	worker.RegisterWorkflow(myWorkflow)
	s.NoError(worker.Start())
	defer worker.Stop()

	// Wait for the namespace to be replicated.
	s.waitForNSReplication(ctx, ns)

	// Execute the workflow.
	workflowID := *s.replicationTaskExecutors.workflowIDToFail.Load()
	run, err := activeClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		ID:        workflowID,
	}, myWorkflow)
	s.NoError(err)

	// Wait for the workflow to complete.
	var result string
	err = run.Get(ctx, &result)
	s.NoError(err)
	s.Equal("hello", result)

	// Wait for the replication task executor to process all the replication tasks for this workflow.
	// That way, we will know when the DLQ contains everything it needs for this workflow.
	serializer := serialization.NewSerializer()
	events := s.waitUntilWorkflowReplicated(ctx, serializer, workflowID)

	// Wait until all the replication tasks for this workflow are in the DLQ.
	// We need to do this because we don't want to start re-enqueuing the DLQ until it contains all the replication
	// tasks for this workflow.
	s.waitUntilReplicationTasksAreInDLQ(ctx, events)

	// Before we re-enqueue the replication tasks, we want to call the `tdbg dlq read` command. This acts as both a
	// sanity check because merge will certainly fail if the DLQ is empty, and also a way to verify that the tdbg
	// command itself works.
	// Create a TDBG client pointing at the standby cluster.
	clientFactory := tdbg.NewClientFactory(
		tdbg.WithFrontendAddress(s.cluster2.Host().FrontendGRPCAddress()),
	)
	// Send the output to a bytes buffer instead of a file because it's faster and simpler.
	var cliOutputBuffer bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = clientFactory
		params.Writer = &cliOutputBuffer
	})

	// Run the TDBG command to read replication tasks from the DLQ.
	// The last message ID is set to MaxInt64 - 1 because the last message ID is exclusive, so if it were
	// set to the max int64, then, because the server increments the value, it would overflow and return no results.
	// We want the maximum possible value because we have no idea what the task ids could be, but this parameter must
	// be specified (only for queue v1), so we just use the maximum possible value to get all available tasks.
	lastMessageID := strconv.Itoa(math.MaxInt64 - 1)
	dlqVersion := "v1"
	dlqType := "history"
	if s.enableQueueV2 {
		dlqVersion = "v2"
		dlqType = strconv.Itoa(tasks.CategoryReplication.ID())
	}
	s.testReadTasks(ctx, app, &cliOutputBuffer, dlqVersion, dlqType, run, lastMessageID)

	// Stop failing the replication tasks for this workflow.
	somethingElse := "something-else"
	s.replicationTaskExecutors.workflowIDToFail.Store(&somethingElse)

	// Re-enqueue the replication tasks.
	cmd := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"--" + tdbg.FlagDLQVersion, dlqVersion,
		"merge",
		"--" + tdbg.FlagCluster, s.clusterNames[0],
		"--" + tdbg.FlagShardID, "1",
		"--" + tdbg.FlagLastMessageID, lastMessageID,
		"--" + tdbg.FlagDLQType, dlqType,
	}
	cliOutputBuffer.Truncate(0)
	s.runTDBGCommand(ctx, app, &cliOutputBuffer, cmd)

	if s.enableQueueV2 {
		// DLQ v2 merges tasks asynchronously, so we need to wait for it to finish. In v1, the merge is synchronous.
		// Specifically, v1 executes the replication tasks in-place, instead of putting them back on a task queue, so
		// we both don't need to wait for the merge to finish, and there would be no events on the task executor channel
		// to wait for anyway.
		s.waitUntilWorkflowReplicated(context.Background(), serializer, workflowID)
	}

	// Wait for the workflow to complete on the standby cluster.
	standbyClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	run = standbyClient.GetWorkflow(ctx, workflowID, "")
	err = run.Get(ctx, &result)
	s.NoError(err)
	s.Equal("hello", result)
}

// This method blocks until the DLQ has received replication tasks which cover all the provided history events.
func (s *historyReplicationDLQSuite) waitUntilReplicationTasksAreInDLQ(
	ctx context.Context,
	events []*historypb.HistoryEvent,
) {
	// Make a map containing all the event IDs that we haven't seen in the DLQ'd replication tasks yet.
	eventIDs := make(map[int64]struct{})
	for _, event := range events {
		eventIDs[event.GetEventId()] = struct{}{}
	}
	for len(eventIDs) > 0 {
		select {
		case request := <-s.dlqWriters.processedDLQRequests:
			firstEventID := request.ReplicationTaskInfo.FirstEventId
			// nextEventID is exclusive.
			nextEventID := request.ReplicationTaskInfo.NextEventId
			if request.ReplicationTaskInfo.TaskType == enumspb.TASK_TYPE_REPLICATION_HISTORY {
				// A single replication task could contain multiple events, so we need to mark all the event IDs that it
				// spans as complete.
				for eventID := firstEventID; eventID < nextEventID; eventID++ {
					delete(eventIDs, eventID)
				}
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for replication task to be added to DLQ")
		}
	}
}

// waitForNSReplication blocks until the namespace has been replicated. We want to do this because replication tasks
// will get dropped if the namespace does not exist.
func (s *historyReplicationDLQSuite) waitForNSReplication(ctx context.Context, ns string) {
	for {
		select {
		case task := <-s.namespaceReplicationTaskExecutors.tasks:
			if task.Info.Name == ns {
				return
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for namespace replication task to be processed")
		}
	}
}

// waitUntilWorkflowReplicated waits until the workflow with the given ID has been replicated to the standby cluster.
// It does this by waiting for the replication task executor to process the workflow completion replication event.
func (s *historyReplicationDLQSuite) waitUntilWorkflowReplicated(
	ctx context.Context,
	serializer serialization.Serializer,
	workflowID string,
) []*historypb.HistoryEvent {
	var historyEvents []*historypb.HistoryEvent
	for {
		select {
		case task := <-s.replicationTaskExecutors.executedTasks:
			attr := task.GetHistoryTaskAttributes()
			if attr == nil {
				continue
			}
			if attr.WorkflowId != workflowID {
				continue
			}
			events, err := serializer.DeserializeEvents(attr.Events)
			s.NoError(err)
			historyEvents = append(historyEvents, events...)
			for _, event := range events {
				if event.GetEventType() == enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
					return historyEvents
				}
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for replication task to be processed")
		}
	}
}

// This method calls `tdbg dlq read`, verifying that it contains the correct replication tasks.
func (s *historyReplicationDLQSuite) testReadTasks(
	ctx context.Context,
	app *cli.App,
	buffer *bytes.Buffer,
	dlqVersion string,
	dlqType string,
	run sdkclient.WorkflowRun,
	lastMessageID string,
) {
	cmd := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"--" + tdbg.FlagDLQVersion, dlqVersion,
		"read",
		"--" + tdbg.FlagCluster, s.clusterNames[0],
		"--" + tdbg.FlagShardID, "1",
		"--" + tdbg.FlagLastMessageID, lastMessageID,
		"--" + tdbg.FlagDLQType, dlqType,
	}
	s.runTDBGCommand(ctx, app, buffer, cmd)

	// Parse the output into replication task protos.
	// Verify that the replication task contains the correct information (operators will want to know which workflow
	// failed to replicate).
	if s.enableQueueV2 {
		replicationTasks, err := tdbgtest.ParseDLQMessages(
			buffer,
			func() *persistencespb.ReplicationTaskInfo {
				return new(persistencespb.ReplicationTaskInfo)
			},
		)
		s.NoError(err)
		s.NotEmpty(replicationTasks)
		task := replicationTasks[0].Payload
		s.Equal(enumspb.TASK_TYPE_REPLICATION_HISTORY, task.GetTaskType())
		s.Equal(run.GetID(), task.WorkflowId)
		s.Equal(run.GetRunID(), task.RunId)
	} else {
		var opts temporalproto.CustomJSONUnmarshalOptions
		replicationTasks, err := tdbgtest.ParseJSONL(
			buffer,
			func(decoder *json.Decoder) (*replicationspb.ReplicationTask, error) {
				task := &replicationspb.ReplicationTask{}
				var obj json.RawMessage
				if err := decoder.Decode(&obj); err != nil {
					return nil, err
				}
				return task, opts.Unmarshal(obj, task)
			},
		)
		s.NoError(err)
		s.NotEmpty(replicationTasks)
		task := replicationTasks[0]
		s.Equal(enumspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK, task.GetTaskType())
		historyTaskAttributes := task.GetHistoryTaskAttributes()
		s.Equal(run.GetID(), historyTaskAttributes.GetWorkflowId())
		s.Equal(run.GetRunID(), historyTaskAttributes.GetRunId())
	}
}

// runTDBGCommand is useful for manually testing the TDBG CLI because, in addition to automated checks that we can run
// on the parsed output, we also just want to know that it looks good to a human.
func (s *historyReplicationDLQSuite) runTDBGCommand(
	ctx context.Context,
	app *cli.App,
	buffer *bytes.Buffer,
	cmd []string,
) {
	cmdString := strings.Join(cmd, " ")
	s.T().Log("TDBG command:", cmdString)
	err := app.RunContext(ctx, cmd)
	s.NoError(err)
	s.T().Log("TDBG output:")
	s.T().Log("========================================")
	b := buffer.Bytes()
	s.T().Log(string(b))
	s.T().Log("========================================")
}

func (s *historyReplicationDLQSuite) getTaskExecutorDecorator() interface{} {
	if s.enableReplicationStream {
		// The replication stream uses a different code path which converts tasks into executables using this interface,
		// so that's a good injection point for us.
		return func(converter replication.ExecutableTaskConverter) replication.ExecutableTaskConverter {
			return &testExecutableTaskConverter{
				replicationTaskExecutorParams: &s.replicationTaskExecutors,
				converter:                     converter,
			}
		}
	}
	// Without the replication stream, we use polling that relies on a task executor, so we can inject our own
	// faulty version here.
	return func(provider replication.TaskExecutorProvider) replication.TaskExecutorProvider {
		return func(params replication.TaskExecutorParams) replication.TaskExecutor {
			taskExecutor := provider(params)
			return &testReplicationTaskExecutor{
				replicationTaskExecutorParams: &s.replicationTaskExecutors,
				taskExecutor:                  taskExecutor,
			}
		}
	}
}

// Execute the replication task as-normal, but also send it to the channel so that the test can wait for it to
// know that the namespace data has been replicated.
func (t *testNamespaceReplicationTaskExecutor) Execute(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	err := t.replicationTaskExecutor.Execute(ctx, task)
	if err != nil {
		return err
	}
	t.tasks <- task
	return nil
}

// WriteTaskToDLQ is the same as the normal dlq writer, but also sends the request to the channel so that the test can
// wait for it to know that the replication task has been added to the DLQ.
func (t *testDLQWriter) WriteTaskToDLQ(
	ctx context.Context,
	request replication.DLQWriteRequest,
) error {
	err := t.DLQWriter.WriteTaskToDLQ(ctx, request)
	t.processedDLQRequests <- request
	return err
}

// Execute the replication task as-normal or return an error if the workflow ID matches the one that we want to fail.
// This is run only when streaming is disabled for replication.
func (f testReplicationTaskExecutor) Execute(
	ctx context.Context,
	replicationTask *replicationspb.ReplicationTask,
	forceApply bool,
) error {
	err := f.execute(ctx, replicationTask, forceApply)
	if attr := replicationTask.GetHistoryTaskAttributes(); attr != nil && attr.WorkflowId == *f.workflowIDToObserve.Load() {
		f.executedTasks <- replicationTask
	}
	return err
}

func (f testReplicationTaskExecutor) execute(
	ctx context.Context,
	replicationTask *replicationspb.ReplicationTask,
	forceApply bool,
) error {
	if attr := replicationTask.GetHistoryTaskAttributes(); attr != nil && attr.WorkflowId == *f.workflowIDToFail.Load() {
		return serviceerror.NewInvalidArgument("failed to apply replication task")
	}
	err := f.taskExecutor.Execute(ctx, replicationTask, forceApply)
	return err
}

// Convert the replication tasks using the testcore converter, but then wrap them in our own faulty executable tasks.
func (t *testExecutableTaskConverter) Convert(
	taskClusterName string,
	clientShardKey replication.ClusterShardKey,
	serverShardKey replication.ClusterShardKey,
	replicationTasks ...*replicationspb.ReplicationTask,
) []replication.TrackableExecutableTask {
	convertedTasks := t.converter.Convert(taskClusterName, clientShardKey, serverShardKey, replicationTasks...)
	testExecutableTasks := make([]replication.TrackableExecutableTask, len(convertedTasks))
	for i, task := range convertedTasks {
		testExecutableTasks[i] = &testExecutableTask{
			replicationTaskExecutorParams: t.replicationTaskExecutorParams,
			TrackableExecutableTask:       task,
			replicationTask:               replicationTasks[i],
		}
	}
	return testExecutableTasks
}

// Execute the replication task as-normal or return an error if the workflow ID matches the one that we want to fail.
// This is run only when streaming is enabled for replication.
func (t *testExecutableTask) Execute() error {
	err := t.execute()
	t.replicationTaskExecutorParams.executedTasks <- t.replicationTask
	return err
}

func (t *testExecutableTask) execute() error {
	if et, ok := t.TrackableExecutableTask.(*replication.ExecutableHistoryTask); ok {
		if et.WorkflowID == *t.workflowIDToFail.Load() {
			return serviceerror.NewInvalidArgument("failed to apply replication task")
		}
	}
	return t.TrackableExecutableTask.Execute()
}
