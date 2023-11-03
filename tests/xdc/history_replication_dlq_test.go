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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	enumspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
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

		replicationTaskExecutorParams          replicationTaskExecutorParams
		executionManagerParams                 dlqWriterParams
		namespaceReplicationTaskExecutorParams namespaceReplicationTaskExecutorParams
	}

	// The below types are used to inject our own implementations of replication dependencies, so that we can both
	// observe events like namespace replication tasks being processed and inject our own faulty code to cause tasks
	// to fail.

	replicationTaskExecutorParams struct {
		// We use an interface{} for these tasks because we don't care about the data, and the concrete type changes
		// depending on whether we have streaming enabled for replication or not.
		tasks chan interface{}
	}
	testReplicationTaskExecutor struct {
		*replicationTaskExecutorParams
		taskExecutor replication.TaskExecutor
	}
	dlqWriterParams struct {
		// This channel is sent to once we're done processing a request to add a message to the DLQ.
		dlqRequests chan replication.WriteRequest
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
	}
)

const (
	testTimeout      = 30 * time.Second
	workflowIDToFail = "history-replication-dlq-test-workflow"
)

func TestHistoryReplicationDLQSuite(t *testing.T) {
	flag.Parse()
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
		tc := tc
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
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.EnableReplicationStream:       s.enableReplicationStream,
		dynamicconfig.EnableHistoryReplicationDLQV2: s.enableQueueV2,
	}

	// Buffer this channel by one because we only care about the first request to add a message to the DLQ.
	s.executionManagerParams.dlqRequests = make(chan replication.WriteRequest, 1)

	// Buffer these channels by 100 because we don't know how many replication tasks there will be until the one that
	// replicates our namespace is executed.
	s.namespaceReplicationTaskExecutorParams.tasks = make(chan *replicationspb.NamespaceTaskAttributes, 100)
	s.replicationTaskExecutorParams.tasks = make(chan interface{}, 100)

	// This can't be very long, so we just use a UUID instead of a more descriptive name.
	// We also don't escape this string in many places, so it can't contain any dashes.
	format := strings.Replace(uuid.New(), "-", "", -1) + "_%s"
	taskExecutorDecorator := s.getTaskExecutorDecorator()
	s.logger = log.NewNoopLogger()
	s.setupSuite(
		[]string{
			fmt.Sprintf(format, "active"),
			fmt.Sprintf(format, "standby"),
		},
		tests.WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(
				taskExecutorDecorator,
				func(dlqWriter replication.DLQWriter) replication.DLQWriter {
					// Replace the dlq writer with one that records DLQ requests so that we can wait until a task is
					// added to the DLQ before querying tdbg.
					return &testDLQWriter{
						dlqWriterParams: &s.executionManagerParams,
						DLQWriter:       dlqWriter,
					}
				},
			),
		),
		tests.WithFxOptionsForService(primitives.WorkerService,
			fx.Decorate(
				func(namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor) namespace.ReplicationTaskExecutor {
					return &testNamespaceReplicationTaskExecutor{
						replicationTaskExecutor:                namespaceReplicationTaskExecutor,
						namespaceReplicationTaskExecutorParams: &s.namespaceReplicationTaskExecutorParams,
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

// This test executes a workflow on the active cluster and verifies that its replication tasks show in the DLQ of the
// standby cluster when they fail to execute.
func (s *historyReplicationDLQSuite) TestWorkflowReplicationTaskFailure() {
	// This test uses channels to synchronize between the main test goroutine and the background task processing for
	// replication, so we use a context with a timeout to ensure that the test doesn't hang forever when we try to
	// receive from a channel.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	// Register a namespace.
	ns := "history-replication-dlq-test-namespace"
	_, err := s.cluster1.GetFrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		IsGlobalNamespace:                true,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(1 * time.Hour * 24), // required param
	})
	s.NoError(err)

	// Create a worker and register a workflow.
	activeClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
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
	for {
		var task *replicationspb.NamespaceTaskAttributes
		select {
		case task = <-s.namespaceReplicationTaskExecutorParams.tasks:
		case <-ctx.Done():
			s.FailNow("timed out waiting for namespace replication task to be processed")
		}
		if task.Info.Name == ns {
			break
		}
	}

	// Execute the workflow.
	run, err := activeClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		ID:        workflowIDToFail,
	}, myWorkflow)
	s.NoError(err)

	// Wait for the workflow to complete.
	var result string
	err = run.Get(ctx, &result)
	s.NoError(err)
	s.Equal("hello", result)

	// Wait for the replication task executor to process the replication task.
	select {
	case <-s.replicationTaskExecutorParams.tasks:
	case <-ctx.Done():
		s.FailNow("timed out waiting for replication task to be processed")
	}

	// Wait for at least one replication task to be added to the DLQ. There could be more, but we only care about the
	// first one because they should all be for this workflow since it's the only one for which we injected replication
	// task failures.
	var request replication.WriteRequest
	select {
	case request = <-s.executionManagerParams.dlqRequests:
	case <-ctx.Done():
		s.FailNow("timed out waiting for replication task to be added to DLQ")
	}
	s.Equal(s.clusterNames[0], request.SourceCluster)
	s.Equal(1, int(request.ShardID))

	// Create a TDBG client pointing at the standby cluster.
	clientFactory := tdbg.NewClientFactory(
		tdbg.WithFrontendAddress(s.cluster2.GetHost().FrontendGRPCAddress()),
	)
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = clientFactory
	})

	// Run the TDBG command to read replication tasks from the DLQ.
	// The last message ID is set to MaxInt64 - 1 because the last message ID is exclusive, so if it were
	// set to the max int64, then, because the server increments the value, it would overflow and return no results.
	// We want the maximum possible value because we have no idea what the task ids could be, but this parameter must
	// be specified, so we just use the maximum possible value to get all available tasks.
	lastMessageID := strconv.Itoa(math.MaxInt64 - 1)
	file := testutils.CreateTemp(s.T(), "", "*")
	dlqVersion := "v1"
	if s.enableQueueV2 {
		dlqVersion = "v2"
	}
	cmd := []string{
		"tdbg",
		"dlq",
		"--" + tdbg.FlagDLQVersion, dlqVersion,
		"read",
		"--" + tdbg.FlagCluster, s.clusterNames[0],
		"--" + tdbg.FlagShardID, "1",
		"--" + tdbg.FlagLastMessageID, lastMessageID,
		"--" + tdbg.FlagMaxMessageCount, "10",
		"--" + tdbg.FlagOutputFilename, file.Name(),
	}
	if s.enableQueueV2 {
		cmd = append(cmd,
			"--"+tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryReplication.ID()),
		)
	} else {
		cmd = append(cmd,
			"--"+tdbg.FlagDLQType, "history",
		)
	}
	cmdString := strings.Join(cmd, " ")
	s.T().Log("TDBG command:", cmdString)
	err = app.Run(cmd)
	s.NoError(err)
	s.T().Log("TDBG output:")
	s.T().Log("========================================")
	bytes, err := io.ReadAll(file)
	s.NoError(err)
	s.T().Log(string(bytes))

	// Parse the output into replication task protos.
	_, err = file.Seek(0, io.SeekStart)
	s.NoError(err)
	s.T().Log("========================================")
	// Verify that the replication task contains the correct information (operators will want to know which workflow
	// failed to replicate).
	if s.enableQueueV2 {
		replicationTasks, err := tdbgtest.ParseDLQMessages(
			file,
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
		replicationTasks, err := tdbgtest.ParseJSONL(
			file,
			func(decoder *json.Decoder) (*replicationspb.ReplicationTask, error) {
				task := &replicationspb.ReplicationTask{}
				return task, jsonpb.UnmarshalNext(decoder, task)
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

func (s *historyReplicationDLQSuite) getTaskExecutorDecorator() interface{} {
	if s.enableReplicationStream {
		// The replication stream uses a different code path which converts tasks into executables using this interface,
		// so that's a good injection point for us.
		return func(converter replication.ExecutableTaskConverter) replication.ExecutableTaskConverter {
			return &testExecutableTaskConverter{
				replicationTaskExecutorParams: &s.replicationTaskExecutorParams,
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
				replicationTaskExecutorParams: &s.replicationTaskExecutorParams,
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
	select {
	case t.tasks <- task:
	default:
	}
	return nil
}

// WriteTaskToDLQ is the same as the normal dlq writer, but also sends the request to the channel so that the test can
// wait for it to know that the replication task has been added to the DLQ.
func (t *testDLQWriter) WriteTaskToDLQ(
	ctx context.Context,
	request replication.WriteRequest,
) error {
	err := t.DLQWriter.WriteTaskToDLQ(ctx, request)
	if err != nil {
		return err
	}
	select {
	case t.dlqRequests <- request:
	default:
	}
	return nil
}

// Execute the replication task as-normal or return an error if the workflow ID matches the one that we want to fail.
func (f testReplicationTaskExecutor) Execute(
	ctx context.Context,
	replicationTask *replicationspb.ReplicationTask,
	forceApply bool,
) error {
	if replicationTask.GetHistoryTaskAttributes().WorkflowId == workflowIDToFail {
		select {
		case f.tasks <- replicationTask:
		default:
		}
		return errors.New("failed to apply replication task")
	}
	return f.taskExecutor.Execute(ctx, replicationTask, forceApply)
}

// Convert the replication tasks using the base converter, but then wrap them in our own faulty executable tasks.
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
		}
	}
	return testExecutableTasks
}

// Execute the replication task as-normal or return an error if the workflow ID matches the one that we want to fail.
func (t *testExecutableTask) Execute() error {
	if et, ok := t.TrackableExecutableTask.(*replication.ExecutableHistoryTask); ok {
		if et.WorkflowID == workflowIDToFail {
			t.replicationTaskExecutorParams.tasks <- struct{}{}
			return serviceerror.NewInvalidArgument("failed to apply replication task")
		}
	}
	return t.TrackableExecutableTask.Execute()
}
