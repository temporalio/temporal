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

package tests

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/fx"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tools/tdbg"
)

type (
	dlqSuite struct {
		FunctionalTestBase
		*require.Assertions
		dlq              persistence.HistoryTaskQueueManager
		workflowID       string
		dlqTasks         chan tasks.Task
		writer           bytes.Buffer
		sdkClientFactory sdk.ClientFactory
		tdgbApp          *cli.App
	}
	dlqTestCase struct {
		name string
		dlqTestParams
		configure func(*dlqTestParams)
	}
	dlqTestParams struct {
		maxMessageCount     string
		lastMessageID       string
		targetCluster       string
		expectedNumMessages int
	}
	testExecutorWrapper struct {
		suite *dlqSuite
	}
	testExecutor struct {
		base  queues.Executor
		suite *dlqSuite
	}
	testDLQWriter struct {
		suite *dlqSuite
		queues.QueueWriter
	}
)

func (s *dlqSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.HistoryTaskDLQEnabled: true,
	}
	s.workflowID = "dlq-test-workflow-id"
	s.dlqTasks = make(chan tasks.Task, 1)
	s.setupSuite(
		"testdata/cluster.yaml",
		WithFxOptionsForService(primitives.HistoryService,
			fx.Populate(&s.dlq),
			fx.Provide(
				func() queues.ExecutorWrapper {
					return &testExecutorWrapper{
						suite: s,
					}
				},
			),
			fx.Decorate(
				func(writer queues.QueueWriter) queues.QueueWriter {
					return &testDLQWriter{
						QueueWriter: writer,
						suite:       s,
					}
				},
			),
		),
		WithFxOptionsForService(primitives.FrontendService,
			fx.Populate(&s.sdkClientFactory),
		),
	)
	s.tdgbApp = tdbg.NewCliApp(
		func(params *tdbg.Params) {
			params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(s.hostPort))
			params.Writer = &s.writer
		},
	)
	s.tdgbApp.ExitErrHandler = func(c *cli.Context, err error) {}
}

func (s *dlqSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *dlqSuite) SetupTest() {
	if TestFlags.PersistenceType == "sql" {
		s.T().Skip("skipping DLQ tests for SQL persistence")
	}
	s.Assertions = require.New(s.T())
}

func TestDLQSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(dlqSuite))
}

func (s *dlqSuite) TestReadArtificialDLQTasks() {
	ctx := context.Background()

	namespaceID := "test-namespace"
	workflowID := "test-workflow-id"
	workflowKey := definition.NewWorkflowKey(namespaceID, workflowID, "test-run-id")

	category := tasks.CategoryTransfer
	sourceCluster := "test-source-cluster-" + s.T().Name()
	// Note: it's ok that this isn't unique across tests because the queue name will still be unique due to the source
	// cluster name being included in the queue name. We use the current cluster name because that's what the default
	// is if the target cluster flag isn't specified.
	targetCluster := "active"
	queueKey := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      category,
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
	}
	_, err := s.dlq.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	s.NoError(err)
	for i := 0; i < 4; i++ {
		task := &tasks.WorkflowTask{
			WorkflowKey: workflowKey,
			TaskID:      int64(42 + i),
		}
		_, err := s.dlq.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
			QueueType:     queueKey.QueueType,
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
			Task:          task,
		})
		s.NoError(err)
	}
	file, err := os.CreateTemp("", "*")
	s.NoError(err)
	s.T().Cleanup(func() {
		s.NoError(os.Remove(file.Name()))
	})
	for _, tc := range []dlqTestCase{
		{
			name: "max message count exceeded",
			configure: func(params *dlqTestParams) {
				params.maxMessageCount = "2"
				params.lastMessageID = "999"
				params.expectedNumMessages = 2
			},
		},
		{
			name: "last message ID exceeded",
			configure: func(params *dlqTestParams) {
				params.maxMessageCount = "999"
				params.lastMessageID = "2" // first message is 0, so this should return 3 messages: 0, 1, 2
				params.expectedNumMessages = 3
			},
		},
		{
			name: "target cluster specified",
			configure: func(params *dlqTestParams) {
			},
		},
		{
			name: "target cluster not specified",
			configure: func(params *dlqTestParams) {
				params.targetCluster = ""
			},
		},
	} {
		tc := tc
		s.Run(tc.name, func() {
			tc.maxMessageCount = "999"
			tc.lastMessageID = "999"
			tc.expectedNumMessages = 4
			tc.targetCluster = targetCluster
			tc.configure(&tc.dlqTestParams)
			args := []string{
				"tdbg",
				"dlq",
				"--" + tdbg.FlagDLQVersion, "v2",
				"read",
				"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
				"--" + tdbg.FlagCluster, sourceCluster,
				"--" + tdbg.FlagPageSize, "1",
				"--" + tdbg.FlagMaxMessageCount, tc.maxMessageCount,
				"--" + tdbg.FlagLastMessageID, tc.lastMessageID,
				"--" + tdbg.FlagOutputFilename, file.Name(),
			}
			if tc.targetCluster != "" {
				args = append(args, "--"+tdbg.FlagTargetCluster, tc.targetCluster)
			}
			cmdString := strings.Join(args, " ")
			s.T().Log("TDBG command:", cmdString)
			err = s.tdgbApp.Run(args)
			s.NoError(err)

			s.T().Log("TDBG output:")
			s.T().Log("========================================")
			output, err := io.ReadAll(file)
			s.NoError(err)
			s.T().Log(string(output))
			_, err = file.Seek(0, io.SeekStart)
			s.NoError(err)
			s.T().Log("========================================")
			s.verifyNumTasks(file, tc.expectedNumMessages)
		})
	}
}

// This test executes an actual workflow for which we've set up an executor wrapper to return a terminal error. This
// causes the workflow task to be added to the DLQ. This tests the end-to-end functionality of the DLQ, whereas the
// above test is more for testing specific CLI flags when reading from the DLQ. After the workflow task is added to the
// DLQ, this test then purges the DLQ and verifies that the task was deleted.
func (s *dlqSuite) TestRealWorkflow() {
	ctx := context.Background()
	run := s.executeWorkflow(ctx)

	select {
	case <-ctx.Done():
		s.FailNow("timed out waiting for workflow to task to be DLQ'd")
	case task := <-s.dlqTasks:
		s.Equal(run.GetRunID(), task.GetRunID())
	}

	dlqTasks := s.readDLQTasks()
	s.NotEmpty(dlqTasks)
	task := dlqTasks[0]
	var taskInfo persistencespb.TransferTaskInfo
	err := taskInfo.Unmarshal(task.Payload.Blob.Data)
	s.NoError(err)
	s.Equal(s.workflowID, taskInfo.WorkflowId)
	s.Equal(run.GetRunID(), taskInfo.RunId)

	maxMessageIDToDelete := task.Metadata.MessageId
	s.purgeMessages(ctx, maxMessageIDToDelete)

	dlqTasks = s.readDLQTasks()
	for _, task := range dlqTasks {
		s.Less(task.Metadata.MessageId, maxMessageIDToDelete, "purge command failed to delete all messages")
	}
}

func (s *dlqSuite) executeWorkflow(ctx context.Context) sdkclient.WorkflowRun {
	myWorkflow := func(ctx workflow.Context) (string, error) {
		return "hello", nil
	}
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	s.NoError(err)
	taskQueue := "dlq-test-task-queue"
	worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
	worker.RegisterWorkflow(myWorkflow)
	s.NoError(worker.Start())
	defer worker.Stop()
	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        s.workflowID,
		TaskQueue: taskQueue,
	}, myWorkflow)
	s.NoError(err)
	return run
}

func (s *dlqSuite) purgeMessages(ctx context.Context, maxMessageIDToDelete int64) {
	args := []string{
		"tdbg",
		"dlq",
		"--" + tdbg.FlagDLQVersion, "v2",
		"purge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageIDToDelete, 10),
	}
	err := s.tdgbApp.Run(args)
	s.NoError(err)
	output := s.writer.Bytes()
	var response adminservice.PurgeDLQTasksResponse
	s.NoError(jsonpb.Unmarshal(bytes.NewReader(output), &response))

	var token adminservice.DLQJobToken
	s.NoError(token.Unmarshal(response.GetJobToken()))

	systemSDKClient := s.sdkClientFactory.GetSystemClient()
	run := systemSDKClient.GetWorkflow(ctx, token.WorkflowId, token.RunId)
	s.NoError(run.Get(ctx, nil))
}

func (s *dlqSuite) readDLQTasks() []*commonspb.HistoryDLQTask {
	file := testutils.CreateTemp(s.T(), "", "*")
	args := []string{
		"tdbg",
		"dlq",
		"--" + tdbg.FlagDLQVersion, "v2",
		"read",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagOutputFilename, file.Name(),
	}
	s.NoError(s.tdgbApp.Run(args))
	dlqTasks := s.parseHistoryDLQTasks(file)
	return dlqTasks
}

func (s *dlqSuite) verifyNumTasks(file *os.File, expectedNumTasks int) {
	dlqTasks := s.parseHistoryDLQTasks(file)
	s.Len(dlqTasks, expectedNumTasks)

	for i, task := range dlqTasks {
		s.Equal(int64(persistence.FirstQueueMessageID+i), task.Metadata.MessageId)

		var taskInfo persistencespb.TransferTaskInfo
		err := taskInfo.Unmarshal(task.Payload.Blob.Data)
		s.NoError(err)
		s.Equal(enums.TASK_TYPE_TRANSFER_WORKFLOW_TASK, taskInfo.TaskType)
		s.Equal("test-namespace", taskInfo.NamespaceId)
		s.Equal("test-workflow-id", taskInfo.WorkflowId)
		s.Equal("test-run-id", taskInfo.RunId)
		s.Equal(int64(42+i), taskInfo.TaskId)
	}
}

func (s *dlqSuite) parseHistoryDLQTasks(file *os.File) []*commonspb.HistoryDLQTask {
	return ParseJSONLProtos[*commonspb.HistoryDLQTask](s.Assertions, file, func() *commonspb.HistoryDLQTask {
		return new(commonspb.HistoryDLQTask)
	})
}

func (t *testDLQWriter) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	res, err := t.QueueWriter.EnqueueTask(ctx, request)
	select {
	case t.suite.dlqTasks <- request.Task:
	default:
	}
	return res, err
}

func (t testExecutor) Execute(ctx context.Context, e queues.Executable) queues.ExecuteResponse {
	if e.GetWorkflowID() == t.suite.workflowID && e.GetCategory() == tasks.CategoryTransfer {
		// Return a terminal error that will cause this task to be added to the DLQ.
		return queues.ExecuteResponse{
			ExecutionErr: serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, errors.New("test error")),
		}
	}
	return t.base.Execute(ctx, e)
}

func (t testExecutorWrapper) Wrap(delegate queues.Executor) queues.Executor {
	return &testExecutor{
		base:  delegate,
		suite: t.suite,
	}
}
