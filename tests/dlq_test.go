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
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
)

type (
	dlqSuite struct {
		FunctionalTestBase
		*require.Assertions
		dlq                     persistence.HistoryTaskQueueManager
		failingWorkflowIDPrefix string
		dlqTasks                chan tasks.Task
		writer                  bytes.Buffer
		sdkClientFactory        sdk.ClientFactory
		tdbgApp                 *cli.App
		worker                  sdkworker.Worker
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

const (
	testTimeout = 10 * time.Second * debug.TimeoutMultiplier
	taskQueue   = "dlq-test-task-queue"
)

func (s *dlqSuite) SetupSuite() {
	s.setAssertions()
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.HistoryTaskDLQEnabled: true,
	}
	s.dlqTasks = make(chan tasks.Task)
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
	s.tdbgApp = tdbgtest.NewCliApp(
		func(params *tdbg.Params) {
			params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(s.hostPort))
			params.Writer = &s.writer
		},
	)
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	s.NoError(err)
	s.worker = sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
	s.worker.RegisterWorkflow(myWorkflow)
	s.NoError(s.worker.Start())
}

func (s *dlqSuite) TearDownSuite() {
	s.worker.Stop()
	s.tearDownSuite()
}

func myWorkflow(workflow.Context) (string, error) {
	return "hello", nil
}

func (s *dlqSuite) SetupTest() {
	if TestFlags.PersistenceType == "sql" {
		s.T().Skip("skipping DLQ tests for SQL persistence")
	}
	s.setAssertions()
	s.failingWorkflowIDPrefix = "dlq-test-terminal-wfts-"
}

func (s *dlqSuite) setAssertions() {
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
				"--" + tdbg.FlagYes,
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
			err = s.tdbgApp.Run(args)
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
func (s *dlqSuite) TestPurgeRealWorkflow() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	_, dlqMessageID := s.executeDoomedWorkflow(ctx)

	// Delete the workflow task from the DLQ.
	s.purgeMessages(ctx, dlqMessageID)

	// Verify that the workflow task is no longer in the DLQ.
	dlqTasks := s.readDLQTasks()
	s.Empty(dlqTasks, "expected DLQ to be empty after purge")
}

// This test executes actual workflows for which we've set up an executor wrapper to return a terminal error. This
// causes the workflow tasks to be added to the DLQ. This tests the end-to-end functionality of the DLQ, whereas the
// above test is more for testing specific CLI flags when reading from the DLQ.
func (s *dlqSuite) TestMergeRealWorkflow() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	// Verify that we can execute a normal workflow.
	run := s.executeWorkflow(ctx, "dlq-test-ok-workflow-id")
	s.validateWorkflowRun(ctx, run)

	// Execute several doomed workflows.
	numWorkflows := 3
	var runs []sdkclient.WorkflowRun
	for i := 0; i < numWorkflows; i++ {
		run, dlqMessageID := s.executeDoomedWorkflow(ctx)
		s.Equal(int64(i), dlqMessageID)
		runs = append(runs, run)
	}

	// Re-enqueue the workflow tasks from the DLQ, but don't fail its WFTs this time.
	s.failingWorkflowIDPrefix = "some-workflow-id-that-wont-exist"
	s.mergeMessages(ctx, int64(numWorkflows-1))

	// Verify that the workflow task was deleted from the DLQ after merging.
	dlqTasks := s.readDLQTasks()
	s.Empty(dlqTasks)

	// Verify that the workflows now eventually complete successfully.
	for i := 0; i < numWorkflows; i++ {
		s.validateWorkflowRun(ctx, runs[i])
	}
}

func (s *dlqSuite) TestDescribePurgeRealWorkflow() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	_, dlqMessageID := s.executeDoomedWorkflow(ctx)

	// Delete the workflow task from the DLQ.
	s.purgeMessages(ctx, dlqMessageID)

	// Verify that the workflow task is no longer in the DLQ.
	dlqTasks := s.readDLQTasks()
	s.Empty(dlqTasks, "expected DLQ to be empty after purge")
}

func (s *dlqSuite) validateWorkflowRun(ctx context.Context, run sdkclient.WorkflowRun) {
	var result string
	err := run.Get(ctx, &result)
	s.NoError(err)
	s.Equal("hello", result)
}

// executeDoomedWorkflow runs a workflow that is guaranteed to produce a workflow task that will be added to the DLQ. It
// then returns the sdk workflow run and the message ID of the DLQ message for the failed workflow task.
func (s *dlqSuite) executeDoomedWorkflow(ctx context.Context) (sdkclient.WorkflowRun, int64) {
	// Execute a workflow.
	// Use a random workflow ID to ensure that we don't have any collisions with other runs.
	run := s.executeWorkflow(ctx, s.failingWorkflowIDPrefix+uuid.New())

	// Wait for the workflow task to be added to the DLQ.
	select {
	case <-ctx.Done():
		s.FailNow("timed out waiting for workflow to task to be DLQ'd")
	case task := <-s.dlqTasks:
		s.Equal(run.GetRunID(), task.GetRunID())
	}

	// Verify that the workflow task is in the DLQ.
	task := s.verifyRunIsInDLQ(run)
	dlqMessageID := task.MessageID
	return run, dlqMessageID
}

func (s *dlqSuite) verifyRunIsInDLQ(run sdkclient.WorkflowRun) tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
	dlqTasks := s.readDLQTasks()
	for _, task := range dlqTasks {
		if task.Payload.RunId == run.GetRunID() {
			return task
		}
	}
	s.Fail("workflow task not found in DLQ", run.GetRunID())
	panic("unreachable")
}

// executeWorkflow just executes a simple no-op workflow that returns "hello" and returns the sdk workflow run.
func (s *dlqSuite) executeWorkflow(ctx context.Context, workflowID string) sdkclient.WorkflowRun {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	s.NoError(err)

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, myWorkflow)
	s.NoError(err)
	return run
}

// purgeMessages from the DLQ up to and including the specified message ID, blocking until the purge workflow completes.
func (s *dlqSuite) purgeMessages(ctx context.Context, maxMessageIDToDelete int64) {
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"--" + tdbg.FlagDLQVersion, "v2",
		"purge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageIDToDelete, 10),
	}
	err := s.tdbgApp.Run(args)
	s.NoError(err)
	output := s.writer.Bytes()
	s.writer.Truncate(0)
	var response adminservice.PurgeDLQTasksResponse
	s.NoError(jsonpb.Unmarshal(bytes.NewReader(output), &response))

	var token adminservice.DLQJobToken
	s.NoError(token.Unmarshal(response.GetJobToken()))

	systemSDKClient := s.sdkClientFactory.GetSystemClient()
	run := systemSDKClient.GetWorkflow(ctx, token.WorkflowId, token.RunId)
	s.NoError(run.Get(ctx, nil))
}

// mergeMessages from the DLQ up to and including the specified message ID, blocking until the merge workflow completes.
func (s *dlqSuite) mergeMessages(ctx context.Context, maxMessageID int64) {
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"--" + tdbg.FlagDLQVersion, "v2",
		"merge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageID, 10),
		"--" + tdbg.FlagPageSize, "1", // to ensure that we test pagination
	}
	err := s.tdbgApp.Run(args)
	s.NoError(err)
	output := s.writer.Bytes()
	s.writer.Truncate(0)
	var response adminservice.MergeDLQTasksResponse
	s.NoError(jsonpb.Unmarshal(bytes.NewReader(output), &response))

	var token adminservice.DLQJobToken
	s.NoError(token.Unmarshal(response.GetJobToken()))

	systemSDKClient := s.sdkClientFactory.GetSystemClient()
	run := systemSDKClient.GetWorkflow(ctx, token.WorkflowId, token.RunId)
	s.NoError(run.Get(ctx, nil))
}

// readDLQTasks from the transfer task DLQ for this cluster and return them.
func (s *dlqSuite) readDLQTasks() []tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
	file := testutils.CreateTemp(s.T(), "", "*")
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"--" + tdbg.FlagDLQVersion, "v2",
		"read",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagOutputFilename, file.Name(),
	}
	s.NoError(s.tdbgApp.Run(args))
	dlqTasks := s.readTransferTasks(file)
	return dlqTasks
}

// verifyNumTasks verifies that the specified file contains the expected number of DLQ tasks, and that each task has the
// expected metadata and payload.
func (s *dlqSuite) verifyNumTasks(file *os.File, expectedNumTasks int) {
	dlqTasks := s.readTransferTasks(file)
	s.Len(dlqTasks, expectedNumTasks)

	for i, task := range dlqTasks {
		s.Equal(int64(persistence.FirstQueueMessageID+i), task.MessageID)
		taskInfo := task.Payload
		s.Equal(enums.TASK_TYPE_TRANSFER_WORKFLOW_TASK, taskInfo.TaskType)
		s.Equal("test-namespace", taskInfo.NamespaceId)
		s.Equal("test-workflow-id", taskInfo.WorkflowId)
		s.Equal("test-run-id", taskInfo.RunId)
		s.Equal(int64(42+i), taskInfo.TaskId)
	}
}

func (s *dlqSuite) readTransferTasks(file *os.File) []tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
	dlqTasks, err := tdbgtest.ParseDLQMessages(file, func() *persistencespb.TransferTaskInfo {
		return new(persistencespb.TransferTaskInfo)
	})
	s.NoError(err)
	return dlqTasks
}

// EnqueueTask is used to intercept writes to the DLQ, so that we can unblock the test upon completion.
func (t *testDLQWriter) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	res, err := t.QueueWriter.EnqueueTask(ctx, request)
	select {
	case t.suite.dlqTasks <- request.Task:
	case <-ctx.Done():
		return res, fmt.Errorf("interrupted while trying to observe DLQ write: %w", ctx.Err())
	}
	return res, err
}

// Wrap is used to wrap the executor with our own faulty one.
func (t testExecutorWrapper) Wrap(delegate queues.Executor) queues.Executor {
	return &testExecutor{
		base:  delegate,
		suite: t.suite,
	}
}

// Execute is used to wrap the executor so that we can intercept the workflow task and ensure it fails with a terminal
// error.
func (t testExecutor) Execute(ctx context.Context, e queues.Executable) queues.ExecuteResponse {
	if strings.HasPrefix(e.GetWorkflowID(), t.suite.failingWorkflowIDPrefix) && e.GetCategory() == tasks.CategoryTransfer {
		// Return a terminal error that will cause this task to be added to the DLQ.
		return queues.ExecuteResponse{
			ExecutionErr: serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, errors.New("test error")),
		}
	}
	return t.base.Execute(ctx, e)
}
