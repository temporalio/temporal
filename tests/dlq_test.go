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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"go.uber.org/fx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	DLQSuite struct {
		testcore.FunctionalTestBase
		*require.Assertions
		dlq              persistence.HistoryTaskQueueManager
		dlqTasks         chan tasks.Task
		writer           bytes.Buffer
		sdkClientFactory sdk.ClientFactory
		tdbgApp          *cli.App
		worker           sdkworker.Worker
		deleteBlockCh    chan interface{}

		failingWorkflowIDPrefix atomic.Pointer[string]
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
		suite *DLQSuite
	}
	testExecutor struct {
		base  queues.Executor
		suite *DLQSuite
	}
	testDLQWriter struct {
		suite *DLQSuite
		queues.QueueWriter
	}
	testTaskQueueManager struct {
		suite *DLQSuite
		persistence.HistoryTaskQueueManager
	}
)

const (
	testTimeout = 10 * time.Second * debug.TimeoutMultiplier
	taskQueue   = "dlq-test-task-queue"
)

func TestDLQSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DLQSuite))
}

func (s *DLQSuite) SetupSuite() {
	s.setAssertions()
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.HistoryTaskDLQEnabled.Key(): true,
	}
	s.SetDynamicConfigOverrides(dynamicConfigOverrides)
	s.dlqTasks = make(chan tasks.Task)
	testPrefix := "dlq-test-terminal-wfts-"
	s.failingWorkflowIDPrefix.Store(&testPrefix)
	s.FunctionalTestBase.SetupSuite(
		"testdata/es_cluster.yaml",
		testcore.WithFxOptionsForService(primitives.HistoryService,
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
			fx.Decorate(
				func(m persistence.HistoryTaskQueueManager) persistence.HistoryTaskQueueManager {
					return &testTaskQueueManager{
						suite:                   s,
						HistoryTaskQueueManager: m,
					}
				},
			),
		),
		testcore.WithFxOptionsForService(primitives.FrontendService,
			fx.Populate(&s.sdkClientFactory),
		),
	)
	s.tdbgApp = tdbgtest.NewCliApp(
		func(params *tdbg.Params) {
			params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(s.FrontendGRPCAddress()))
			params.Writer = &s.writer
		},
	)
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace(),
	})
	s.NoError(err)
	s.worker = sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
	s.worker.RegisterWorkflow(myWorkflow)
	s.NoError(s.worker.Start())
}

func (s *DLQSuite) TearDownSuite() {
	s.worker.Stop()
	s.FunctionalTestBase.TearDownSuite()
}

func myWorkflow(workflow.Context) (string, error) {
	return "hello", nil
}

func (s *DLQSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	s.setAssertions()
	s.deleteBlockCh = make(chan interface{})
	close(s.deleteBlockCh)
}

func (s *DLQSuite) setAssertions() {
	s.Assertions = require.New(s.T())
}

func (s *DLQSuite) TestReadArtificialDLQTasks() {
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
			SourceShardID: tasks.GetShardIDForTask(task, int(s.GetTestClusterConfig().HistoryConfig.NumHistoryShards)),
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
			err = s.tdbgApp.RunContext(ctx, args)
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
// This test will then call DescribeDLQJob and CancelDLQJob api to verify.
func (s *DLQSuite) TestPurgeRealWorkflow() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	_, dlqMessageID := s.executeDoomedWorkflow(ctx)

	// Delete the workflow task from the DLQ.
	token := s.purgeMessages(ctx, dlqMessageID)

	// Verify that the workflow task is no longer in the DLQ.
	dlqTasks := s.readDLQTasks(ctx)
	s.Empty(dlqTasks, "expected DLQ to be empty after purge")

	// Run DescribeJob and validate
	response := s.describeJob(ctx, token)
	s.Equal(enums.DLQ_OPERATION_TYPE_PURGE, response.OperationType)
	s.Equal(enums.DLQ_OPERATION_STATE_COMPLETED, response.OperationState)
	s.Equal(dlqMessageID, response.MaxMessageId)
	s.Equal(dlqMessageID, response.LastProcessedMessageId)
	s.Equal(int64(1), response.MessagesProcessed)

	// Try to cancel completed workflow
	cancelResponse := s.cancelJob(ctx, token)
	s.Equal(false, cancelResponse.Canceled)
}

// This test executes actual workflows for which we've set up an executor wrapper to return a terminal error. This
// causes the workflow tasks to be added to the DLQ. This tests the end-to-end functionality of the DLQ, whereas the
// above test is more for testing specific CLI flags when reading from the DLQ.
// This test will then call DescribeDLQJob and CancelDLQJob api to verify.
func (s *DLQSuite) TestMergeRealWorkflow() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	// Verify that we can execute a normal workflow.
	run := s.executeWorkflow(ctx, "dlq-test-ok-workflow-id")
	s.validateWorkflowRun(ctx, run)

	// Execute several doomed workflows.
	numWorkflows := 3
	var dlqMessageID int64
	var runs []sdkclient.WorkflowRun
	for i := 0; i < numWorkflows; i++ {
		run, dlqMessageID = s.executeDoomedWorkflow(ctx)
		runs = append(runs, run)
	}

	// Re-enqueue the workflow tasks from the DLQ, but don't fail its WFTs this time.
	nonExistantID := "some-workflow-id-that-wont-exist"
	s.failingWorkflowIDPrefix.Store(&nonExistantID)
	token := s.mergeMessages(ctx, dlqMessageID)

	// Verify that the workflow task was deleted from the DLQ after merging.
	dlqTasks := s.readDLQTasks(ctx)
	s.Empty(dlqTasks)

	// Verify that the workflows now eventually complete successfully.
	for i := 0; i < numWorkflows; i++ {
		s.validateWorkflowRun(ctx, runs[i])
	}

	// Run DescribeJob and validate
	response := s.describeJob(ctx, token)
	s.Equal(enums.DLQ_OPERATION_TYPE_MERGE, response.OperationType)
	s.Equal(enums.DLQ_OPERATION_STATE_COMPLETED, response.OperationState)
	s.Equal(dlqMessageID, response.MaxMessageId)
	s.Equal(dlqMessageID, response.LastProcessedMessageId)
	s.Equal(int64(numWorkflows), response.MessagesProcessed)

	// Try to cancel completed workflow
	cancelResponse := s.cancelJob(ctx, token)
	s.Equal(false, cancelResponse.Canceled)
}

func (s *DLQSuite) TestCancelRunningMerge() {
	s.deleteBlockCh = make(chan interface{})
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	// Execute several doomed workflows.
	_, dlqMessageID := s.executeDoomedWorkflow(ctx)

	token := s.mergeMessagesWithoutBlocking(ctx, dlqMessageID)

	// Try to cancel running workflow
	cancelResponse := s.cancelJob(ctx, token)
	s.Equal(true, cancelResponse.Canceled)
	// Unblock waiting tests on Delete
	close(s.deleteBlockCh)
	// Delete the workflow task from the DLQ.
	s.purgeMessages(ctx, dlqMessageID)
}

func (s *DLQSuite) TestListQueues() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()
	targetCluster := "active"
	category := tasks.CategoryTransfer
	sourceCluster := "test-source-cluster-" + s.T().Name()

	queueKey1 := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      category,
		SourceCluster: sourceCluster + "_1",
		TargetCluster: targetCluster,
	}
	_, err := s.dlq.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey1,
	})
	s.NoError(err)

	queueKey2 := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      category,
		SourceCluster: sourceCluster + "_2",
		TargetCluster: targetCluster,
	}
	_, err = s.dlq.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey2,
	})
	s.NoError(err)

	// Insert a message to second queue
	_, err = s.dlq.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		SourceCluster: sourceCluster + "_2",
		TargetCluster: targetCluster,
		Task:          &tasks.WorkflowTask{},
		SourceShardID: 1,
	})
	s.NoError(err)

	queueInfos := s.listQueues(ctx)
	qi0 := adminservice.ListQueuesResponse_QueueInfo{
		QueueName:    queueKey1.GetQueueName(),
		MessageCount: 0,
	}
	qi1 := adminservice.ListQueuesResponse_QueueInfo{
		QueueName:    queueKey2.GetQueueName(),
		MessageCount: 1,
	}
	var found0, found1 bool
	for _, qi := range queueInfos {
		found0 = found0 || proto.Equal(qi, &qi0)
		found1 = found1 || proto.Equal(qi, &qi1)

	}
	s.True(found0, "unable to find %v in %v", &qi0, queueInfos)
	s.True(found1, "unable to find %v in %v", &qi1, queueInfos)
}

func (s *DLQSuite) validateWorkflowRun(ctx context.Context, run sdkclient.WorkflowRun) {
	var result string
	err := run.Get(ctx, &result)
	s.NoError(err)
	s.Equal("hello", result)
}

// executeDoomedWorkflow runs a workflow that is guaranteed to produce a workflow task that will be added to the DLQ. It
// then returns the sdk workflow run and the message ID of the DLQ message for the failed workflow task.
func (s *DLQSuite) executeDoomedWorkflow(ctx context.Context) (sdkclient.WorkflowRun, int64) {
	// Execute a workflow.
	// Use a random workflow ID to ensure that we don't have any collisions with other runs.
	run := s.executeWorkflow(ctx, *s.failingWorkflowIDPrefix.Load()+uuid.New())

	// Wait for the workflow task to be added to the DLQ.
	select {
	case <-ctx.Done():
		s.FailNow("timed out waiting for workflow to task to be DLQ'd")
	case task := <-s.dlqTasks:
		s.Equal(run.GetRunID(), task.GetRunID())
	}

	// Verify that the workflow task is in the DLQ.
	task := s.verifyRunIsInDLQ(ctx, run)
	dlqMessageID := task.MessageID
	return run, dlqMessageID
}

func (s *DLQSuite) verifyRunIsInDLQ(
	ctx context.Context,
	run sdkclient.WorkflowRun,
) tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
	dlqTasks := s.readDLQTasks(ctx)
	for _, task := range dlqTasks {
		if task.Payload.RunId == run.GetRunID() {
			return task
		}
	}
	s.Fail("workflow task not found in DLQ", run.GetRunID())
	panic("unreachable")
}

// executeWorkflow just executes a simple no-op workflow that returns "hello" and returns the sdk workflow run.
func (s *DLQSuite) executeWorkflow(ctx context.Context, workflowID string) sdkclient.WorkflowRun {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace(),
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
func (s *DLQSuite) purgeMessages(ctx context.Context, maxMessageIDToDelete int64) string {
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"purge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageIDToDelete, 10),
	}
	err := s.tdbgApp.RunContext(ctx, args)
	s.NoError(err)
	output := s.writer.Bytes()
	s.writer.Truncate(0)
	var data map[string]string
	err = json.Unmarshal(output, &data)
	s.NoError(err)
	tokenString := data["jobToken"]

	var response adminservice.PurgeDLQTasksResponse
	s.NoError(protojson.Unmarshal(output, &response))
	var token adminservice.DLQJobToken
	s.NoError(proto.Unmarshal(response.GetJobToken(), &token))

	systemSDKClient := s.sdkClientFactory.GetSystemClient()
	run := systemSDKClient.GetWorkflow(ctx, token.WorkflowId, token.RunId)
	s.NoError(run.Get(ctx, nil))
	return tokenString
}

// mergeMessages from the DLQ up to and including the specified message ID, blocking until the merge workflow completes.
func (s *DLQSuite) mergeMessages(ctx context.Context, maxMessageID int64) string {
	tokenString := s.mergeMessagesWithoutBlocking(ctx, maxMessageID)
	tokenBytes, err := base64.StdEncoding.DecodeString(tokenString)
	s.NoError(err)
	var token adminservice.DLQJobToken
	s.NoError(token.Unmarshal(tokenBytes))
	systemSDKClient := s.sdkClientFactory.GetSystemClient()
	run := systemSDKClient.GetWorkflow(ctx, token.WorkflowId, token.RunId)
	s.NoError(run.Get(ctx, nil))
	return tokenString
}

// mergeMessages from the DLQ up to and including the specified message ID, returns immediately after running tdbg command.
func (s *DLQSuite) mergeMessagesWithoutBlocking(ctx context.Context, maxMessageID int64) string {
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"merge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageID, 10),
		"--" + tdbg.FlagPageSize, "1", // to ensure that we test pagination
	}
	err := s.tdbgApp.RunContext(ctx, args)
	s.NoError(err)
	output := s.writer.Bytes()
	s.writer.Truncate(0)
	var data map[string]string
	err = json.Unmarshal(output, &data)
	s.NoError(err)
	tokenString := data["jobToken"]
	var response adminservice.MergeDLQTasksResponse
	s.NoError(protojson.Unmarshal(output, &response))
	return tokenString
}

// readDLQTasks from the transfer task DLQ for this cluster and return them.
func (s *DLQSuite) readDLQTasks(ctx context.Context) []tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
	file := testutils.CreateTemp(s.T(), "", "*")
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"read",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagOutputFilename, file.Name(),
	}
	s.NoError(s.tdbgApp.RunContext(ctx, args))
	dlqTasks := s.readTransferTasks(file)
	return dlqTasks
}

// Calls describe dlq job and verify the output
func (s *DLQSuite) describeJob(ctx context.Context, token string) *adminservice.DescribeDLQJobResponse {
	args := []string{
		"tdbg",
		"dlq",
		"job",
		"describe",
		"--" + tdbg.FlagJobToken, token,
	}
	err := s.tdbgApp.RunContext(ctx, args)
	s.NoError(err)
	output := s.writer.Bytes()
	s.T().Log(string(output))
	s.writer.Truncate(0)
	var response adminservice.DescribeDLQJobResponse
	s.NoError(protojson.Unmarshal(output, &response))
	return &response
}

// Calls delete dlq job and verify the output
func (s *DLQSuite) cancelJob(ctx context.Context, token string) *adminservice.CancelDLQJobResponse {
	args := []string{
		"tdbg",
		"dlq",
		"job",
		"cancel",
		"--" + tdbg.FlagJobToken, token,
		"--" + tdbg.FlagReason, "testing cancel",
	}
	err := s.tdbgApp.RunContext(ctx, args)
	s.NoError(err)
	output := s.writer.Bytes()
	s.T().Log(string(output))
	s.writer.Truncate(0)
	var response adminservice.CancelDLQJobResponse
	s.NoError(protojson.Unmarshal(output, &response))
	return &response
}

// List all queues
func (s *DLQSuite) listQueues(ctx context.Context) []*adminservice.ListQueuesResponse_QueueInfo {
	args := []string{
		"tdbg",
		"dlq",
		"list",
		"--" + tdbg.FlagPrintJSON,
	}

	err := s.tdbgApp.RunContext(ctx, args)
	s.NoError(err)
	b := s.writer.Bytes()
	s.writer.Truncate(0)
	var arr []*adminservice.ListQueuesResponse_QueueInfo
	jsonpb := codec.NewJSONPBEncoder()
	err = jsonpb.DecodeSlice(b, func() proto.Message {
		resp := &adminservice.ListQueuesResponse_QueueInfo{}
		arr = append(arr, resp)
		return resp
	})
	s.NoError(err)
	return arr
}

// verifyNumTasks verifies that the specified file contains the expected number of DLQ tasks, and that each task has the
// expected metadata and payload.
func (s *DLQSuite) verifyNumTasks(file *os.File, expectedNumTasks int) {
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

func (s *DLQSuite) readTransferTasks(file *os.File) []tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
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
//
//nolint:err113
func (t testExecutor) Execute(ctx context.Context, e queues.Executable) queues.ExecuteResponse {
	if strings.HasPrefix(e.GetWorkflowID(), *t.suite.failingWorkflowIDPrefix.Load()) && e.GetCategory() == tasks.CategoryTransfer {
		// Return a terminal error that will cause this task to be added to the DLQ.
		return queues.ExecuteResponse{
			ExecutionErr: serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, errors.New("test error")),
		}
	}
	return t.base.Execute(ctx, e)
}

// ReadTasks is used to block the dlq job workflow until one of them is cancelled in TestCancelRunningMerge.
func (m *testTaskQueueManager) DeleteTasks(
	ctx context.Context,
	request *persistence.DeleteTasksRequest,
) (*persistence.DeleteTasksResponse, error) {
	<-m.suite.deleteBlockCh
	return m.HistoryTaskQueueManager.DeleteTasks(ctx, request)
}
