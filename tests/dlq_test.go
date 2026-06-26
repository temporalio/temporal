package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	DLQSuite struct {
		parallelsuite.Suite[*DLQSuite]
	}
	dlqTestEnv struct {
		*testcore.TestEnv

		dlq             persistence.HistoryTaskQueueManager
		writer          bytes.Buffer
		systemSDKClient sdkclient.Client
		deleteBlockCh   chan any

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
)

func TestDLQSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &DLQSuite{}) //nolint:staticcheck // SA1019: DLQ tests use dedicated clusters with fault injection and worker-service DLQ jobs.
}

func (s *DLQSuite) newTestEnv(opts ...testcore.TestOption) *dlqTestEnv {
	w := &dlqTestEnv{}
	testPrefix := "dlq-test-terminal-wfts-"
	w.failingWorkflowIDPrefix.Store(&testPrefix)

	baseOpts := []testcore.TestOption{
		// Return a terminal error that will cause workflow task to be added to the DLQ.
		testcore.WithPersistenceFaultInjection(&config.FaultInjection{
			Injector: func(target config.FaultInjectionTarget) error {
				if target.Store != config.ExecutionStoreName || target.Method != "GetWorkflowExecution" {
					return nil
				}
				request, ok := target.Request.(*persistence.GetWorkflowExecutionRequest)
				if !ok || !strings.HasPrefix(request.WorkflowID, *w.failingWorkflowIDPrefix.Load()) {
					return nil
				}
				return serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, errors.New("test error"))
			},
		}),
	}
	w.TestEnv = testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
	w.SdkWorker().RegisterWorkflow(s.myWorkflow)

	var err error
	w.dlq, err = w.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
	s.NoError(err)
	s.T().Cleanup(w.dlq.Close)

	w.systemSDKClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  w.FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	s.T().Cleanup(w.systemSDKClient.Close)

	w.deleteBlockCh = make(chan any)
	close(w.deleteBlockCh)

	// DeleteDLQTasks is used to block the dlq job workflow until one of them is cancelled in TestCancelRunningMerge.
	w.InjectHook(testhooks.NewHook(
		testhooks.HistoryDLQTaskDeleteInterceptor,
		func(
			ctx context.Context,
			request *historyservice.DeleteDLQTasksRequest,
			deleteTasks func(context.Context, *historyservice.DeleteDLQTasksRequest) (*historyservice.DeleteDLQTasksResponse, error),
		) (*historyservice.DeleteDLQTasksResponse, error) {
			<-w.deleteBlockCh
			return deleteTasks(ctx, request)
		},
	))

	return w
}

func (env *dlqTestEnv) runTdbg(ctx context.Context, args []string) error {
	return tdbgtest.NewCliApp(
		func(params *tdbg.Params) {
			params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(env.FrontendGRPCAddress()))
			params.Writer = &env.writer
		},
	).RunContext(ctx, args)
}

func (s *DLQSuite) myWorkflow(workflow.Context) (string, error) {
	return "hello", nil
}

func (s *DLQSuite) TestReadArtificialDLQTasks() {
	env := s.newTestEnv()

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
	_, err := env.dlq.CreateQueue(s.Context(), &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	s.NoError(err)
	for i := range 4 {
		task := &tasks.WorkflowTask{
			WorkflowKey: workflowKey,
			TaskID:      int64(42 + i),
		}
		_, err := env.dlq.EnqueueTask(s.Context(), &persistence.EnqueueTaskRequest{
			QueueType:     queueKey.QueueType,
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
			Task:          task,
			SourceShardID: tasks.GetShardIDForTask(task, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards)),
		})
		s.NoError(err)
	}
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
		s.Run(tc.name, func(s *DLQSuite) {
			file := testutils.CreateTemp(s.T(), "", "*")
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
			err := env.runTdbg(s.Context(), args)
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
	env := s.newTestEnv(testcore.WithWorkerService("dlq purge workflow"))

	_, dlqMessageID := s.executeDoomedWorkflow(env)

	// Delete the workflow task from the DLQ.
	token := s.purgeMessages(env, dlqMessageID)

	// Verify that the workflow task is no longer in the DLQ.
	dlqTasks := s.readDLQTasks(env)
	s.Empty(dlqTasks, "expected DLQ to be empty after purge")

	// Run DescribeJob and validate
	response := s.describeJob(env, token)
	s.Equal(enumsspb.DLQ_OPERATION_TYPE_PURGE, response.OperationType)
	s.Equal(enumsspb.DLQ_OPERATION_STATE_COMPLETED, response.OperationState)
	s.Equal(dlqMessageID, response.MaxMessageId)
	s.Equal(dlqMessageID, response.LastProcessedMessageId)
	s.Equal(int64(1), response.MessagesProcessed)

	// Try to cancel completed workflow
	cancelResponse := s.cancelJob(env, token)
	s.False(cancelResponse.Canceled)
}

// This test executes actual workflows for which we've set up an executor wrapper to return a terminal error. This
// causes the workflow tasks to be added to the DLQ. This tests the end-to-end functionality of the DLQ, whereas the
// above test is more for testing specific CLI flags when reading from the DLQ.
// This test will then call DescribeDLQJob and CancelDLQJob api to verify.
func (s *DLQSuite) TestMergeRealWorkflow() {
	env := s.newTestEnv(testcore.WithWorkerService("dlq merge workflow"))

	// Verify that we can execute a normal workflow.
	run := s.executeWorkflow(env, "dlq-test-ok-workflow-id")
	s.validateWorkflowRun(env, run)

	// Execute several doomed workflows.
	numWorkflows := 3
	var dlqMessageID int64
	var runs []sdkclient.WorkflowRun
	for range numWorkflows {
		run, dlqMessageID = s.executeDoomedWorkflow(env)
		runs = append(runs, run)
	}

	// Re-enqueue the workflow tasks from the DLQ, but don't fail its WFTs this time.
	nonExistantID := "some-workflow-id-that-wont-exist"
	env.failingWorkflowIDPrefix.Store(&nonExistantID)
	token := s.mergeMessages(env, dlqMessageID)

	// Verify that the workflow task was deleted from the DLQ after merging.
	dlqTasks := s.readDLQTasks(env)
	s.Empty(dlqTasks)

	// Verify that the workflows now eventually complete successfully.
	for i := range numWorkflows {
		s.validateWorkflowRun(env, runs[i])
	}

	// Run DescribeJob and validate
	response := s.describeJob(env, token)
	s.Equal(enumsspb.DLQ_OPERATION_TYPE_MERGE, response.OperationType)
	s.Equal(enumsspb.DLQ_OPERATION_STATE_COMPLETED, response.OperationState)
	s.Equal(dlqMessageID, response.MaxMessageId)
	s.Equal(dlqMessageID, response.LastProcessedMessageId)
	s.Equal(int64(numWorkflows), response.MessagesProcessed)

	// Try to cancel completed workflow
	cancelResponse := s.cancelJob(env, token)
	s.False(cancelResponse.Canceled)
}

func (s *DLQSuite) TestCancelRunningMerge() {
	env := s.newTestEnv(testcore.WithWorkerService("dlq merge workflow"))
	env.deleteBlockCh = make(chan any)

	// Execute several doomed workflows.
	_, dlqMessageID := s.executeDoomedWorkflow(env)

	token := s.mergeMessagesWithoutBlocking(env, dlqMessageID)

	// Try to cancel running workflow
	cancelResponse := s.cancelJob(env, token)
	s.True(cancelResponse.Canceled)
	// Unblock waiting tests on Delete
	close(env.deleteBlockCh)
	// Delete the workflow task from the DLQ.
	s.purgeMessages(env, dlqMessageID)
}

func (s *DLQSuite) TestListQueues() {
	env := s.newTestEnv()
	targetCluster := "active"
	category := tasks.CategoryTransfer
	sourceCluster := "test-source-cluster-" + s.T().Name()

	queueKey1 := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      category,
		SourceCluster: sourceCluster + "_1",
		TargetCluster: targetCluster,
	}
	_, err := env.dlq.CreateQueue(s.Context(), &persistence.CreateQueueRequest{
		QueueKey: queueKey1,
	})
	s.NoError(err)

	queueKey2 := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      category,
		SourceCluster: sourceCluster + "_2",
		TargetCluster: targetCluster,
	}
	_, err = env.dlq.CreateQueue(s.Context(), &persistence.CreateQueueRequest{
		QueueKey: queueKey2,
	})
	s.NoError(err)

	// Insert a message to second queue
	_, err = env.dlq.EnqueueTask(s.Context(), &persistence.EnqueueTaskRequest{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		SourceCluster: sourceCluster + "_2",
		TargetCluster: targetCluster,
		Task:          &tasks.WorkflowTask{},
		SourceShardID: 1,
	})
	s.NoError(err)

	queueInfos := s.listQueues(env)
	qi0 := adminservice.ListQueuesResponse_QueueInfo{
		QueueName:     queueKey1.GetQueueName(),
		MessageCount:  0,
		LastMessageId: -1,
	}
	qi1 := adminservice.ListQueuesResponse_QueueInfo{
		QueueName:     queueKey2.GetQueueName(),
		MessageCount:  1,
		LastMessageId: 0,
	}
	var found0, found1 bool
	for _, qi := range queueInfos {
		found0 = found0 || proto.Equal(qi, &qi0)
		found1 = found1 || proto.Equal(qi, &qi1)

	}
	s.True(found0, "unable to find %v in %v", &qi0, queueInfos)
	s.True(found1, "unable to find %v in %v", &qi1, queueInfos)
}

func (s *DLQSuite) validateWorkflowRun(env *dlqTestEnv, run sdkclient.WorkflowRun) {
	var result string
	err := run.Get(s.Context(), &result)
	s.NoError(err)
	s.Equal("hello", result)
}

// executeDoomedWorkflow runs a workflow that is guaranteed to produce a workflow task that will be added to the DLQ. It
// then returns the sdk workflow run and the message ID of the DLQ message for the failed workflow task.
func (s *DLQSuite) executeDoomedWorkflow(env *dlqTestEnv) (sdkclient.WorkflowRun, int64) {
	// Execute a workflow.
	// Use a random workflow ID to ensure that we don't have any collisions with other runs.
	run := s.executeWorkflow(env, *env.failingWorkflowIDPrefix.Load()+uuid.NewString())

	// Wait for the workflow task to be added to the DLQ.
	var found *tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo]
	await.Require(s.Context(), s.T(), func(t *await.T) {
		dlqTasks := s.readDLQTasks(env)
		for _, task := range dlqTasks {
			if task.Payload.RunId == run.GetRunID() {
				found = &task
				return
			}
		}
		require.Failf(t, "workflow task not found in DLQ", "run ID: %s", run.GetRunID())
	}, 10*time.Second, 100*time.Millisecond)

	return run, found.MessageID
}

// executeWorkflow just executes a simple no-op workflow that returns "hello" and returns the sdk workflow run.
func (s *DLQSuite) executeWorkflow(env *dlqTestEnv, workflowID string) sdkclient.WorkflowRun {
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: env.WorkerTaskQueue(),
	}, s.myWorkflow)
	s.NoError(err)
	return run
}

// purgeMessages from the DLQ up to and including the specified message ID, blocking until the purge workflow completes.
func (s *DLQSuite) purgeMessages(env *dlqTestEnv, maxMessageIDToDelete int64) string {
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"purge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageIDToDelete, 10),
	}
	err := env.runTdbg(s.Context(), args)
	s.NoError(err)
	output := env.writer.Bytes()
	env.writer.Truncate(0)
	var data map[string]string
	err = json.Unmarshal(output, &data)
	s.NoError(err)
	tokenString := data["jobToken"]

	var response adminservice.PurgeDLQTasksResponse
	s.NoError(protojson.Unmarshal(output, &response))
	var token adminservice.DLQJobToken
	s.NoError(proto.Unmarshal(response.GetJobToken(), &token))

	run := env.systemSDKClient.GetWorkflow(s.Context(), token.WorkflowId, token.RunId)
	s.NoError(run.Get(s.Context(), nil))
	return tokenString
}

// mergeMessages from the DLQ up to and including the specified message ID, blocking until the merge workflow completes.
func (s *DLQSuite) mergeMessages(env *dlqTestEnv, maxMessageID int64) string {
	tokenString := s.mergeMessagesWithoutBlocking(env, maxMessageID)
	tokenBytes, err := base64.StdEncoding.DecodeString(tokenString)
	s.NoError(err)
	var token adminservice.DLQJobToken
	s.NoError(token.Unmarshal(tokenBytes))
	run := env.systemSDKClient.GetWorkflow(s.Context(), token.WorkflowId, token.RunId)
	s.NoError(run.Get(s.Context(), nil))
	return tokenString
}

// mergeMessages from the DLQ up to and including the specified message ID, returns immediately after running tdbg command.
func (s *DLQSuite) mergeMessagesWithoutBlocking(env *dlqTestEnv, maxMessageID int64) string {
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"merge",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagLastMessageID, strconv.FormatInt(maxMessageID, 10),
		"--" + tdbg.FlagPageSize, "1", // to ensure that we test pagination
	}
	err := env.runTdbg(s.Context(), args)
	s.NoError(err)
	output := env.writer.Bytes()
	env.writer.Truncate(0)
	var data map[string]string
	err = json.Unmarshal(output, &data)
	s.NoError(err)
	tokenString := data["jobToken"]
	var response adminservice.MergeDLQTasksResponse
	s.NoError(protojson.Unmarshal(output, &response))
	return tokenString
}

// readDLQTasks from the transfer task DLQ for this cluster and return them.
func (s *DLQSuite) readDLQTasks(env *dlqTestEnv) []tdbgtest.DLQMessage[*persistencespb.TransferTaskInfo] {
	file := testutils.CreateTemp(s.T(), "", "*")
	args := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
		"read",
		"--" + tdbg.FlagDLQType, strconv.Itoa(tasks.CategoryTransfer.ID()),
		"--" + tdbg.FlagOutputFilename, file.Name(),
	}
	s.NoError(env.runTdbg(s.Context(), args))
	dlqTasks := s.readTransferTasks(file)
	return dlqTasks
}

// Calls describe dlq job and verify the output
func (s *DLQSuite) describeJob(env *dlqTestEnv, token string) *adminservice.DescribeDLQJobResponse {
	args := []string{
		"tdbg",
		"dlq",
		"job",
		"describe",
		"--" + tdbg.FlagJobToken, token,
	}
	err := env.runTdbg(s.Context(), args)
	s.NoError(err)
	output := env.writer.Bytes()
	s.T().Log(string(output))
	env.writer.Truncate(0)
	var response adminservice.DescribeDLQJobResponse
	s.NoError(protojson.Unmarshal(output, &response))
	return &response
}

// Calls delete dlq job and verify the output
func (s *DLQSuite) cancelJob(env *dlqTestEnv, token string) *adminservice.CancelDLQJobResponse {
	args := []string{
		"tdbg",
		"dlq",
		"job",
		"cancel",
		"--" + tdbg.FlagJobToken, token,
		"--" + tdbg.FlagReason, "testing cancel",
	}
	err := env.runTdbg(s.Context(), args)
	s.NoError(err)
	output := env.writer.Bytes()
	s.T().Log(string(output))
	env.writer.Truncate(0)
	var response adminservice.CancelDLQJobResponse
	s.NoError(protojson.Unmarshal(output, &response))
	return &response
}

// List all queues
func (s *DLQSuite) listQueues(env *dlqTestEnv) []*adminservice.ListQueuesResponse_QueueInfo {
	args := []string{
		"tdbg",
		"dlq",
		"list",
		"--" + tdbg.FlagPrintJSON,
	}

	err := env.runTdbg(s.Context(), args)
	s.NoError(err)
	b := env.writer.Bytes()
	env.writer.Truncate(0)
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
		s.Equal(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, taskInfo.TaskType)
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
