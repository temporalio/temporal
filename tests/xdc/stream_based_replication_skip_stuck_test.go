package xdc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
)

// This test covers when the replication stream sender cannot *build* ("convert") a task
// and its retries are exhausted, and history.ReplicationStreamSenderSkipStuckTask is enabled, the
// sender must skip (discard) the task and advance past it instead of wedging the whole stream. It
// uses the ReplicationStreamSenderTaskInterceptor test hook to force one workflow's tasks to always
// fail conversion (mirroring the production poison pill, e.g. "version histories does not contains
// given item"), then verifies (a) a subsequent workflow still replicates (the stream is not wedged)
// and (b) the failed workflow was dropped (never replicated).
type streamSenderSkipStuckSuite struct {
	xdcBaseSuite

	// failWorkflowID is the workflow whose replication tasks the sender hook forces to fail.
	failWorkflowID atomic.Pointer[string]
	// convertAttempts receives the workflowID each time the sender hook injects a conversion
	// failure, so the test can confirm the stuck path was actually exercised.
	convertAttempts chan string
}

func TestStreamSenderSkipStuckSuite(t *testing.T) {
	t.Parallel()
	s := &streamSenderSkipStuckSuite{}
	suite.Run(t, s)
}

func (s *streamSenderSkipStuckSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():              true,
		dynamicconfig.ReplicationStreamSenderSkipStuckTask.Key(): true,
		// Bound the give-up to a single fast attempt so the test doesn't wait the ~3 minute
		// production retry budget before the task is skipped.
		dynamicconfig.ReplicationStreamSenderErrorRetryMaxAttempts.Key(): 1,
		dynamicconfig.ReplicationStreamSenderErrorRetryExpiration.Key():  time.Second,
		dynamicconfig.ReplicationStreamSenderErrorRetryWait.Key():        time.Millisecond,
	}

	s.convertAttempts = make(chan string, 100)
	noFail := ""
	s.failWorkflowID.Store(&noFail)

	s.logger = log.NewTestLogger()
	s.setupSuite()

	for _, cluster := range s.clusters {
		cluster.InjectHook(s.T(), testhooks.NewHook(
			testhooks.ReplicationStreamSenderTaskInterceptor,
			s.interceptSenderTask,
		), testhooks.GlobalScope)
	}
}

func (s *streamSenderSkipStuckSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *streamSenderSkipStuckSuite) SetupTest() {
	s.setupTest()
}

// interceptSenderTask forces the configured workflow's replication tasks to fail conversion with a
// retryable error (an Internal error, as the real "version histories does not contains given item"
// poison pill surfaces); all other tasks convert normally.
func (s *streamSenderSkipStuckSuite) interceptSenderTask(task tasks.Task, convert func() error) error {
	if failID := *s.failWorkflowID.Load(); failID != "" && task.GetWorkflowID() == failID {
		select {
		case s.convertAttempts <- task.GetWorkflowID():
		default:
		}
		return serviceerror.NewInternal("injected conversion failure for stuck-task test")
	}
	return convert()
}

func (s *streamSenderSkipStuckSuite) TestStuckTaskIsSkippedAndStreamKeepsFlowing() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ns := s.createGlobalNamespace()

	activeClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)
	defer activeClient.Close()

	tq := "stream-sender-skip-stuck-task-queue"
	worker := sdkworker.New(activeClient, tq, sdkworker.Options{})
	echoWorkflow := func(ctx workflow.Context) (string, error) {
		return "hello", nil
	}
	worker.RegisterWorkflow(echoWorkflow)
	s.NoError(worker.Start())
	defer worker.Stop()

	// Force the sender to fail this workflow's replication tasks.
	stuckID := "stuck-" + uuid.NewString()
	s.failWorkflowID.Store(&stuckID)

	// Run the stuck workflow to completion on the active cluster. Its replication tasks will
	// repeatedly fail to convert and, with the flag on, be skipped.
	var result string
	stuckRun, err := activeClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        stuckID,
		TaskQueue: tq,
	}, echoWorkflow)
	s.NoError(err)
	s.NoError(stuckRun.Get(ctx, &result))
	s.Equal("hello", result)

	// Confirm the sender actually attempted (and failed) to convert the stuck workflow's task.
	select {
	case <-s.convertAttempts:
	case <-ctx.Done():
		s.FailNow("timed out waiting for the sender to attempt the stuck workflow's task")
	}

	// Start a second, non-failing workflow AFTER the stuck one. With a single history shard, its
	// task sits behind the stuck task in the same stream: it can only replicate if the stuck task
	// was skipped and the watermark advanced past it.
	okID := "ok-" + uuid.NewString()
	okRun, err := activeClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        okID,
		TaskQueue: tq,
	}, echoWorkflow)
	s.NoError(err)
	s.NoError(okRun.Get(ctx, &result))
	s.Equal("hello", result)

	standbyFrontend := s.clusters[1].FrontendClient()

	// The non-failing workflow must replicate to the standby: proof the stream is not wedged.
	await.Require(ctx, s.T(), func(t *await.T) {
		_, err := standbyFrontend.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: okID},
		})
		require.NoError(t, err)
	}, replicationWaitTime, replicationCheckInterval)

	// The stuck workflow was discarded and should never appear on the standby.
	_, err = standbyFrontend.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: stuckID},
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
}
