package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestPrematureEndOfStream reproduces the "premature end of stream" bug (ACT-536).
//
// Mechanism: speculative WFT metadata is never persisted (RecordWorkflowTaskStarted
// sets updateAction.Noop=true for speculative WFTs). Closing the shard drops the
// in-memory speculative events. When the SDK has a sticky cache miss and calls
// GetWorkflowExecutionHistory, the reopened shard's mutable state has no speculative
// events to append, producing a 2-event gap.
func TestPrematureEndOfStream(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())

	logCap := &logCapture{}
	armed := atomic.Bool{}
	shardClosed := atomic.Bool{}
	wfID := fmt.Sprintf("eos-%s", uuid.NewString()[:8])

	interceptor := grpc.WithUnaryInterceptor(func(
		ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if armed.Load() &&
			strings.HasSuffix(method, "GetWorkflowExecutionHistory") &&
			shardClosed.CompareAndSwap(false, true) {
			env.CloseShard(env.NamespaceID().String(), wfID)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})

	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:          env.GetTestCluster().Host().FrontendGRPCAddress(),
		Namespace:         env.Namespace().String(),
		Logger:            logCap,
		ConnectionOptions: sdkclient.ConnectionOptions{DialOptions: []grpc.DialOption{interceptor}},
	})
	require.NoError(t, err)
	defer client.Close()

	tq := fmt.Sprintf("eos-%s", uuid.NewString()[:8])
	w := sdkworker.New(client, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			_ = workflow.SetUpdateHandler(ctx, "u", func(ctx workflow.Context) error { return nil })
			workflow.GetSignalChannel(ctx, "done").Receive(ctx, nil)
			return nil
		},
		workflow.RegisterOptions{Name: "wf"},
	)
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: wfID, TaskQueue: tq, WorkflowRunTimeout: 60 * time.Second,
	}, "wf")
	require.NoError(t, err)

	// Wait for first WFT to complete (establishes sticky binding).
	require.Eventually(t, func() bool {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx,
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run.GetRunID()},
			})
		return err == nil && resp.GetWorkflowExecutionInfo().GetHistoryLength() >= 4
	}, 5*time.Second, 50*time.Millisecond)

	// Purge SDK sticky cache → next WFT on sticky queue will be a cache miss.
	sdkworker.PurgeStickyWorkflowCache()
	armed.Store(true)

	// Send Update → speculative WFT on sticky queue → SDK cache miss →
	// GetWorkflowExecutionHistory. The interceptor closes the shard before the
	// RPC, so speculative events are lost.
	handle, err := client.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID: wfID, RunID: run.GetRunID(),
		UpdateName: "u", WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		_ = handle.Get(ctx, nil)
	}

	t.Log(strings.Join(logCap.messages, "\n"))

	require.Eventually(t, func() bool {
		return logCap.contains("CONFIRMING THAT SEANS CHANGES ARE ACTUALLY RUNNING")
	}, 10*time.Second, 100*time.Millisecond,
		"expected SDK to log 'premature end of stream'")

	// require.Eventually(t, func() bool {
	// 	return logCap.contains("premature end of stream")
	// }, 10*time.Second, 100*time.Millisecond,
	// 	"expected SDK to log 'premature end of stream'")

	_ = client.TerminateWorkflow(ctx, wfID, run.GetRunID(), "cleanup")
}

// TestConsecutiveWorkflowTasksViaForceCreate demonstrates the history pattern
//
//	WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskCompleted,
//	WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskCompleted
//
// which occurs when RespondWorkflowTaskCompleted is called with
// ForceCreateNewWorkflowTask=true. The server immediately schedules a new WFT
// without any intervening events (no activities, signals, or timers).
//
// In practice the Go SDK does this when executing local activities: it
// "heartbeats" the current WFT to extend its timeout while local activities
// run, producing consecutive WFT cycles.
func TestConsecutiveWorkflowTasksViaForceCreate(t *testing.T) {
	env := testcore.NewEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := fmt.Sprintf("consec-wft-%s", uuid.NewString()[:8])
	tq := fmt.Sprintf("tq-%s", uuid.NewString()[:8])
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start a workflow.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          env.Namespace().String(),
		RequestId:          uuid.NewString(),
		WorkflowId:         wfID,
		WorkflowType:       &commonpb.WorkflowType{Name: "test-workflow"},
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: durationpb.New(60 * time.Second),
	})
	require.NoError(t, err)

	we := &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: startResp.RunId}

	// Poll and start the first WFT.
	poll1, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
	})
	require.NoError(t, err)
	require.NotEmpty(t, poll1.TaskToken)

	// Complete the first WFT with ForceCreateNewWorkflowTask=true.
	// The server immediately schedules a second WFT with no intervening events,
	// and ReturnNewWorkflowTask=true delivers its token in-line.
	resp, err := env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:                  env.Namespace().String(),
		TaskToken:                  poll1.TaskToken,
		ForceCreateNewWorkflowTask: true,
		ReturnNewWorkflowTask:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.WorkflowTask)

	// Complete the second WFT, finishing the workflow.
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: resp.WorkflowTask.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	require.NoError(t, err)

	// Events 2-7 are the two consecutive WFT cycles with nothing in between.
	env.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionCompleted`, env.GetHistory(env.Namespace().String(), we))
}

type logCapture struct {
	mu       sync.Mutex
	messages []string
}

var _ sdklog.Logger = (*logCapture)(nil)

func (l *logCapture) Debug(string, ...interface{}) {}
func (l *logCapture) Info(string, ...interface{})  {}
func (l *logCapture) Warn(msg string, kv ...interface{}) {
	l.mu.Lock()
	l.messages = append(l.messages, fmt.Sprintf("%s %v", msg, kv))
	l.mu.Unlock()
}
func (l *logCapture) Error(msg string, kv ...interface{}) {
	l.mu.Lock()
	l.messages = append(l.messages, fmt.Sprintf("%s %v", msg, kv))
	l.mu.Unlock()
}
func (l *logCapture) With(...interface{}) sdklog.Logger { return l }
func (l *logCapture) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.messages {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}
