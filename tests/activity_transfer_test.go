package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/rpc/grpcfaults"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type activityTransferSuite struct {
	parallelsuite.Suite[*activityTransferSuite]
}

func TestActivityTransferPriority(t *testing.T) {
	parallelsuite.Run(t, &activityTransferSuite{}, false)
}

func TestActivityTransferFair(t *testing.T) {
	parallelsuite.Run(t, &activityTransferSuite{}, true)
}

func (s *activityTransferSuite) TestLostStartResponse(fair bool) {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newEnv(fair, testcore.WithSpanExporter(exporter))
	ctx := s.Context()
	workflowID := env.Tv().WorkflowID()

	var injected atomic.Bool
	type heldStart struct {
		response  *historyservice.RecordActivityTaskStartedResponse
		remaining time.Duration
	}
	committed := make(chan heldStart, 1)
	originalAdd := make(chan trace.SpanContext, 1)
	recovering := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var firstAdd, firstRecovery sync.Once
	faults := env.GetTestCluster().Host().GetGRPCFaultGenerator()
	s.T().Cleanup(faults.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: env.NamespaceID()},
		func(rpcCtx context.Context, _ string, req any) *grpcfaults.Outcome {
			switch r := req.(type) {
			case *matchingservice.AddActivityTaskRequest:
				if r.GetExecution().GetWorkflowId() == workflowID {
					firstAdd.Do(func() {
						// The server span's remote parent identifies this one client invocation.
						originalAdd <- trace.SpanFromContext(rpcCtx).(sdktrace.ReadOnlySpan).Parent()
					})
				}
			case *historyservice.RecoverActivityTaskStartedRequest:
				if r.GetStartRequest().GetWorkflowExecution().GetWorkflowId() == workflowID {
					firstRecovery.Do(func() { close(recovering) })
					select {
					case <-release:
					case <-rpcCtx.Done():
						return &grpcfaults.Outcome{Error: rpcCtx.Err()}
					}
				}
			default:
				return nil
			}
			return nil
		}))
	// The post-handler hook has the actual start RPC's deadline. History has already committed.
	unregister := env.GetTestCluster().Host().GetGRPCFaultGenerator().RegisterResponseCallback(
		grpcfaults.Scope{NamespaceID: env.NamespaceID()},
		func(rpcCtx context.Context, _ string, req, resp any, err error) *grpcfaults.Outcome {
			start, ok := req.(*historyservice.RecordActivityTaskStartedRequest)
			if !ok || start.GetWorkflowExecution().GetWorkflowId() != workflowID || err != nil || !injected.CompareAndSwap(false, true) {
				return nil
			}
			deadline, _ := rpcCtx.Deadline()
			committed <- heldStart{response: resp.(*historyservice.RecordActivityTaskStartedResponse), remaining: time.Until(deadline)}
			select {
			case <-rpcCtx.Done():
				return &grpcfaults.Outcome{Error: rpcCtx.Err()}
			case <-ctx.Done():
				return &grpcfaults.Outcome{Error: ctx.Err()}
			}
		})
	s.T().Cleanup(unregister)

	var deliveries atomic.Int32
	tokens := make(chan []byte, 1)
	w := worker.New(env.SdkClient(), env.Tv().TaskQueue().Name, worker.Options{Identity: env.Tv().WorkerIdentity()})
	w.RegisterActivityWithOptions(func(ctx context.Context, _ string) error {
		if deliveries.Add(1) == 1 {
			tokens <- activity.GetInfo(ctx).TaskToken
		}
		return nil
	}, activity.RegisterOptions{Name: "testWFA"})
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)
	s.waitForPoller(env, env.Tv().WorkerIdentity())

	run := s.startActivity(env, workflowID)
	held := await.Rcv(s.T(), committed)
	s.Positive(held.remaining, "the start must commit before the RPC deadline")
	s.LessOrEqual(held.remaining, time.Second, "expected the one-second sync-match start RPC")
	start := held.response
	s.Equal(int32(1), start.GetAttempt())
	s.NotNil(start.GetClock())
	add := await.Rcv(s.T(), originalAdd)
	s.True(add.IsValid())
	await.Rcv(s.T(), recovering)
	s.Await(func(s *activityTransferSuite) {
		for _, span := range exporter.GetSpans() {
			if span.SpanContext.SpanID() == add.SpanID() && span.SpanContext.TraceID() == add.TraceID() {
				s.Equal(trace.SpanKindClient, span.SpanKind)
				s.Less(span.EndTime.Sub(span.StartTime), 3*time.Second)
				s.Contains(span.Attributes, semconv.RPCResponseStatusCode("OK"))
				return
			}
		}
		s.Fail("original AddActivityTask client span has not finished")
	}, 8*time.Second, 25*time.Millisecond)
	unblock()
	s.NoErrorf(run.Get(ctx, nil), "History committed attempt 1. Worker deliveries: %d", deliveries.Load())
	s.Equal(int32(1), deliveries.Load())
	token, err := tasktoken.NewSerializer().Deserialize(await.Rcv(s.T(), tokens))
	s.NoError(err)
	s.Equal(start.GetScheduledEvent().GetEventId(), token.GetScheduledEventId())
	s.Equal(start.GetAttempt(), token.GetAttempt())
	s.ProtoEqual(start.GetClock(), token.GetClock())
	s.Equal(start.GetVersion(), token.GetVersion())
	s.Equal(start.GetStartVersion(), token.GetStartVersion())
	s.requireSingleCompletedActivity(env, run)
}

func (s *activityTransferSuite) TestLostAddResponse(fair bool) {
	env := s.newEnv(fair)
	ctx := s.Context()
	workflowID := env.Tv().WorkflowID()
	const workerA, workerB = "worker-a", "worker-b"

	var addCount atomic.Int32
	adds := make(chan *clockspb.VectorClock, 2)
	releaseAdd := make(chan struct{})
	secondStart := make(chan struct{}, 1)
	env.InjectResponseFault(func(req, _ any, handlerErr error) error {
		switch r := req.(type) {
		case *matchingservice.AddActivityTaskRequest:
			if r.GetExecution().GetWorkflowId() != workflowID || handlerErr != nil {
				return nil
			}
			n := addCount.Add(1)
			if n <= 2 {
				adds <- r.GetClock()
			}
			if n != 1 {
				return nil
			}
			select {
			case <-releaseAdd:
				return serviceerror.NewUnavailable("lost AddActivityTask response")
			case <-ctx.Done():
				return ctx.Err()
			}
		case *historyservice.RecordActivityTaskStartedRequest:
			if r.GetWorkflowExecution().GetWorkflowId() == workflowID && r.GetPollRequest().GetIdentity() == workerB {
				select {
				case secondStart <- struct{}{}:
				default:
				}
			}
			return nil
		default:
			return nil
		}
	})

	firstPoll := s.pollActivity(env, workerA, "")
	run := s.startActivity(env, workflowID)
	first, err := firstPoll.Get(ctx)
	s.NoError(err)
	s.Equal(workflowID, first.GetWorkflowExecution().GetWorkflowId())
	s.Equal(int32(1), first.GetAttempt())
	originalTransfer := await.Rcv(s.T(), adds)
	s.NotNil(originalTransfer)
	secondPoll := s.pollActivity(env, workerB, "")
	close(releaseAdd)
	s.ProtoEqual(originalTransfer, await.Rcv(s.T(), adds))
	await.Rcv(s.T(), secondStart)

	// Give B's outstanding poll another Activity. A duplicate delivery would return A's Activity here.
	control := s.startActivity(env, env.Tv().WithWorkflowIDNumber(2).WorkflowID())
	second, err := secondPoll.Get(ctx)
	s.NoError(err)
	s.Equal(control.GetRunID(), second.GetWorkflowExecution().GetRunId(), "worker B received worker A's running Activity")
	for _, task := range []*workflowservice.PollActivityTaskQueueResponse{first, second} {
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
		})
		s.NoError(err)
	}
	s.NoError(run.Get(ctx, nil))
	s.NoError(control.Get(ctx, nil))
	s.requireSingleCompletedActivity(env, run)
}

func (s *activityTransferSuite) pollActivity(env *testcore.TestEnv, identity, controlQueue string) future.Future[*workflowservice.PollActivityTaskQueueResponse] {
	pollCtx, cancel := context.WithCancel(s.Context())
	s.T().Cleanup(cancel)
	result := future.NewFuture[*workflowservice.PollActivityTaskQueueResponse]()
	go func() {
		result.Set(env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(), TaskQueue: env.Tv().TaskQueue(),
			Identity: identity, WorkerInstanceKey: identity, WorkerControlTaskQueue: controlQueue,
		}))
	}()
	s.waitForPoller(env, identity)
	return result
}

func (s *activityTransferSuite) TestLostRecoveryResponse(fair bool) {
	s.lostRecoveryResponse(fair, false)
}

func (s *activityTransferSuite) TestRecoveredCancellationToken(fair bool) {
	s.lostRecoveryResponse(fair, true)
}

func (s *activityTransferSuite) lostRecoveryResponse(fair, cancelActivity bool) {
	env := s.newEnv(fair, testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))
	workflowID := env.Tv().WorkflowID()
	faults := env.GetTestCluster().Host().GetGRPCFaultGenerator()
	scope := grpcfaults.Scope{NamespaceID: env.NamespaceID()}
	initial := make(chan *historyservice.RecordActivityTaskStartedRequest, 1)
	var firstStart, oldHistory atomic.Bool
	s.T().Cleanup(faults.RegisterRequestCallback(scope, func(ctx context.Context, _ string, req any) *grpcfaults.Outcome {
		switch r := req.(type) {
		case *historyservice.RecordActivityTaskStartedRequest:
			if r.GetWorkflowExecution().GetWorkflowId() == workflowID && firstStart.CompareAndSwap(false, true) {
				initial <- r
				<-ctx.Done() // The original start never reached the handler.
				return &grpcfaults.Outcome{Error: ctx.Err()}
			}
		case *historyservice.RecoverActivityTaskStartedRequest:
			if r.GetStartRequest().GetWorkflowExecution().GetWorkflowId() == workflowID && oldHistory.CompareAndSwap(false, true) {
				return &grpcfaults.Outcome{Error: serviceerror.NewUnimplemented("old History server")}
			}
		default:
			return nil
		}
		return nil
	}))
	type recorded struct {
		request  *historyservice.RecoverActivityTaskStartedRequest
		response *historyservice.RecordActivityTaskStartedResponse
	}
	starts := make(chan recorded, 2)
	var replies atomic.Int32
	env.InjectResponseFault(func(req, resp any, handlerErr error) error {
		r, ok := req.(*historyservice.RecoverActivityTaskStartedRequest)
		if !ok || handlerErr != nil || r.GetStartRequest().GetWorkflowExecution().GetWorkflowId() != workflowID {
			return nil
		}
		n := replies.Add(1)
		if n <= 2 {
			starts <- recorded{r, resp.(*historyservice.RecoverActivityTaskStartedResponse).GetStartResponse()}
		}
		if n == 1 {
			return serviceerror.NewUnavailable("lost committed recovery response")
		}
		return nil
	})
	controlQueue := env.Tv().ControlQueueName(env.Namespace().String())
	poll := s.pollActivity(env, env.Tv().WorkerInstanceKey(), controlQueue)
	run := s.startActivity(env, workflowID)
	task, err := poll.Get(s.Context())
	s.NoError(err)
	original := await.Rcv(s.T(), initial)
	first := await.Rcv(s.T(), starts)
	retry := await.Rcv(s.T(), starts)
	s.True(oldHistory.Load())
	s.Equal(original.GetRequestId(), first.request.GetWithheldStartRequestId())
	s.NotEqual(original.GetRequestId(), first.request.GetStartRequest().GetRequestId())
	s.ProtoEqual(original.GetActivityTransferInfo(), first.request.GetStartRequest().GetActivityTransferInfo())
	s.ProtoEqual(first.request, retry.request)
	s.ProtoEqual(first.response, retry.response)
	s.Equal(env.Namespace().String(), retry.response.GetWorkflowNamespace())
	s.NotEmpty(retry.response.GetWorkflowType().GetName())
	s.Equal(int32(1), task.GetAttempt())
	// Both orderings take the same History lease. An original start arriving
	// after recovery, or a row reread without the receipt, cannot adopt it.
	history := env.GetTestCluster().HistoryClient()
	var duplicate *serviceerrors.TaskAlreadyStarted
	_, err = history.RecordActivityTaskStarted(s.Context(), original)
	s.ErrorAs(err, &duplicate)
	reread := common.CloneProto(first.request)
	reread.StartRequest.RequestId = uuid.NewString()
	reread.WithheldStartRequestId = ""
	_, err = history.RecoverActivityTaskStarted(s.Context(), reread)
	s.ErrorAs(err, &duplicate)
	if cancelActivity {
		_, err := env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(), WorkflowExecution: task.WorkflowExecution, Reason: "verify the recovered cancellation token",
		})
		s.NoError(err)
		control, err := env.FrontendClient().PollNexusTaskQueue(s.Context(), &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(), Identity: env.Tv().WorkerInstanceKey(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueue, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
		})
		s.NoError(err)
		var commands workerservicepb.ExecuteCommandsRequest
		s.NoError(payload.Decode(control.GetRequest().GetStartOperation().GetPayload(), &commands))
		s.Len(commands.Commands, 1)
		s.Equal(task.TaskToken, commands.Commands[0].GetCancelActivity().GetTaskToken())
		return
	}
	_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(), TaskToken: task.GetTaskToken(),
	})
	s.NoError(err)
	s.NoError(run.Get(s.Context(), nil))
	s.requireSingleCompletedActivity(env, run)
}

func (s *activityTransferSuite) newEnv(fair bool, opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append(opts,
		testcore.WithDynamicConfig(dynamicconfig.EnableActivityTransferRecovery, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnableFairness, fair),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)
	env := testcore.NewEnv(s.T(), opts...)
	env.SdkWorker().RegisterWorkflow(wfaSingleActivityWorkflow)
	return env
}

func (s *activityTransferSuite) startActivity(env *testcore.TestEnv, workflowID string) sdkclient.WorkflowRun {
	s.T().Helper()
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: env.WorkerTaskQueue(),
	}, wfaSingleActivityWorkflow, wfaActivityParams{
		Cfg:        activityConfig{MaxAttempts: 1, StartToClose: 12 * time.Second, ScheduleToClose: 30 * time.Second},
		ActivityTQ: env.Tv().TaskQueue().Name,
		ActivityID: env.Tv().ActivityID(),
	})
	s.NoError(err)
	return run
}

func (s *activityTransferSuite) waitForPoller(env *testcore.TestEnv, identity string) {
	s.T().Helper()
	s.Await(func(s *activityTransferSuite) {
		resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     env.Tv().TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		})
		s.NoError(err)
		var identities []string
		for _, poller := range resp.GetPollers() {
			identities = append(identities, poller.GetIdentity())
		}
		s.Contains(identities, identity)
	}, 5*time.Second, 25*time.Millisecond)
}

func (s *activityTransferSuite) requireSingleCompletedActivity(env *testcore.TestEnv, run sdkclient.WorkflowRun) {
	s.T().Helper()
	events := env.RequireHistory(s, &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()})
	s.RequireHistoryEvent(events, enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED)
	s.RequireHistoryEvent(events, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
}
