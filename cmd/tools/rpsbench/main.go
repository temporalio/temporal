package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type counters struct {
	success uint64
	failed  uint64
	rpc     uint64
	started uint64
}

func main() {
	target := flag.String("target", "127.0.0.1:7233", "Temporal frontend address")
	namespace := flag.String("namespace", "bench", "Temporal namespace")
	taskQueue := flag.String("task-queue", "bench-task-queue", "Task queue")
	mode := flag.String("mode", "start", "Benchmark mode: start, signal, history, complete, e2e, e2e-parallel")
	pollerGroups := flag.Bool("poller-groups", false, "Reuse server-advertised poller group IDs for e2e/e2e-parallel polls")
	duration := flag.Duration("duration", 10*time.Second, "Benchmark duration")
	concurrency := flag.Int("concurrency", 16, "Concurrent callers")
	starters := flag.Int("starters", 0, "Starter goroutines for e2e-parallel; defaults to concurrency")
	pollers := flag.Int("pollers", 0, "Poller/completer goroutines for e2e-parallel; defaults to concurrency")
	maxOutstanding := flag.Int("max-outstanding", 0, "Max started but uncompleted workflows for e2e-parallel; defaults to 4*concurrency")
	pool := flag.Int("pool", 1024, "Workflow pool size for signal/history modes")
	timeout := flag.Duration("timeout", 5*time.Second, "Per-RPC timeout")
	warmup := flag.Duration("warmup", 0, "Warmup duration")
	flag.Parse()

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = conn.Close()
	}()

	client := workflowservice.NewWorkflowServiceClient(conn)
	if err := ensureNamespace(context.Background(), client, *namespace); err != nil {
		fmt.Fprintf(os.Stderr, "register namespace: %v\n", err)
		os.Exit(1)
	}

	workflows, err := setupMode(context.Background(), client, *namespace, *taskQueue, *mode, *pool, *concurrency, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup %s: %v\n", *mode, err)
		os.Exit(1)
	}

	if *warmup > 0 {
		if *mode == "e2e-parallel" {
			runParallelE2E(client, *namespace, *taskQueue, *pollerGroups, *warmup, *concurrency, *starters, *pollers, *maxOutstanding, *timeout)
		} else {
			run(client, *namespace, *taskQueue, *mode, workflows, *pollerGroups, *warmup, *concurrency, *timeout)
		}
	}

	var benchResult result
	if *mode == "e2e-parallel" {
		benchResult = runParallelE2E(client, *namespace, *taskQueue, *pollerGroups, *duration, *concurrency, *starters, *pollers, *maxOutstanding, *timeout)
	} else {
		benchResult = run(client, *namespace, *taskQueue, *mode, workflows, *pollerGroups, *duration, *concurrency, *timeout)
	}
	elapsed := benchResult.elapsed.Seconds()
	success := atomic.LoadUint64(&benchResult.counts.success)
	failed := atomic.LoadUint64(&benchResult.counts.failed)
	rpc := atomic.LoadUint64(&benchResult.counts.rpc)
	started := atomic.LoadUint64(&benchResult.counts.started)
	fmt.Printf("mode=%s duration=%s concurrency=%d success=%d failed=%d rpcs=%d rps=%.2f rpc_rps=%.2f",
		*mode, benchResult.elapsed.Round(time.Millisecond), *concurrency, success, failed, rpc, float64(success)/elapsed, float64(rpc)/elapsed)
	if started != 0 {
		fmt.Printf(" starts=%d start_rps=%.2f", started, float64(started)/elapsed)
	}
	if benchResult.firstErr != "" {
		fmt.Printf(" first_error=%q", benchResult.firstErr)
	}
	if benchResult.phases.count() != 0 {
		fmt.Print(benchResult.phases.format())
	}
	fmt.Println()
}

type result struct {
	counts   counters
	elapsed  time.Duration
	firstErr string
	phases   *phaseTimings
}

type e2eTimings struct {
	start           time.Duration
	poll            time.Duration
	scheduleToStart time.Duration
	complete        time.Duration
}

type phaseTimings struct {
	start           durationSamples
	poll            durationSamples
	scheduleToStart durationSamples
	complete        durationSamples
}

type durationSamples struct {
	mu     sync.Mutex
	values []float64
}

func (s *durationSamples) add(d time.Duration) {
	s.mu.Lock()
	s.values = append(s.values, float64(d)/float64(time.Millisecond))
	s.mu.Unlock()
}

func (s *durationSamples) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.values)
}

func (s *durationSamples) snapshot() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]float64, len(s.values))
	copy(cp, s.values)
	return cp
}

func (p *phaseTimings) add(t e2eTimings) {
	if t.start != 0 {
		p.start.add(t.start)
	}
	p.poll.add(t.poll)
	if t.scheduleToStart != 0 {
		p.scheduleToStart.add(t.scheduleToStart)
	}
	p.complete.add(t.complete)
}

func (p *phaseTimings) count() int {
	return p.start.count() + p.poll.count() + p.scheduleToStart.count() + p.complete.count()
}

func (p *phaseTimings) format() string {
	var b strings.Builder
	if p.start.count() != 0 {
		fmt.Fprintf(&b, " start_p50_ms=%.2f start_p95_ms=%.2f start_p99_ms=%.2f",
			percentile(p.start.snapshot(), 0.50),
			percentile(p.start.snapshot(), 0.95),
			percentile(p.start.snapshot(), 0.99),
		)
	}
	if p.poll.count() != 0 {
		fmt.Fprintf(&b, " poll_p50_ms=%.2f poll_p95_ms=%.2f poll_p99_ms=%.2f",
			percentile(p.poll.snapshot(), 0.50),
			percentile(p.poll.snapshot(), 0.95),
			percentile(p.poll.snapshot(), 0.99),
		)
	}
	if p.scheduleToStart.count() != 0 {
		fmt.Fprintf(&b, " wft_s2s_p50_ms=%.2f wft_s2s_p95_ms=%.2f wft_s2s_p99_ms=%.2f",
			percentile(p.scheduleToStart.snapshot(), 0.50),
			percentile(p.scheduleToStart.snapshot(), 0.95),
			percentile(p.scheduleToStart.snapshot(), 0.99),
		)
	}
	if p.complete.count() != 0 {
		fmt.Fprintf(&b, " complete_p50_ms=%.2f complete_p95_ms=%.2f complete_p99_ms=%.2f",
			percentile(p.complete.snapshot(), 0.50),
			percentile(p.complete.snapshot(), 0.95),
			percentile(p.complete.snapshot(), 0.99),
		)
	}
	return b.String()
}

func ensureNamespace(ctx context.Context, client workflowservice.WorkflowServiceClient, namespace string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := client.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	if status.Code(err) == codes.AlreadyExists {
		err = nil
	}
	if err != nil {
		return err
	}
	for {
		_, err = client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func setupMode(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	mode string,
	pool int,
	concurrency int,
	timeout time.Duration,
) ([]string, error) {
	switch mode {
	case "start", "e2e", "e2e-parallel":
		return nil, nil
	case "signal", "history", "complete":
	default:
		return nil, fmt.Errorf("unknown mode %q", mode)
	}
	if pool < concurrency {
		pool = concurrency
	}
	ids := make([]string, pool)
	prefix := fmt.Sprintf("bench-%s-%d", mode, time.Now().UnixNano())
	var next atomic.Uint64
	var failed atomic.Uint64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	workers := concurrency
	if workers > 64 {
		workers = 64
	}
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			for {
				idx := int(next.Add(1)) - 1
				if idx >= pool {
					return
				}
				id := fmt.Sprintf("%s-%d", prefix, idx)
				err := startWorkflow(client, namespace, taskQueue, id, requestID(fmt.Sprintf("%s-request-%d", prefix, idx)), timeout, time.Minute)
				if err != nil {
					failed.Add(1)
					if firstErr.Load() == nil {
						firstErr.Store(err.Error())
					}
					continue
				}
				ids[idx] = id
			}
		})
	}
	wg.Wait()
	if failed.Load() != 0 {
		return nil, fmt.Errorf("%d setup starts failed; first error: %s", failed.Load(), firstErr.Load())
	}
	return ids, nil
}

func run(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	mode string,
	workflows []string,
	usePollerGroups bool,
	duration time.Duration,
	concurrency int,
	timeout time.Duration,
) result {
	deadline := time.Now().Add(duration)
	started := time.Now()
	var wg sync.WaitGroup
	var seq atomic.Uint64
	var counts counters
	var firstErr atomic.Value
	var phases phaseTimings

	for i := 0; i < concurrency; i++ {
		worker := i
		wg.Go(func() {
			pollerGroupID := ""
			for {
				if time.Now().After(deadline) {
					return
				}
				id := seq.Add(1)
				rpcs, nextPollerGroupID, timings, err := runIteration(
					client,
					namespace,
					taskQueue,
					mode,
					workflows,
					usePollerGroups,
					started,
					worker,
					id,
					pollerGroupID,
					timeout,
				)
				if err != nil {
					atomic.AddUint64(&counts.failed, 1)
					if firstErr.Load() == nil {
						firstErr.Store(err.Error())
					}
					continue
				}
				if timings.poll != 0 {
					phases.add(timings)
					pollerGroupID = nextPollerGroupID
				}
				atomic.AddUint64(&counts.success, 1)
				atomic.AddUint64(&counts.rpc, uint64(rpcs))
			}
		})
	}

	wg.Wait()
	return result{counts: counts, elapsed: time.Since(started), firstErr: loadFirstErr(&firstErr), phases: &phases}
}

func runIteration(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	mode string,
	workflows []string,
	usePollerGroups bool,
	started time.Time,
	worker int,
	id uint64,
	pollerGroupID string,
	timeout time.Duration,
) (int, string, e2eTimings, error) {
	requestIDSeed := fmt.Sprintf("request-%d-%d-%d", started.UnixNano(), worker, id)
	switch mode {
	case "start":
		return 1, pollerGroupID, e2eTimings{}, startWorkflow(
			client,
			namespace,
			taskQueue,
			fmt.Sprintf("bench-%d-%d-%d", started.UnixNano(), worker, id),
			requestID(requestIDSeed),
			timeout,
			time.Minute,
		)
	case "signal":
		return 1, pollerGroupID, e2eTimings{}, signalWorkflow(
			client,
			namespace,
			workflows[int(id)%len(workflows)],
			requestID(requestIDSeed),
			timeout,
		)
	case "history":
		return 1, pollerGroupID, e2eTimings{}, getHistory(
			client,
			namespace,
			workflows[int(id)%len(workflows)],
			timeout,
		)
	case "e2e":
		timings, nextPollerGroupID, err := runE2E(
			client,
			namespace,
			taskQueue,
			fmt.Sprintf("bench-e2e-%d-%d-%d", started.UnixNano(), worker, id),
			requestID(fmt.Sprintf("request-e2e-%d-%d-%d", started.UnixNano(), worker, id)),
			pollerGroupID,
			usePollerGroups,
			worker,
			timeout,
		)
		return 3, nextPollerGroupID, timings, err
	case "complete":
		timings, nextPollerGroupID, err := completeWorkflowTask(
			client,
			namespace,
			taskQueue,
			pollerGroupID,
			usePollerGroups,
			worker,
			timeout,
		)
		return 2, nextPollerGroupID, timings, err
	default:
		return 0, pollerGroupID, e2eTimings{}, fmt.Errorf("unknown mode %q", mode)
	}
}

func runParallelE2E(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	usePollerGroups bool,
	duration time.Duration,
	concurrency int,
	starters int,
	pollers int,
	maxOutstanding int,
	timeout time.Duration,
) result {
	if starters <= 0 {
		starters = concurrency
	}
	if pollers <= 0 {
		pollers = concurrency
	}
	if maxOutstanding <= 0 {
		maxOutstanding = 4 * concurrency
	}
	if maxOutstanding < starters {
		maxOutstanding = starters
	}

	startedAt := time.Now()
	deadline := startedAt.Add(duration)
	runCtx, stopStarters := context.WithDeadline(context.Background(), deadline)
	defer stopStarters()
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	outstanding := make(chan struct{}, maxOutstanding)
	var counts counters
	var phases phaseTimings
	var firstErr atomic.Value
	var seq atomic.Uint64
	var starterWG sync.WaitGroup
	var workerWG sync.WaitGroup

	recordErr := func(err error) {
		if err == nil {
			return
		}
		atomic.AddUint64(&counts.failed, 1)
		if firstErr.Load() == nil {
			firstErr.Store(err.Error())
		}
	}

	for i := 0; i < pollers; i++ {
		worker := i
		workerWG.Go(func() {
			runPoller(
				workerCtx,
				client,
				namespace,
				taskQueue,
				usePollerGroups,
				worker,
				timeout,
				deadline,
				outstanding,
				recordErr,
				&counts,
				&phases,
			)
		})
	}

	for i := 0; i < starters; i++ {
		worker := i
		starterWG.Go(func() {
			runStarter(
				runCtx,
				client,
				namespace,
				taskQueue,
				worker,
				timeout,
				startedAt,
				deadline,
				outstanding,
				&seq,
				recordErr,
				&counts,
				&phases,
			)
		})
	}

	starterWG.Wait()
	drainDeadline := time.Now().Add(timeout)
	for len(outstanding) != 0 && time.Now().Before(drainDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	stopWorkers()
	workerWG.Wait()

	return result{counts: counts, elapsed: duration, firstErr: loadFirstErr(&firstErr), phases: &phases}
}

func runPoller(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	usePollerGroups bool,
	worker int,
	timeout time.Duration,
	deadline time.Time,
	outstanding chan struct{},
	recordErr func(error),
	counts *counters,
	phases *phaseTimings,
) {
	pollerGroupID := ""
	for {
		if ctx.Err() != nil {
			return
		}
		timings, nextPollerGroupID, err := completeWorkflowTaskWithContext(
			ctx,
			client,
			namespace,
			taskQueue,
			pollerGroupID,
			usePollerGroups,
			worker,
			timeout,
		)
		if err != nil {
			if ctx.Err() != nil || status.Code(err) == codes.Canceled {
				return
			}
			if timings.complete != 0 {
				releaseOutstanding(outstanding)
			}
			if time.Now().Before(deadline) {
				recordErr(err)
			}
			continue
		}
		releaseOutstanding(outstanding)
		pollerGroupID = nextPollerGroupID
		if time.Now().Before(deadline) {
			phases.add(timings)
			atomic.AddUint64(&counts.success, 1)
			atomic.AddUint64(&counts.rpc, 2)
		}
	}
}

func runStarter(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	worker int,
	timeout time.Duration,
	startedAt time.Time,
	deadline time.Time,
	outstanding chan struct{},
	seq *atomic.Uint64,
	recordErr func(error),
	counts *counters,
	phases *phaseTimings,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case outstanding <- struct{}{}:
		}

		id := seq.Add(1)
		workflowID := fmt.Sprintf("bench-e2e-parallel-%d-%d-%d", startedAt.UnixNano(), worker, id)
		reqID := requestID(fmt.Sprintf("request-e2e-parallel-%d-%d-%d", startedAt.UnixNano(), worker, id))
		opStarted := time.Now()
		err := startWorkflow(client, namespace, taskQueue, workflowID, reqID, timeout, 10*time.Second)
		if err != nil {
			releaseOutstanding(outstanding)
			if time.Now().Before(deadline) {
				atomic.AddUint64(&counts.rpc, 1)
				recordErr(err)
			}
			continue
		}
		if time.Now().Before(deadline) {
			phases.start.add(time.Since(opStarted))
			atomic.AddUint64(&counts.started, 1)
			atomic.AddUint64(&counts.rpc, 1)
		}
	}
}

func startWorkflow(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	workflowID string,
	requestID string,
	timeout time.Duration,
	workflowTaskTimeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                namespace,
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: "BenchWorkflow"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowExecutionTimeout: durationpb.New(time.Hour),
		WorkflowRunTimeout:       durationpb.New(time.Hour),
		WorkflowTaskTimeout:      durationpb.New(workflowTaskTimeout),
		Identity:                 "temporal-rpsbench",
		RequestId:                requestID,
	})
	return err
}

func signalWorkflow(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	workflowID string,
	requestID string,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		SignalName:        "bench-signal",
		Identity:          "temporal-rpsbench",
		RequestId:         requestID,
	})
	return err
}

func getHistory(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	workflowID string,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              namespace,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: workflowID},
		MaximumPageSize:        10,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		WaitNewEvent:           false,
		SkipArchival:           true,
	})
	return err
}

func runE2E(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	workflowID string,
	requestID string,
	pollerGroupID string,
	usePollerGroups bool,
	worker int,
	timeout time.Duration,
) (e2eTimings, string, error) {
	var timings e2eTimings
	started := time.Now()
	if err := startWorkflow(client, namespace, taskQueue, workflowID, requestID, timeout, 10*time.Second); err != nil {
		timings.start = time.Since(started)
		return timings, pollerGroupID, err
	}
	timings.start = time.Since(started)

	workerTimings, pollerGroupID, err := completeWorkflowTask(client, namespace, taskQueue, pollerGroupID, usePollerGroups, worker, timeout)
	timings.poll = workerTimings.poll
	timings.complete = workerTimings.complete
	return timings, pollerGroupID, err
}

func completeWorkflowTask(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	pollerGroupID string,
	usePollerGroups bool,
	worker int,
	timeout time.Duration,
) (e2eTimings, string, error) {
	return completeWorkflowTaskWithContext(context.Background(), client, namespace, taskQueue, pollerGroupID, usePollerGroups, worker, timeout)
}

func completeWorkflowTaskWithContext(
	parent context.Context,
	client workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueue string,
	pollerGroupID string,
	usePollerGroups bool,
	worker int,
	timeout time.Duration,
) (e2eTimings, string, error) {
	var timings e2eTimings
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, timeout)
	pollResp, err := client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:     namespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		PollerGroupId: pollerGroupID,
		Identity:      "temporal-rpsbench",
	})
	cancel()
	if err != nil {
		timings.poll = time.Since(started)
		return timings, pollerGroupID, err
	}
	timings.poll = time.Since(started)
	if len(pollResp.GetTaskToken()) == 0 {
		return timings, pollerGroupID, errors.New("empty task token")
	}
	if scheduled, started := pollResp.GetScheduledTime(), pollResp.GetStartedTime(); scheduled != nil && started != nil {
		timings.scheduleToStart = started.AsTime().Sub(scheduled.AsTime())
	}
	if usePollerGroups {
		pollerGroupID = choosePollerGroupID(pollResp, pollerGroupID, worker)
	}

	started = time.Now()
	ctx, cancel = context.WithTimeout(parent, timeout)
	_, err = client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:  namespace,
		Identity:   "temporal-rpsbench",
		TaskToken:  pollResp.GetTaskToken(),
		ResourceId: pollResp.GetWorkflowExecution().GetWorkflowId(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	cancel()
	timings.complete = time.Since(started)
	return timings, pollerGroupID, err
}

func sanitize(s string) string {
	return strings.ReplaceAll(s, "\n", " ")
}

func loadFirstErr(firstErr *atomic.Value) string {
	v, ok := firstErr.Load().(string)
	if !ok {
		return ""
	}
	return sanitize(v)
}

func requestID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	sum[6] = (sum[6] & 0x0f) | 0x40
	sum[8] = (sum[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

func choosePollerGroupID(resp *workflowservice.PollWorkflowTaskQueueResponse, current string, worker int) string {
	groups := resp.GetPollerGroupsInfo().GetPollerGroups()
	if len(groups) == 0 {
		//nolint:staticcheck // Older servers only return the deprecated field.
		groups = resp.GetPollerGroupInfos()
	}
	if len(groups) == 0 {
		return current
	}
	group := groups[worker%len(groups)].GetId()
	if group == "" {
		return current
	}
	return group
}

func releaseOutstanding(outstanding chan struct{}) {
	select {
	case <-outstanding:
	default:
	}
}

func percentile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	idx := int(float64(len(values)-1) * q)
	return values[idx]
}
