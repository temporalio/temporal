package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	serverlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/service/localexecution"
	temporalite "go.temporal.io/server/temporaltest/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	demoNamespace    = "local-first-demo"
	demoWorkflowType = "local-first-core-loop"
	demoWorkflowID   = "local-first-core-steel-thread"
	demoActivityType = "local-first-core-activity"
	demoTaskQueue    = "local-first-core-steel-thread"
	modeUpstream     = "upstream"
	modeBridge       = "bridge"
)

type options struct {
	mode             string
	stateDirectory   string
	syncInterval     time.Duration
	upstreamAddress  string
	namespace        string
	workflowID       string
	runID            string
	workflowType     string
	activityType     string
	taskQueue        string
	iterations       int
	localServerID    string
	failSyncAttempts int
}

type readyMessage struct {
	UpstreamAddress string `json:"upstream_address"`
	LocalAddress    string `json:"local_address"`
	Namespace       string `json:"namespace"`
	WorkflowID      string `json:"workflow_id"`
	RunID           string `json:"run_id"`
	WorkflowType    string `json:"workflow_type"`
	ActivityType    string `json:"activity_type"`
	TaskQueue       string `json:"task_queue"`
	Iterations      int    `json:"iterations"`
}

func main() {
	opts := options{}
	flag.StringVar(&opts.mode, "mode", modeBridge, "server mode: upstream or bridge")
	flag.StringVar(&opts.stateDirectory, "state-dir", "", "directory for bridge SQLite state")
	flag.DurationVar(&opts.syncInterval, "sync-interval", time.Second, "interval between upstream history syncs")
	flag.StringVar(&opts.upstreamAddress, "upstream-address", "", "upstream Temporal frontend address")
	flag.StringVar(&opts.namespace, "namespace", demoNamespace, "workflow namespace")
	flag.StringVar(&opts.workflowID, "workflow-id", demoWorkflowID, "workflow ID to bridge")
	flag.StringVar(&opts.runID, "run-id", "", "workflow run ID to bridge")
	flag.StringVar(&opts.workflowType, "workflow-type", demoWorkflowType, "workflow type advertised to Core")
	flag.StringVar(&opts.activityType, "activity-type", demoActivityType, "Activity type advertised to Core")
	flag.StringVar(&opts.taskQueue, "task-queue", demoTaskQueue, "task queue used by the workflow")
	flag.IntVar(&opts.iterations, "iterations", 3, "number of Activities the Core workflow driver will execute")
	flag.StringVar(&opts.localServerID, "local-server-id", "local-first-core-steel-thread", "stable local bridge identifier")
	flag.IntVar(&opts.failSyncAttempts, "fail-sync-attempts", 0, "number of synchronization attempts to fail for testing")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.TODO(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opts); err != nil {
		log.Printf("local-first demo server failed: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
	switch opts.mode {
	case modeUpstream:
		return runUpstream(ctx, opts)
	case modeBridge:
		return runBridge(ctx, opts)
	default:
		return fmt.Errorf("unsupported --mode %q", opts.mode)
	}
}

func runUpstream(ctx context.Context, opts options) error {
	if err := validateCommonOptions(opts); err != nil {
		return err
	}

	upstream, err := startServer("", true, opts.namespace, true)
	if err != nil {
		return fmt.Errorf("start upstream server: %w", err)
	}
	defer stopServer("upstream", upstream)

	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()
	upstreamClient, err := upstream.NewClient(startupCtx, opts.namespace)
	if err != nil {
		return fmt.Errorf("connect upstream client: %w", err)
	}
	defer upstreamClient.Close()
	if err := waitForHistory(startupCtx, upstreamClient); err != nil {
		return fmt.Errorf("wait for upstream history service: %w", err)
	}

	run, err := upstreamClient.ExecuteWorkflow(
		startupCtx,
		client.StartWorkflowOptions{
			ID:                  opts.workflowID,
			TaskQueue:           opts.taskQueue,
			WorkflowTaskTimeout: time.Minute,
		},
		opts.workflowType,
		opts.iterations,
	)
	if err != nil {
		return fmt.Errorf("start upstream workflow: %w", err)
	}
	if err := writeReady(readyMessage{
		UpstreamAddress: upstream.FrontendHostPort(),
		Namespace:       opts.namespace,
		WorkflowID:      run.GetID(),
		RunID:           run.GetRunID(),
		WorkflowType:    opts.workflowType,
		ActivityType:    opts.activityType,
		TaskQueue:       opts.taskQueue,
		Iterations:      opts.iterations,
	}); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func runBridge(ctx context.Context, opts options) error {
	if err := validateBridgeOptions(opts); err != nil {
		return err
	}
	if err := os.MkdirAll(opts.stateDirectory, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	local, err := startServer(filepath.Join(opts.stateDirectory, "local.sqlite"), false, opts.namespace, false)
	if err != nil {
		return fmt.Errorf("start local server: %w", err)
	}
	defer stopServer("local", local)

	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()
	upstreamClient, err := client.DialContext(startupCtx, client.Options{
		HostPort:  opts.upstreamAddress,
		Namespace: opts.namespace,
	})
	if err != nil {
		return fmt.Errorf("connect upstream client: %w", err)
	}
	defer upstreamClient.Close()
	localClient, err := local.NewClient(startupCtx, opts.namespace)
	if err != nil {
		return fmt.Errorf("connect local client: %w", err)
	}
	defer localClient.Close()
	if err := waitForHistory(startupCtx, upstreamClient); err != nil {
		return fmt.Errorf("wait for upstream history service: %w", err)
	}
	if err := waitForHistory(startupCtx, localClient); err != nil {
		return fmt.Errorf("wait for local history service: %w", err)
	}

	acquisition, err := acquireLocalExecution(startupCtx, upstreamClient, opts)
	if err != nil {
		return err
	}
	execution := acquisition.GetWorkflowExecution()
	upstreamAdmin, upstreamNamespaceID, closeUpstreamAdmin, err := adminClient(
		startupCtx,
		opts.upstreamAddress,
		opts.namespace,
	)
	if err != nil {
		return fmt.Errorf("connect upstream admin client: %w", err)
	}
	defer closeUpstreamAdmin()
	localAdmin, localNamespaceID, closeLocalAdmin, err := adminClient(
		startupCtx,
		local.FrontendHostPort(),
		opts.namespace,
	)
	if err != nil {
		return fmt.Errorf("connect local admin client: %w", err)
	}
	defer closeLocalAdmin()

	baselineImporter, err := localexecution.NewBaselineImporter(
		localexecution.HistoryEndpoint{
			NamespaceID: upstreamNamespaceID,
			AdminClient: upstreamAdmin,
		},
		localexecution.HistoryEndpoint{
			Namespace:   opts.namespace,
			AdminClient: localAdmin,
		},
	)
	if err != nil {
		return fmt.Errorf("configure baseline import: %w", err)
	}
	baseline, err := baselineImporter.Import(startupCtx, execution)
	if err != nil {
		return fmt.Errorf("import baseline: %w", err)
	}
	if baseline.LastEventID != acquisition.GetLocalExecutionInfo().GetLastSynchronizedEventId() ||
		baseline.LastEventVersion != acquisition.GetLocalExecutionInfo().GetLastSynchronizedEventVersion() {
		return fmt.Errorf(
			"imported baseline cursor %d/%d does not match acquired cursor %d/%d",
			baseline.LastEventID,
			baseline.LastEventVersion,
			acquisition.GetLocalExecutionInfo().GetLastSynchronizedEventId(),
			acquisition.GetLocalExecutionInfo().GetLastSynchronizedEventVersion(),
		)
	}
	stateController, err := localexecution.NewExecutionStateController(
		opts.namespace,
		opts.localServerID,
		acquisition.GetLocalExecutionInfo().GetFencingEpoch(),
		localAdmin,
	)
	if err != nil {
		return fmt.Errorf("configure local execution state: %w", err)
	}

	syncTargetAdmin := upstreamAdmin
	if opts.failSyncAttempts > 0 {
		faults := &faultInjectingAdminClient{AdminServiceClient: upstreamAdmin}
		faults.remaining.Store(int64(opts.failSyncAttempts))
		syncTargetAdmin = faults
	}

	replicator, err := localexecution.NewHistoryReplicator(
		localexecution.HistoryEndpoint{
			NamespaceID: localNamespaceID,
			AdminClient: localAdmin,
		},
		localexecution.ReplicationTarget{
			Namespace:      opts.namespace,
			LocalServerID:  opts.localServerID,
			OwnershipToken: acquisition.GetLocalExecutionInfo().GetOwnershipToken(),
			FencingEpoch:   acquisition.GetLocalExecutionInfo().GetFencingEpoch(),
			AdminClient:    syncTargetAdmin,
		},
		localexecution.SyncCursor{
			EventID: baseline.LastEventID,
			Version: baseline.LastEventVersion,
		},
	)
	if err != nil {
		return fmt.Errorf("configure history synchronization: %w", err)
	}

	syncErrors := startHistorySynchronization(
		ctx,
		acquisition,
		replicator,
		stateController,
		opts.syncInterval,
	)

	if err := writeReady(readyMessage{
		UpstreamAddress: opts.upstreamAddress,
		LocalAddress:    local.FrontendHostPort(),
		Namespace:       opts.namespace,
		WorkflowID:      opts.workflowID,
		RunID:           opts.runID,
		WorkflowType:    opts.workflowType,
		ActivityType:    opts.activityType,
		TaskQueue:       opts.taskQueue,
		Iterations:      opts.iterations,
	}); err != nil {
		return err
	}

	return waitForBridge(ctx, syncErrors)
}

func startHistorySynchronization(
	ctx context.Context,
	acquisition *workflowservice.PollWorkflowTaskQueueResponse,
	replicator *localexecution.HistoryReplicator,
	stateController *localexecution.ExecutionStateController,
	interval time.Duration,
) <-chan error {
	syncErrors := make(chan error, 1)
	go func() {
		leaseExpiration := acquisition.GetLocalExecutionInfo().GetLeaseExpirationTime()
		if leaseExpiration == nil || leaseExpiration.CheckValid() != nil {
			syncErrors <- errors.New("acquisition returned an invalid lease expiration")
			return
		}
		syncErrors <- replicator.RunControlled(ctx, acquisition.GetWorkflowExecution(), localexecution.SynchronizationLoopOptions{
			Interval:        interval,
			LeaseExpiration: leaseExpiration.AsTime(),
			StateController: stateController,
		}, logSynchronizationResult)
	}()
	return syncErrors
}

func logSynchronizationResult(result localexecution.SyncResult) {
	if result.Released {
		log.Printf("synchronized %d batches through event %d and released ownership", result.HistoryBatches, result.LastEventID)
		return
	}
	log.Printf("synchronized %d batches through event %d", result.HistoryBatches, result.LastEventID)
}

func waitForBridge(ctx context.Context, syncErrors <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-syncErrors:
		if errors.Is(err, localexecution.ErrLocalExecutionOwnershipLost) {
			log.Printf("%v; local task tokens invalidated", err)
			<-ctx.Done()
			return nil
		}
		if err != nil {
			return fmt.Errorf("history synchronization stopped: %w", err)
		}
		return nil
	}
}

type faultInjectingAdminClient struct {
	adminservice.AdminServiceClient
	remaining atomic.Int64
}

func (c *faultInjectingAdminClient) SyncLocalExecution(
	ctx context.Context,
	request *adminservice.SyncLocalExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.SyncLocalExecutionResponse, error) {
	if c.remaining.Add(-1) >= 0 {
		return nil, serviceerror.NewUnavailable("injected synchronization failure")
	}
	return c.AdminServiceClient.SyncLocalExecution(ctx, request, opts...)
}

func acquireLocalExecution(
	ctx context.Context,
	upstreamClient client.Client,
	opts options,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	acquisition, err := upstreamClient.WorkflowService().PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: opts.namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: opts.taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: opts.localServerID,
			LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
				LocalServerId:          opts.localServerID,
				ProtocolVersion:        localexecution.ProtocolVersion,
				SyncInterval:           durationpb.New(opts.syncInterval),
				RequestedLeaseDuration: durationpb.New(3 * opts.syncInterval),
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("acquire upstream workflow task: %w", err)
	}
	if acquisition.GetLocalExecutionInfo() == nil {
		return nil, fmt.Errorf(
			"upstream did not return local execution ownership (task token bytes: %d, started event: %d, history events: %d)",
			len(acquisition.GetTaskToken()),
			acquisition.GetStartedEventId(),
			len(acquisition.GetHistory().GetEvents()),
		)
	}
	if len(acquisition.GetTaskToken()) != 0 {
		return nil, errors.New("upstream returned a workflow task token for local execution acquisition")
	}
	execution := acquisition.GetWorkflowExecution()
	if execution.GetWorkflowId() != opts.workflowID || execution.GetRunId() != opts.runID {
		return nil, fmt.Errorf(
			"acquired unexpected execution %s/%s",
			execution.GetWorkflowId(),
			execution.GetRunId(),
		)
	}
	return acquisition, nil
}

func validateCommonOptions(opts options) error {
	if opts.namespace == "" {
		return errors.New("--namespace is required")
	}
	if opts.workflowID == "" {
		return errors.New("--workflow-id is required")
	}
	if opts.workflowType == "" {
		return errors.New("--workflow-type is required")
	}
	if opts.activityType == "" {
		return errors.New("--activity-type is required")
	}
	if opts.taskQueue == "" {
		return errors.New("--task-queue is required")
	}
	if opts.iterations <= 0 {
		return errors.New("--iterations must be positive")
	}
	return nil
}

func validateBridgeOptions(opts options) error {
	if err := validateCommonOptions(opts); err != nil {
		return err
	}
	if opts.stateDirectory == "" {
		return errors.New("--state-dir is required")
	}
	if opts.upstreamAddress == "" {
		return errors.New("--upstream-address is required")
	}
	if opts.runID == "" {
		return errors.New("--run-id is required")
	}
	if opts.localServerID == "" {
		return errors.New("--local-server-id is required")
	}
	if opts.syncInterval <= 0 {
		return errors.New("--sync-interval must be positive")
	}
	if opts.failSyncAttempts < 0 {
		return errors.New("--fail-sync-attempts must not be negative")
	}
	return nil
}

func writeReady(message readyMessage) error {
	if err := json.NewEncoder(os.Stdout).Encode(message); err != nil {
		return fmt.Errorf("write readiness message: %w", err)
	}
	return nil
}

func startServer(
	databasePath string,
	ephemeral bool,
	namespace string,
	enableLocalExecution bool,
) (*temporalite.LiteServer, error) {
	dynamicConfig := dynamicconfig.StaticClient{}
	if enableLocalExecution {
		dynamicConfig[dynamicconfig.EnableLocalExecution.Key()] = []dynamicconfig.ConstrainedValue{{Value: true}}
	}
	server, err := temporalite.NewLiteServer(&temporalite.LiteServerConfig{
		Ephemeral:             ephemeral,
		DatabaseFilePath:      databasePath,
		FrontendIP:            "127.0.0.1",
		Namespaces:            []string{namespace},
		Logger:                serverlog.NewNoopLogger(),
		EnableGlobalNamespace: true,
		DynamicConfig:         dynamicConfig,
	})
	if err != nil {
		return nil, err
	}
	if err := server.Start(); err != nil {
		return nil, err
	}
	return server, nil
}

func stopServer(name string, server *temporalite.LiteServer) {
	if err := server.Stop(); err != nil {
		log.Printf("stop %s server: %v", name, err)
	}
}

func waitForHistory(ctx context.Context, temporalClient client.Client) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := temporalClient.DescribeWorkflowExecution(ctx, "history-readiness-probe", "")
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func adminClient(
	ctx context.Context,
	address string,
	namespace string,
) (adminservice.AdminServiceClient, string, func(), error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, "", nil, err
	}
	closeConnection := func() {
		if err := connection.Close(); err != nil {
			log.Printf("close admin connection: %v", err)
		}
	}
	response, err := workflowservice.NewWorkflowServiceClient(connection).DescribeNamespace(
		ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace},
	)
	if err != nil {
		closeConnection()
		return nil, "", nil, err
	}
	return adminservice.NewAdminServiceClient(connection), response.GetNamespaceInfo().GetId(), closeConnection, nil
}
