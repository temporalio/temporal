package scanner

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/worker/scanner/build_ids"
	"go.temporal.io/server/service/worker/scanner/scheduleinvariants"
)

type (
	// Config defines the configuration for scanner
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn

		// PersistenceMaxQPS the max rate of calls to persistence
		PersistenceMaxQPS dynamicconfig.IntPropertyFn
		// Persistence contains the persistence configuration
		Persistence *config.Persistence
		// TaskQueueScannerEnabled indicates if taskQueue scanner should be started as part of scanner
		TaskQueueScannerEnabled dynamicconfig.BoolPropertyFn
		// BuildIdScavengerEnabled indicates if the build ID scavenger should be started as part of scanner
		BuildIdScavengerEnabled dynamicconfig.BoolPropertyFn
		// HistoryScannerEnabled indicates if history scanner should be started as part of scanner
		HistoryScannerEnabled dynamicconfig.BoolPropertyFn
		// ExecutionsScannerEnabled indicates if executions scanner should be started as part of scanner
		ExecutionsScannerEnabled dynamicconfig.BoolPropertyFn
		// HistoryScannerDataMinAge indicates the cleanup threshold of history branch data
		// Only clean up history branches that older than this threshold
		HistoryScannerDataMinAge dynamicconfig.DurationPropertyFn
		// HistoryScannerVerifyRetention indicates if the history scavenger to do retention verification
		HistoryScannerVerifyRetention dynamicconfig.BoolPropertyFn
		// ExecutionScannerPerHostQPS the max rate of calls to scan execution data per host
		ExecutionScannerPerHostQPS dynamicconfig.IntPropertyFn
		// ExecutionScannerPerShardQPS the max rate of calls to scan execution data per shard
		ExecutionScannerPerShardQPS dynamicconfig.IntPropertyFn
		// ExecutionDataDurationBuffer is the data TTL duration buffer of execution data
		ExecutionDataDurationBuffer dynamicconfig.DurationPropertyFn
		// ExecutionScannerWorkerCount is the execution scavenger task worker number
		ExecutionScannerWorkerCount dynamicconfig.IntPropertyFn
		// ExecutionScannerHistoryEventIdValidator indicates if the execution scavenger to validate history event id.
		ExecutionScannerHistoryEventIdValidator dynamicconfig.BoolPropertyFn

		// RemovableBuildIdDurationSinceDefault is the minimum duration since a build ID was last default in its
		// containing set for it to be considered for removal.
		RemovableBuildIdDurationSinceDefault dynamicconfig.DurationPropertyFn
		// BuildIdScavengerVisibilityRPS is the rate limit for visibility calls from the build ID scavenger
		BuildIdScavengerVisibilityRPS dynamicconfig.FloatPropertyFn

		// Schedule-invariants scanners. Each runs as an independent cron workflow; all three
		// require advanced (Elasticsearch) visibility to be configured (otherwise they're skipped).
		ScheduleInvariantsScannerOverdueNextActionTimeEnabled      dynamicconfig.BoolPropertyFn
		ScheduleInvariantsScannerStuckOpenEnabled                  dynamicconfig.BoolPropertyFn
		ScheduleInvariantsScannerUnknownStateEnabled               dynamicconfig.BoolPropertyFn
		ScheduleInvariantsScannerOverdueNextActionTimeTolerance    dynamicconfig.DurationPropertyFn
		ScheduleInvariantsScannerVisibilityRPS                     dynamicconfig.FloatPropertyFn
		ScheduleInvariantsScannerScanInterval                      dynamicconfig.DurationPropertyFn
		ScheduleInvariantsScannerStuckOpenIdleTimeBufferMultiplier dynamicconfig.IntPropertyFn
	}

	// scannerContext is the context object that gets
	// passed around within the scanner workflows / activities
	scannerContext struct {
		cfg                *Config
		logger             log.Logger
		sdkClientFactory   sdk.ClientFactory
		metricsHandler     metrics.Handler
		executionManager   persistence.ExecutionManager
		taskManager        persistence.TaskManager
		visibilityManager  manager.VisibilityManager
		metadataManager    persistence.MetadataManager
		historyClient      historyservice.HistoryServiceClient
		matchingClient     matchingservice.MatchingServiceClient
		adminClient        adminservice.AdminServiceClient
		namespaceRegistry  namespace.Registry
		currentClusterName string
		hostInfo           membership.HostInfo
		serializer         serialization.Serializer
	}

	// Scanner is the background sub-system that does full scans
	// of database tables to cleanup resources, monitor anamolies
	// and emit stats for analytics
	Scanner struct {
		context         scannerContext
		wg              sync.WaitGroup
		lifecycleCancel context.CancelFunc
	}
)

// New returns a new instance of scanner daemon
// Scanner is the background sub-system that does full
// scans of database tables in an attempt to cleanup
// resources, monitor system anamolies and emit stats
// for analysis and alerting
func New(
	logger log.Logger,
	cfg *Config,
	sdkClientFactory sdk.ClientFactory,
	metricsHandler metrics.Handler,
	executionManager persistence.ExecutionManager,
	metadataManager persistence.MetadataManager,
	visibilityManager manager.VisibilityManager,
	taskManager persistence.TaskManager,
	historyClient historyservice.HistoryServiceClient,
	adminClient adminservice.AdminServiceClient,
	matchingClient matchingservice.MatchingServiceClient,
	registry namespace.Registry,
	currentClusterName string,
	hostInfo membership.HostInfo,
	serializer serialization.Serializer,
) *Scanner {
	return &Scanner{
		context: scannerContext{
			cfg:                cfg,
			sdkClientFactory:   sdkClientFactory,
			logger:             logger,
			metricsHandler:     metricsHandler,
			executionManager:   executionManager,
			taskManager:        taskManager,
			visibilityManager:  visibilityManager,
			metadataManager:    metadataManager,
			historyClient:      historyClient,
			matchingClient:     matchingClient,
			adminClient:        adminClient,
			namespaceRegistry:  registry,
			currentClusterName: currentClusterName,
			hostInfo:           hostInfo,
			serializer:         serializer,
		},
	}
}

// Start starts the scanner
func (s *Scanner) Start() error {
	ctx := context.WithValue(context.Background(), scannerContextKey, s.context)
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundHighCallerInfo)
	ctx, s.lifecycleCancel = context.WithCancel(ctx)

	workerOpts := worker.Options{
		Identity:                               "temporal-system@" + s.context.hostInfo.Identity(),
		MaxConcurrentActivityExecutionSize:     s.context.cfg.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: s.context.cfg.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       s.context.cfg.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       s.context.cfg.MaxConcurrentWorkflowTaskPollers(),

		BackgroundActivityContext: ctx,
	}

	var workerTaskQueueNames []string
	if s.context.cfg.Persistence.DefaultStoreType() != config.StoreTypeSQL && s.context.cfg.ExecutionsScannerEnabled() {
		s.wg.Add(1)
		go s.startWorkflowWithRetry(ctx, executionsScannerWFStartOptions, executionsScannerWFTypeName)
		workerTaskQueueNames = append(workerTaskQueueNames, executionsScannerTaskQueueName)
	} else if s.context.cfg.ExecutionsScannerEnabled() {
		s.context.logger.Info("ExecutionsScanner is not supported for SQL store")
	}

	if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeSQL && s.context.cfg.TaskQueueScannerEnabled() {
		s.wg.Add(1)
		go s.startWorkflowWithRetry(ctx, tlScannerWFStartOptions, tqScannerWFTypeName)
		workerTaskQueueNames = append(workerTaskQueueNames, tqScannerTaskQueueName)
	}

	if s.context.cfg.HistoryScannerEnabled() {
		s.wg.Add(1)
		go s.startWorkflowWithRetry(ctx, historyScannerWFStartOptions, historyScannerWFTypeName)
		workerTaskQueueNames = append(workerTaskQueueNames, historyScannerTaskQueueName)
	}

	if s.context.cfg.BuildIdScavengerEnabled() {
		s.wg.Add(1)
		go s.startWorkflowWithRetry(ctx, build_ids.BuildIdScavengerWFStartOptions, build_ids.BuildIdScavangerWorkflowName)

		buildIdsActivities := build_ids.NewActivities(
			s.context.logger,
			s.context.taskManager,
			s.context.metadataManager,
			s.context.visibilityManager,
			s.context.namespaceRegistry,
			s.context.matchingClient,
			s.context.currentClusterName,
			s.context.cfg.RemovableBuildIdDurationSinceDefault,
			s.context.cfg.BuildIdScavengerVisibilityRPS,
		)

		work := s.context.sdkClientFactory.NewWorker(s.context.sdkClientFactory.GetSystemClient(), build_ids.BuildIdScavengerTaskQueueName, workerOpts)
		work.RegisterWorkflowWithOptions(build_ids.BuildIdScavangerWorkflow, workflow.RegisterOptions{Name: build_ids.BuildIdScavangerWorkflowName})
		work.RegisterActivityWithOptions(buildIdsActivities.ScavengeBuildIds, activity.RegisterOptions{Name: build_ids.BuildIdScavangerActivityName})

		// TODO: Nothing is gracefully stopping these workers or listening for fatal errors.
		if err := work.Start(); err != nil {
			return err
		}
	}

	scheduleInvariantsAnyEnabled := s.context.cfg.ScheduleInvariantsScannerOverdueNextActionTimeEnabled() ||
		s.context.cfg.ScheduleInvariantsScannerStuckOpenEnabled() ||
		s.context.cfg.ScheduleInvariantsScannerUnknownStateEnabled()
	if scheduleInvariantsAnyEnabled && !s.context.visibilityManager.HasStoreName(elasticsearch.PersistenceName) {
		s.context.logger.Info("schedule-invariants scanners are enabled but advanced (Elasticsearch) visibility is not configured; skipping")
	} else if scheduleInvariantsAnyEnabled {
		scheduleActivities := scheduleinvariants.NewActivities(
			s.context.logger,
			s.context.metricsHandler,
			s.context.metadataManager,
			s.context.visibilityManager,
			s.context.namespaceRegistry,
			s.context.sdkClientFactory,
			s.context.currentClusterName,
			clock.NewRealTimeSource(),
			s.context.cfg.ScheduleInvariantsScannerVisibilityRPS,
			s.context.cfg.ScheduleInvariantsScannerOverdueNextActionTimeTolerance,
			s.context.cfg.ScheduleInvariantsScannerScanInterval,
			s.context.cfg.ScheduleInvariantsScannerStuckOpenIdleTimeBufferMultiplier,
		)

		if s.context.cfg.ScheduleInvariantsScannerOverdueNextActionTimeEnabled() {
			s.wg.Add(1)
			go s.startWorkflowWithRetry(ctx, scheduleinvariants.OverdueNextActionTimeWFStartOptions, scheduleinvariants.OverdueNextActionTimeWorkflowName)

			work := s.context.sdkClientFactory.NewWorker(s.context.sdkClientFactory.GetSystemClient(), scheduleinvariants.OverdueNextActionTimeTaskQueue, workerOpts)
			work.RegisterWorkflowWithOptions(scheduleinvariants.OverdueNextActionTimeWorkflow, workflow.RegisterOptions{Name: scheduleinvariants.OverdueNextActionTimeWorkflowName})
			work.RegisterActivityWithOptions(scheduleActivities.ScanOverdueNextActionTime, activity.RegisterOptions{Name: scheduleinvariants.OverdueNextActionTimeActivityName})

			// TODO: Nothing is gracefully stopping these workers or listening for fatal errors.
			if err := work.Start(); err != nil {
				return err
			}
		}

		if s.context.cfg.ScheduleInvariantsScannerStuckOpenEnabled() {
			s.wg.Add(1)
			go s.startWorkflowWithRetry(ctx, scheduleinvariants.StuckOpenWFStartOptions, scheduleinvariants.StuckOpenWorkflowName)

			work := s.context.sdkClientFactory.NewWorker(s.context.sdkClientFactory.GetSystemClient(), scheduleinvariants.StuckOpenTaskQueue, workerOpts)
			work.RegisterWorkflowWithOptions(scheduleinvariants.StuckOpenWorkflow, workflow.RegisterOptions{Name: scheduleinvariants.StuckOpenWorkflowName})
			work.RegisterActivityWithOptions(scheduleActivities.ScanStuckOpen, activity.RegisterOptions{Name: scheduleinvariants.StuckOpenActivityName})

			if err := work.Start(); err != nil {
				return err
			}
		}

		if s.context.cfg.ScheduleInvariantsScannerUnknownStateEnabled() {
			s.wg.Add(1)
			go s.startWorkflowWithRetry(ctx, scheduleinvariants.UnknownStateWFStartOptions, scheduleinvariants.UnknownStateWorkflowName)

			work := s.context.sdkClientFactory.NewWorker(s.context.sdkClientFactory.GetSystemClient(), scheduleinvariants.UnknownStateTaskQueue, workerOpts)
			work.RegisterWorkflowWithOptions(scheduleinvariants.UnknownStateWorkflow, workflow.RegisterOptions{Name: scheduleinvariants.UnknownStateWorkflowName})
			work.RegisterActivityWithOptions(scheduleActivities.ScanUnknownState, activity.RegisterOptions{Name: scheduleinvariants.UnknownStateActivityName})

			if err := work.Start(); err != nil {
				return err
			}
		}
	}

	// TODO: There's no reason to register all activities and workflows on every task queue.
	for _, tl := range workerTaskQueueNames {
		work := s.context.sdkClientFactory.NewWorker(s.context.sdkClientFactory.GetSystemClient(), tl, workerOpts)

		work.RegisterWorkflowWithOptions(TaskQueueScannerWorkflow, workflow.RegisterOptions{Name: tqScannerWFTypeName})
		work.RegisterWorkflowWithOptions(HistoryScannerWorkflow, workflow.RegisterOptions{Name: historyScannerWFTypeName})
		work.RegisterWorkflowWithOptions(ExecutionsScannerWorkflow, workflow.RegisterOptions{Name: executionsScannerWFTypeName})
		work.RegisterActivityWithOptions(TaskQueueScavengerActivity, activity.RegisterOptions{Name: taskQueueScavengerActivityName})
		work.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
		work.RegisterActivityWithOptions(ExecutionsScavengerActivity, activity.RegisterOptions{Name: executionsScavengerActivityName})

		// TODO: Nothing is gracefully stopping these workers or listening for fatal errors.
		if err := work.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scanner) Stop() {
	s.lifecycleCancel()
	s.wg.Wait()
}

func (s *Scanner) startWorkflowWithRetry(ctx context.Context, options sdkclient.StartWorkflowOptions, workflowType string, workflowArgs ...any) {
	defer s.wg.Done()

	policy := backoff.NewExponentialRetryPolicy(time.Second).
		WithMaximumInterval(time.Minute).
		WithExpirationInterval(backoff.NoInterval)
	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		return s.startWorkflow(
			ctx,
			s.context.sdkClientFactory.GetSystemClient(),
			options,
			workflowType,
			workflowArgs...,
		)
	}, policy, func(err error) bool {
		return true
	})
	// if the scanner shuts down before the workflow is started, then the error will be context canceled
	if err != nil && !common.IsContextCanceledErr(err) {
		s.context.logger.Fatal("unable to start scanner", tag.WorkflowType(workflowType), tag.Error(err))
	}
}

func (s *Scanner) startWorkflow(
	ctx context.Context,
	client sdkclient.Client,
	options sdkclient.StartWorkflowOptions,
	workflowType string,
	workflowArgs ...any,
) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	_, err := client.ExecuteWorkflow(ctx, options, workflowType, workflowArgs...)
	cancel()
	if err != nil {
		if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
			return nil
		}
		s.context.logger.Error("error starting workflow", tag.WorkflowType(workflowType), tag.Error(err))
		return err
	}
	s.context.logger.Info("workflow successfully started", tag.WorkflowType(workflowType))
	return nil
}
