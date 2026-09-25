package scanner

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/worker/scanner/build_ids"
	"go.uber.org/mock/gomock"
)

type scannerTestSuite struct {
	suite.Suite
}

func TestScanner(t *testing.T) {
	suite.Run(t, new(scannerTestSuite))
}

func (s *scannerTestSuite) TestScannerEnabled() {
	type expectedScanner struct {
		WFTypeName    string
		TaskQueueName string
	}
	executionScanner := expectedScanner{
		WFTypeName:    executionsScannerWFTypeName,
		TaskQueueName: executionsScannerTaskQueueName,
	}
	_ = executionScanner
	taskQueueScanner := expectedScanner{
		WFTypeName:    tqScannerWFTypeName,
		TaskQueueName: tqScannerTaskQueueName,
	}
	historyScanner := expectedScanner{
		WFTypeName:    historyScannerWFTypeName,
		TaskQueueName: historyScannerTaskQueueName,
	}
	buildIdScavenger := expectedScanner{
		WFTypeName:    build_ids.BuildIdScavangerWorkflowName,
		TaskQueueName: build_ids.BuildIdScavengerTaskQueueName,
	}

	type testCase struct {
		Name                     string
		ExecutionsScannerEnabled bool
		TaskQueueScannerEnabled  bool
		HistoryScannerEnabled    bool
		BuildIdScavengerEnabled  bool
		DefaultStore             string
		ExpectedScanners         []expectedScanner
	}

	for _, c := range []testCase{
		{
			Name:                     "NothingEnabledNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{},
		},
		{
			Name:                     "NothingEnabledSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{},
		},
		{
			Name:                     "HistoryScannerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{historyScanner},
		},
		{
			Name:                     "HistoryScannerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{historyScanner},
		},
		{
			Name:                     "TaskQueueScannerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{}, // TODO: enable task queue scanner for NoSQL?
		},
		{
			Name:                     "TaskQueueScannerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{taskQueueScanner},
		},
		{
			Name:                     "ExecutionsScannerNoSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{executionScanner},
		},
		{
			Name:                     "ExecutionsScannerSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{}, // ExecutionsScanner is not supported for SQL store
		},
		{
			Name:                     "BuildIdScavengerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{buildIdScavenger},
		},
		{
			Name:                     "BuildIdScavengerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{buildIdScavenger},
		},
		{
			Name:                     "AllScannersSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{historyScanner, taskQueueScanner, buildIdScavenger}, // ExecutionsScanner is not supported for SQL store
		},
		{
			Name:                     "AllScannersNoSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{historyScanner, executionScanner, buildIdScavenger}, // TaskQueueScanner is only supported for SQL store
		},
	} {
		s.Run(c.Name, func() {
			ctrl := gomock.NewController(s.T())
			mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
			mockSdkClient := mocksdk.NewMockClient(ctrl)
			mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
			mockAdminClient := adminservicemock.NewMockAdminServiceClient(ctrl)
			scanner := New(
				log.NewNoopLogger(),
				&Config{
					MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
					MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
					MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
					MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
					HistoryScannerEnabled:                  dynamicconfig.GetBoolPropertyFn(c.HistoryScannerEnabled),
					HistoryScannerCronSchedule:             dynamicconfig.GetStringPropertyFn(historyScannerWFStartOptions.CronSchedule),
					BuildIdScavengerEnabled:                dynamicconfig.GetBoolPropertyFn(c.BuildIdScavengerEnabled),
					ExecutionsScannerEnabled:               dynamicconfig.GetBoolPropertyFn(c.ExecutionsScannerEnabled),
					TaskQueueScannerEnabled:                dynamicconfig.GetBoolPropertyFn(c.TaskQueueScannerEnabled),
					ScheduleInvariantsScannerOptions:       dynamicconfig.GetTypedPropertyFn(dynamicconfig.DefaultScheduleInvariantsScannerParams),
					Persistence: &config.Persistence{
						DefaultStore: c.DefaultStore,
						DataStores: map[string]config.DataStore{
							config.StoreTypeNoSQL: {},
							config.StoreTypeSQL: {
								SQL: &config.SQL{},
							},
						},
					},
				},
				mockSdkClientFactory,
				metrics.NoopMetricsHandler,
				p.NewMockExecutionManager(ctrl),
				// These nils are irrelevant since they're only used by the build ID scavenger which is not tested here.
				nil,
				nil,
				p.NewMockTaskManager(ctrl),
				historyservicemock.NewMockHistoryServiceClient(ctrl),
				mockAdminClient,
				nil,
				mockNamespaceRegistry,
				"active-cluster",
				membership.NewHostInfoFromAddress("localhost"),
				serialization.NewSerializer(),
			)
			var wg sync.WaitGroup
			for _, sc := range c.ExpectedScanners {
				wg.Add(1)
				worker := mocksdk.NewMockWorker(ctrl)
				worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
				worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
				worker.EXPECT().Start()
				worker.EXPECT().Stop()
				mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), sc.TaskQueueName, gomock.Any()).Return(worker)
				mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
				mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), sc.WFTypeName,
					gomock.Any()).Do(func(
					_ context.Context,
					_ client.StartWorkflowOptions,
					_ string,
					_ ...any,
				) {
					wg.Done()
				})
			}
			err := scanner.Start()
			s.NoError(err)
			wg.Wait()
			scanner.Stop()
		})
	}
}

// TestScannerWorkflow tests that the scanner can be shut down even when it hasn't finished starting.
// This fixes a rare issue that can occur when Stop() is called quickly after Start(). When Start() is called, the
// scanner starts a new goroutine for each scanner type. In that goroutine, an sdk client is created which dials the
// frontend service. If the test driver calls Stop() on the server, then the server stops the frontend service and the
// history service. In some cases, the frontend services stops before the sdk client has finished connecting to it.
// This causes the startWorkflow() call to fail with an error.  However, startWorkflowWithRetry retries the call for
// a whole minute, which causes the test to take a long time to fail. So, instead we immediately cancel all async
// requests when Stop() is called.
func (s *scannerTestSuite) TestScannerShutdown() {
	ctrl := gomock.NewController(s.T())

	logger := log.NewTestLogger()
	mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
	mockSdkClient := mocksdk.NewMockClient(ctrl)
	mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
	mockAdminClient := adminservicemock.NewMockAdminServiceClient(ctrl)
	worker := mocksdk.NewMockWorker(ctrl)
	scanner := New(
		logger,
		&Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			HistoryScannerEnabled:                  dynamicconfig.GetBoolPropertyFn(true),
			HistoryScannerCronSchedule:             dynamicconfig.GetStringPropertyFn(historyScannerWFStartOptions.CronSchedule),
			ExecutionsScannerEnabled:               dynamicconfig.GetBoolPropertyFn(false),
			TaskQueueScannerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			BuildIdScavengerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			ScheduleInvariantsScannerOptions:       dynamicconfig.GetTypedPropertyFn(dynamicconfig.DefaultScheduleInvariantsScannerParams),
			Persistence: &config.Persistence{
				DefaultStore: config.StoreTypeNoSQL,
				DataStores: map[string]config.DataStore{
					config.StoreTypeNoSQL: {},
				},
			},
		},
		mockSdkClientFactory,
		metrics.NoopMetricsHandler,
		p.NewMockExecutionManager(ctrl),
		// These nils are irrelevant since they're only used by the build ID scavenger which is not tested here.
		nil,
		nil,
		p.NewMockTaskManager(ctrl),
		historyservicemock.NewMockHistoryServiceClient(ctrl),
		mockAdminClient,
		nil,
		mockNamespaceRegistry,
		"active-cluster",
		membership.NewHostInfoFromAddress("localhost"),
		serialization.NewSerializer(),
	)
	mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().Start()
	worker.EXPECT().Stop()
	mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), gomock.Any(), gomock.Any()).Return(worker)
	var wg sync.WaitGroup
	wg.Add(1)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context,
		_ client.StartWorkflowOptions,
		_ string,
		_ ...any,
	) (client.WorkflowRun, error) {
		wg.Done()
		<-ctx.Done()
		return nil, ctx.Err()
	})
	err := scanner.Start()
	s.NoError(err)
	wg.Wait()
	scanner.Stop()
}

func (s *scannerTestSuite) TestHistoryScannerWFStartOptionsCronSchedule() {
	for _, c := range []struct {
		Name           string
		ConfiguredCron string
		ExpectedCron   string
	}{
		{
			Name:           "Default",
			ConfiguredCron: "0 */12 * * *",
			ExpectedCron:   "0 */12 * * *",
		},
		{
			Name:           "Custom",
			ConfiguredCron: "0 * * * *",
			ExpectedCron:   "0 * * * *",
		},
		{
			Name:           "EmptyFallsBackToDefault",
			ConfiguredCron: "",
			ExpectedCron:   "0 */12 * * *",
		},
		{
			Name:           "InvalidFallsBackToDefault",
			ConfiguredCron: "not-a-cron-spec",
			ExpectedCron:   "0 */12 * * *",
		},
	} {
		s.Run(c.Name, func() {
			scanner := &Scanner{
				context: scannerContext{
					cfg: &Config{
						HistoryScannerCronSchedule: dynamicconfig.GetStringPropertyFn(c.ConfiguredCron),
					},
					logger: log.NewNoopLogger(),
				},
			}
			options := scanner.historyScannerWFStartOptions()
			s.Equal(c.ExpectedCron, options.CronSchedule)
			s.Equal(historyScannerWFID, options.ID)
			s.Equal(historyScannerTaskQueueName, options.TaskQueue)
		})
	}
}

type fakeHistoryEventIterator struct {
	events []*historypb.HistoryEvent
}

func (f *fakeHistoryEventIterator) HasNext() bool {
	return len(f.events) > 0
}

func (f *fakeHistoryEventIterator) Next() (*historypb.HistoryEvent, error) {
	event := f.events[0]
	f.events = f.events[1:]
	return event, nil
}

func startedEventWithCronSchedule(cronSchedule string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				CronSchedule: cronSchedule,
			},
		},
	}
}

func (s *scannerTestSuite) newHistoryScannerForCronTest(ctrl *gomock.Controller, cronSchedule string, mockSdkClientFactory *sdk.MockClientFactory) *Scanner {
	return New(
		log.NewNoopLogger(),
		&Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			HistoryScannerEnabled:                  dynamicconfig.GetBoolPropertyFn(true),
			HistoryScannerCronSchedule:             dynamicconfig.GetStringPropertyFn(cronSchedule),
			BuildIdScavengerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			ExecutionsScannerEnabled:               dynamicconfig.GetBoolPropertyFn(false),
			TaskQueueScannerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			ScheduleInvariantsScannerOptions:       dynamicconfig.GetTypedPropertyFn(dynamicconfig.DefaultScheduleInvariantsScannerParams),
			Persistence: &config.Persistence{
				DefaultStore: config.StoreTypeNoSQL,
				DataStores: map[string]config.DataStore{
					config.StoreTypeNoSQL: {},
				},
			},
		},
		mockSdkClientFactory,
		metrics.NoopMetricsHandler,
		p.NewMockExecutionManager(ctrl),
		nil,
		nil,
		p.NewMockTaskManager(ctrl),
		historyservicemock.NewMockHistoryServiceClient(ctrl),
		adminservicemock.NewMockAdminServiceClient(ctrl),
		nil,
		namespace.NewMockRegistry(ctrl),
		"active-cluster",
		membership.NewHostInfoFromAddress("localhost"),
		serialization.NewSerializer(),
	)
}

// TestHistoryScannerRestartedOnCronScheduleChange tests that a running history scanner
// workflow is terminated and restarted when the configured cron schedule differs from
// the schedule the running workflow was started with.
func (s *scannerTestSuite) TestHistoryScannerRestartedOnCronScheduleChange() {
	ctrl := gomock.NewController(s.T())
	mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
	mockSdkClient := mocksdk.NewMockClient(ctrl)
	worker := mocksdk.NewMockWorker(ctrl)

	newCronSchedule := "0 * * * *"
	scanner := s.newHistoryScannerForCronTest(ctrl, newCronSchedule, mockSdkClientFactory)

	mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().Start()
	worker.EXPECT().Stop()
	mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), historyScannerTaskQueueName, gomock.Any()).Return(worker)

	// The first start attempt hits an already running workflow started on the old schedule.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), historyScannerWFTypeName).DoAndReturn(func(
		_ context.Context,
		options client.StartWorkflowOptions,
		_ string,
		_ ...any,
	) (client.WorkflowRun, error) {
		s.Equal(newCronSchedule, options.CronSchedule)
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", "")
	})
	mockSdkClient.EXPECT().GetWorkflowHistory(gomock.Any(), historyScannerWFID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT).Return(
		&fakeHistoryEventIterator{events: []*historypb.HistoryEvent{startedEventWithCronSchedule("0 */12 * * *")}},
	)
	mockSdkClient.EXPECT().TerminateWorkflow(gomock.Any(), historyScannerWFID, "", cronScheduleChangedTerminationReason).Return(nil)

	// The retry then starts the workflow on the new schedule.
	var wg sync.WaitGroup
	wg.Add(1)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), historyScannerWFTypeName).DoAndReturn(func(
		_ context.Context,
		options client.StartWorkflowOptions,
		_ string,
		_ ...any,
	) (client.WorkflowRun, error) {
		s.Equal(newCronSchedule, options.CronSchedule)
		wg.Done()
		return nil, nil
	})

	err := scanner.Start()
	s.NoError(err)
	wg.Wait()
	scanner.Stop()
}

// TestHistoryScannerNotRestartedWhenCronScheduleUnchanged tests that a running history
// scanner workflow is left alone when it already runs on the configured cron schedule.
func (s *scannerTestSuite) TestHistoryScannerNotRestartedWhenCronScheduleUnchanged() {
	ctrl := gomock.NewController(s.T())
	mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
	mockSdkClient := mocksdk.NewMockClient(ctrl)
	worker := mocksdk.NewMockWorker(ctrl)

	cronSchedule := "0 * * * *"
	scanner := s.newHistoryScannerForCronTest(ctrl, cronSchedule, mockSdkClientFactory)

	mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().Start()
	worker.EXPECT().Stop()
	mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), historyScannerTaskQueueName, gomock.Any()).Return(worker)

	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), historyScannerWFTypeName).Return(
		nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", ""),
	)
	var wg sync.WaitGroup
	wg.Add(1)
	mockSdkClient.EXPECT().GetWorkflowHistory(gomock.Any(), historyScannerWFID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT).DoAndReturn(func(
		_ context.Context,
		_ string,
		_ string,
		_ bool,
		_ enumspb.HistoryEventFilterType,
	) client.HistoryEventIterator {
		wg.Done()
		return &fakeHistoryEventIterator{events: []*historypb.HistoryEvent{startedEventWithCronSchedule(cronSchedule)}}
	})
	// No TerminateWorkflow and no second ExecuteWorkflow expected.

	err := scanner.Start()
	s.NoError(err)
	wg.Wait()
	scanner.Stop()
}
