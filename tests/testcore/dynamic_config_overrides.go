package testcore

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/components/nexusoperations"
)

var (
	// Functional tests don't use dynamic config files. All settings get their default values
	// defined in common/dynamicconfig/constants.go.
	//
	// There are 4 ways to override a setting:
	// 1. Globally using this file. Every test suite creates a new test cluster using this overrides.
	// 2. Per test suite using FunctionalTestBase.SetupSuiteWithCluster() and WithDynamicConfigOverrides() option.
	// 3. Per test using FunctionalTestBase.OverrideDynamicConfig() method.
	// 4. Per specific cluster per test (if test has more than one cluster) using TestCluster.OverrideDynamicConfig() method.
	//
	// NOTE1: settings which are not really dynamic (requires server restart to take effect) can't be overridden on test level,
	//        i.e., must be overridden globally (1) or per test suite (2).
	// NOTE2: per test overrides change the value for the cluster, therefore, it affects not only a specific test, but
	//        all tests for that suite. The automatic cleanup reverts to the previous value and tests don't affect each other.
	//        But that means tests in the same suite can't be run in parallel. This is not a problem because testify
	//        doesn't allow parallel execution of tests in the same suite anyway. If one day, it is allowed,
	//        unique namespaces with overrides per namespace should be used for tests that require overrides.
	dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendRPS.Key():                                         3000,
		dynamicconfig.FrontendMaxNamespaceVisibilityRPSPerInstance.Key():        50,
		dynamicconfig.FrontendMaxNamespaceVisibilityBurstRatioPerInstance.Key(): 1,
		dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts.Key():       1,
		dynamicconfig.SecondaryVisibilityWritingMode.Key():                      visibility.SecondaryVisibilityWritingModeOff,
		dynamicconfig.WorkflowTaskHeartbeatTimeout.Key():                        5 * time.Second,
		dynamicconfig.ReplicationTaskFetcherAggregationInterval.Key():           200 * time.Millisecond,
		dynamicconfig.ReplicationTaskFetcherErrorRetryWait.Key():                50 * time.Millisecond,
		dynamicconfig.ReplicationTaskProcessorErrorRetryWait.Key():              time.Millisecond,
		dynamicconfig.ClusterMetadataRefreshInterval.Key():                      100 * time.Millisecond,
		dynamicconfig.NamespaceCacheRefreshInterval.Key():                       NamespaceCacheRefreshInterval,
		dynamicconfig.ReplicationEnableUpdateWithNewTaskMerge.Key():             true,
		dynamicconfig.FrontendMaskInternalErrorDetails.Key():                    false,
		dynamicconfig.HistoryScannerEnabled.Key():                               false,
		dynamicconfig.TaskQueueScannerEnabled.Key():                             false,
		dynamicconfig.ExecutionsScannerEnabled.Key():                            false,
		dynamicconfig.BuildIdScavengerEnabled.Key():                             false,

		// Better to read through in tests than add artificial sleeps (which is what we previously had).
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Key(): true,

		// Enable raw history for functional tests.
		// TODO (prathyush): remove this after setting it to true by default.
		dynamicconfig.SendRawHistoryBetweenInternalServices.Key(): true,

		dynamicconfig.RetentionTimerJitterDuration.Key(): time.Second,

		dynamicconfig.EnableTransitionHistory.Key(): true,

		dynamicconfig.NumPendingChildExecutionsLimitError.Key():             ClientSuiteLimit,
		dynamicconfig.NumPendingActivitiesLimitError.Key():                  ClientSuiteLimit,
		dynamicconfig.NumPendingCancelRequestsLimitError.Key():              ClientSuiteLimit,
		dynamicconfig.NumPendingSignalsLimitError.Key():                     ClientSuiteLimit,
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): ClientSuiteLimit,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():          true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key():      true,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                    1 * time.Millisecond,
		nexusoperations.RecordCancelRequestCompletionEvents.Key():           true,
		nexusoperations.UseSystemCallbackURL.Key():                          true,
	}
)
