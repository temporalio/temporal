package tests

import "testing"

func TestAcquireShardSuite(t *testing.T) {
	runTestAcquireShardSuite(t)
}

func TestActivityAPIBatchCancelClientTestSuite(t *testing.T) {
	runTestActivityAPIBatchCancelClientTestSuite(t)
}

func TestActivityAPIBatchDeleteClientTestSuite(t *testing.T) {
	runTestActivityAPIBatchDeleteClientTestSuite(t)
}

func TestActivityAPIBatchResetClientTestSuite(t *testing.T) {
	runTestActivityAPIBatchResetClientTestSuite(t)
}

func TestActivityAPIBatchSecurityTestSuite(t *testing.T) {
	runTestActivityAPIBatchSecurityTestSuite(t)
}

func TestActivityAPIBatchTerminateClientTestSuite(t *testing.T) {
	runTestActivityAPIBatchTerminateClientTestSuite(t)
}

func TestActivityApiBatchUnpauseClientTestSuite(t *testing.T) {
	runTestActivityAPIBatchUnpauseClientTestSuite(t)
}

func TestActivityApiBatchUpdateOptionsClientTestSuite(t *testing.T) {
	runTestActivityAPIBatchUpdateOptionsClientTestSuite(t)
}

func TestActivityApiPauseClientTestSuite(t *testing.T) {
	runTestActivityAPIPauseClientTestSuite(t)
}

func TestActivityApiPause_AttributesToActivityInContextMetadata(t *testing.T) {
	runTestActivityAPIPauseAttributesToActivityInContextMetadata(t)
}

func TestActivityApiResetClientTestSuite(t *testing.T) {
	runTestActivityAPIResetClientTestSuite(t)
}

func TestActivityApiRulesClientTestSuite(t *testing.T) {
	runTestActivityAPIRulesClientTestSuite(t)
}

func TestActivityApiUpdateClientTestSuite(t *testing.T) {
	runTestActivityAPIUpdateClientTestSuite(t)
}

func TestActivityClientTestSuite(t *testing.T) {
	runTestActivityClientTestSuite(t)
}

func TestActivityParityTestSuite(t *testing.T) {
	runTestActivityParityTestSuite(t)
}

func TestActivityTestSuite(t *testing.T) {
	runTestActivityTestSuite(t)
}

func TestActivityUpdateExecutionOptionsApi(t *testing.T) {
	runTestActivityUpdateExecutionOptionsAPI(t)
}

func TestAddTasksSuite(t *testing.T) {
	runTestAddTasksSuite(t)
}

func TestAdminBatchRefreshWorkflowTasksTestSuite(t *testing.T) {
	runTestAdminBatchRefreshWorkflowTasksTestSuite(t)
}

func TestAdminRebuildMutableState_ChasmDisabled(t *testing.T) {
	runTestAdminRebuildMutableStateChasmDisabled(t)
}

func TestAdminRebuildMutableState_ChasmEnabled(t *testing.T) {
	runTestAdminRebuildMutableStateChasmEnabled(t)
}

func TestAdvancedVisibilitySuite(t *testing.T) {
	runTestAdvancedVisibilitySuite(t)
}

func TestAdvancedVisibilitySuiteLegacy(t *testing.T) {
	runTestAdvancedVisibilitySuiteLegacy(t)
}

func TestArchivalSuite(t *testing.T) {
	runTestArchivalSuite(t)
}

func TestCallbacksMigrationSuite(t *testing.T) {
	runTestCallbacksMigrationSuite(t)
}

func TestCallbacksSuiteCHASM(t *testing.T) {
	runTestCallbacksSuiteCHASM(t)
}

func TestCallbacksSuiteHSM(t *testing.T) {
	runTestCallbacksSuiteHSM(t)
}

func TestCancelWorkflowSuite(t *testing.T) {
	runTestCancelWorkflowSuite(t)
}

func TestChasmSuite(t *testing.T) {
	runTestChasmSuite(t)
}

func TestChildWorkflowSuite(t *testing.T) {
	runTestChildWorkflowSuite(t)
}

func TestClientDataConverterTestSuite(t *testing.T) {
	runTestClientDataConverterTestSuite(t)
}

func TestClientMiscTestSuite(t *testing.T) {
	runTestClientMiscTestSuite(t)
}

func TestContinueAsNewTestSuite(t *testing.T) {
	runTestContinueAsNewTestSuite(t)
}

func TestCronTestClientSuite(t *testing.T) {
	runTestCronTestClientSuite(t)
}

func TestCronTestSuite(t *testing.T) {
	runTestCronTestSuite(t)
}

func TestDLQSuite(t *testing.T) {
	runTestDLQSuite(t)
}

func TestDeploymentVersionSuite(t *testing.T) {
	runTestDeploymentVersionSuite(t)
}

func TestDescribeTestSuite(t *testing.T) {
	runTestDescribeTestSuite(t)
}

func TestDispatchCancelToWorkerWithEagerActivity(t *testing.T) {
	runTestDispatchCancelToWorkerWithEagerActivity(t)
}

func TestEagerWorkflowTestSuite(t *testing.T) {
	runTestEagerWorkflowTestSuite(t)
}

func TestFairnessAutoEnableSuite(t *testing.T) {
	runTestFairnessAutoEnableSuite(t)
}

func TestFairnessSuite(t *testing.T) {
	runTestFairnessSuite(t)
}

func TestGetHistorySuite_DisableTransitionHistory(t *testing.T) {
	runTestGetHistorySuiteDisableTransitionHistory(t)
}

func TestGetHistorySuite_EnableTransitionHistory(t *testing.T) {
	runTestGetHistorySuiteEnableTransitionHistory(t)
}

func TestHistoryNodeCleanupSuite(t *testing.T) {
	runTestHistoryNodeCleanupSuite(t)
}

func TestHttpApiTestSuite(t *testing.T) {
	runTestHTTPAPITestSuite(t)
}

func TestLinksTestSuite(t *testing.T) {
	runTestLinksTestSuite(t)
}

func TestMaxBufferedEventSuite(t *testing.T) {
	runTestMaxBufferedEventSuite(t)
}

func TestMirroredIncludeExcludeSpec(t *testing.T) {
	runTestMirroredIncludeExcludeSpec(t)
}

func TestMirroredIncludeExcludeSpecOnUpdate(t *testing.T) {
	runTestMirroredIncludeExcludeSpecOnUpdate(t)
}

func TestNamespaceInterceptorTestSuite(t *testing.T) {
	runTestNamespaceInterceptorTestSuite(t)
}

func TestNamespaceSuite(t *testing.T) {
	runTestNamespaceSuite(t)
}

func TestNexusAPIValidationTestSuite(t *testing.T) {
	runTestNexusAPIValidationTestSuite(t)
}

func TestNexusApiTestSuiteWithLegacyErrorPaths(t *testing.T) {
	runTestNexusAPITestSuiteWithLegacyErrorPaths(t)
}

func TestNexusApiTestSuiteWithTemporalFailures(t *testing.T) {
	runTestNexusAPITestSuiteWithTemporalFailures(t)
}

func TestNexusEndpointsCommonSuite(t *testing.T) {
	runTestNexusEndpointsCommonSuite(t)
}

func TestNexusEndpointsMatchingSuite(t *testing.T) {
	runTestNexusEndpointsMatchingSuite(t)
}

func TestNexusEndpointsOperatorSuite(t *testing.T) {
	runTestNexusEndpointsOperatorSuite(t)
}

func TestNexusMatchingTestSuite(t *testing.T) {
	runTestNexusMatchingTestSuite(t)
}

func TestNexusStandaloneTestSuite(t *testing.T) {
	runTestNexusStandaloneTestSuite(t)
}

func TestNexusWorkflowTestSuiteCHASM(t *testing.T) {
	runTestNexusWorkflowTestSuiteCHASM(t)
}

func TestNexusWorkflowTestSuiteHSM(t *testing.T) {
	runTestNexusWorkflowTestSuiteHSM(t)
}

func TestNexusWorkflowUpdateTestSuite(t *testing.T) {
	runTestNexusWorkflowUpdateTestSuite(t)
}

func TestNilSearchAttributeSuite(t *testing.T) {
	runTestNilSearchAttributeSuite(t)
}

func TestPartitionScaling_Backlog(t *testing.T) {
	runTestPartitionScalingBacklog(t)
}

func TestPartitionScaling_Down(t *testing.T) {
	runTestPartitionScalingDown(t)
}

func TestPartitionScaling_Down_AndStopPolling(t *testing.T) {
	runTestPartitionScalingDownAndStopPolling(t)
}

func TestPartitionScaling_Down_FromDC(t *testing.T) {
	runTestPartitionScalingDownFromDC(t)
}

func TestPartitionScaling_Up(t *testing.T) {
	runTestPartitionScalingUp(t)
}

func TestPartitionScaling_Up_FromDC(t *testing.T) {
	runTestPartitionScalingUpFromDC(t)
}

func TestPauseWorkflowExecutionSuite(t *testing.T) {
	runTestPauseWorkflowExecutionSuite(t)
}

func TestPollerScalingFunctionalSuite(t *testing.T) {
	runTestPollerScalingFunctionalSuite(t)
}

func TestPrematureEosTestSuite(t *testing.T) {
	runTestPrematureEosTestSuite(t)
}

func TestPrioritySuite(t *testing.T) {
	runTestPrioritySuite(t)
}

func TestPurgeDLQTasksSuite(t *testing.T) {
	runTestPurgeDLQTasksSuite(t)
}

func TestQueryWorkflowSuite(t *testing.T) {
	runTestQueryWorkflowSuite(t)
}

func TestRawHistorySuite(t *testing.T) {
	runTestRawHistorySuite(t)
}

func TestRelayTaskTestSuite(t *testing.T) {
	runTestRelayTaskTestSuite(t)
}

func TestResetWorkflowTestSuite(t *testing.T) {
	runTestResetWorkflowTestSuite(t)
}

func TestScheduleCHASM(t *testing.T) {
	runTestScheduleCHASM(t)
}

func TestScheduleCHASMWorkflowPauseInteraction(t *testing.T) {
	runTestScheduleCHASMWorkflowPauseInteraction(t)
}

func TestScheduleCountsVisibility(t *testing.T) {
	runTestScheduleCountsVisibility(t)
}

func TestScheduleCreationRolloutPercent(t *testing.T) {
	runTestScheduleCreationRolloutPercent(t)
}

func TestScheduleFarFutureActionTimes(t *testing.T) {
	runTestScheduleFarFutureActionTimes(t)
}

func TestScheduleManyCalendars(t *testing.T) {
	runTestScheduleManyCalendars(t)
}

func TestScheduleMigrationDeferredWithRunningWorkflow(t *testing.T) {
	runTestScheduleMigrationDeferredWithRunningWorkflow(t)
}

func TestScheduleMigrationTestSuite(t *testing.T) {
	runTestScheduleMigrationTestSuite(t)
}

func TestScheduleMigrationV1ToV2NoDuplicateRecentActions(t *testing.T) {
	runTestScheduleMigrationV1ToV2NoDuplicateRecentActions(t)
}

func TestScheduleMigration_NoRunningWorkflows_GeneratorStarts(t *testing.T) {
	runTestScheduleMigrationNoRunningWorkflowsGeneratorStarts(t)
}

func TestScheduleMigration_StaleRunningDoesNotSkipPending(t *testing.T) {
	runTestScheduleMigrationStaleRunningDoesNotSkipPending(t)
}

func TestScheduleNextActionTimeVisibility(t *testing.T) {
	runTestScheduleNextActionTimeVisibility(t)
}

func TestScheduleV1(t *testing.T) {
	runTestScheduleV1(t)
}

func TestScheduleV1WorkflowPauseInteraction(t *testing.T) {
	runTestScheduleV1WorkflowPauseInteraction(t)
}

func TestSignalWithStartFromWorkflowTestSuite(t *testing.T) {
	runTestSignalWithStartFromWorkflowTestSuite(t)
}

func TestSignalWorkflowTestSuiteChasm(t *testing.T) {
	runTestSignalWorkflowTestSuiteChasm(t)
}

func TestSignalWorkflowTestSuiteLegacy(t *testing.T) {
	runTestSignalWorkflowTestSuiteLegacy(t)
}

func TestSizeLimitFunctionalSuite(t *testing.T) {
	runTestSizeLimitFunctionalSuite(t)
}

func TestStandaloneActivityTestSuite(t *testing.T) {
	runTestStandaloneActivityTestSuite(t)
}

func TestStickyTqTestSuite(t *testing.T) {
	runTestStickyTqTestSuite(t)
}

func TestTLSFunctionalSuite(t *testing.T) {
	runTestTLSFunctionalSuite(t)
}

func TestTaskQueueStats_Pri_Suite(t *testing.T) {
	runTestTaskQueueStatsPriSuite(t)
}

func TestTaskQueueSuite(t *testing.T) {
	runTestTaskQueueSuite(t)
}

func TestTimeSkippingFastForwardFunctionalSuite(t *testing.T) {
	runTestTimeSkippingFastForwardFunctionalSuite(t)
}

func TestTimeSkippingPropagationTestSuite(t *testing.T) {
	runTestTimeSkippingPropagationTestSuite(t)
}

func TestTimeSkippingTestSuite(t *testing.T) {
	runTestTimeSkippingTestSuite(t)
}

func TestTransientTaskSuite(t *testing.T) {
	runTestTransientTaskSuite(t)
}

func TestUpdateWithStartSuite(t *testing.T) {
	runTestUpdateWithStartSuite(t)
}

func TestUpdateWorkflowSdkSuite(t *testing.T) {
	runTestUpdateWorkflowSdkSuite(t)
}

func TestUserMetadataSuite(t *testing.T) {
	runTestUserMetadataSuite(t)
}

func TestUserTimersTestSuite(t *testing.T) {
	runTestUserTimersTestSuite(t)
}

func TestVersioning3FunctionalSuite(t *testing.T) {
	runTestVersioning3FunctionalSuite(t)
}

func TestVersioning3OneTimeOverrideFunctionalSuite(t *testing.T) {
	runTestVersioning3OneTimeOverrideFunctionalSuite(t)
}

func TestVersioningFunctionalSuite(t *testing.T) {
	runTestVersioningFunctionalSuite(t)
}

func TestWFTFailureReportedProblemsTestSuite(t *testing.T) {
	runTestWFTFailureReportedProblemsTestSuite(t)
}

func TestWorkerCommandsTaskSuite(t *testing.T) {
	runTestWorkerCommandsTaskSuite(t)
}

func TestWorkerDeploymentSuite(t *testing.T) {
	runTestWorkerDeploymentSuite(t)
}

func TestWorkerRegistryTestSuite(t *testing.T) {
	runTestWorkerRegistryTestSuite(t)
}

func TestWorkflowAPIBatchCancelClientTestSuite(t *testing.T) {
	runTestWorkflowAPIBatchCancelClientTestSuite(t)
}

func TestWorkflowAPIBatchDeleteClientTestSuite(t *testing.T) {
	runTestWorkflowAPIBatchDeleteClientTestSuite(t)
}

func TestWorkflowAPIBatchResetClientTestSuite(t *testing.T) {
	runTestWorkflowAPIBatchResetClientTestSuite(t)
}

func TestWorkflowAPIBatchSignalClientTestSuite(t *testing.T) {
	runTestWorkflowAPIBatchSignalClientTestSuite(t)
}

func TestWorkflowAPIBatchTerminateClientTestSuite(t *testing.T) {
	runTestWorkflowAPIBatchTerminateClientTestSuite(t)
}

func TestWorkflowAPIBatchUpdateOptionsClientTestSuite(t *testing.T) {
	runTestWorkflowAPIBatchUpdateOptionsClientTestSuite(t)
}

func TestWorkflowAliasSearchAttributeTestSuite(t *testing.T) {
	runTestWorkflowAliasSearchAttributeTestSuite(t)
}

func TestWorkflowBufferedEventsTestSuite(t *testing.T) {
	runTestWorkflowBufferedEventsTestSuite(t)
}

func TestWorkflowCompletionPaginationTestSuite(t *testing.T) {
	runTestWorkflowCompletionPaginationTestSuite(t)
}

func TestWorkflowDeleteExecutionSuite(t *testing.T) {
	runTestWorkflowDeleteExecutionSuite(t)
}

func TestWorkflowFailuresTestSuite(t *testing.T) {
	runTestWorkflowFailuresTestSuite(t)
}

func TestWorkflowMemoTestSuite(t *testing.T) {
	runTestWorkflowMemoTestSuite(t)
}

func TestWorkflowResetTestSuite(t *testing.T) {
	runTestWorkflowResetTestSuite(t)
}

func TestWorkflowResetWithChildTestSuite(t *testing.T) {
	runTestWorkflowResetWithChildTestSuite(t)
}

func TestWorkflowTaskTestSuite(t *testing.T) {
	runTestWorkflowTaskTestSuite(t)
}

func TestWorkflowTestSuite(t *testing.T) {
	runTestWorkflowTestSuite(t)
}

func TestWorkflowTimerTestSuite(t *testing.T) {
	runTestWorkflowTimerTestSuite(t)
}

func TestWorkflowTypeEncodingSuite(t *testing.T) {
	runTestWorkflowTypeEncodingSuite(t)
}

func TestWorkflowUpdateSuite(t *testing.T) {
	runTestWorkflowUpdateSuite(t)
}

func TestWorkflowVisibilityTestSuite(t *testing.T) {
	runTestWorkflowVisibilityTestSuite(t)
}
