gRPC Fault Injection Testing Opportunities

 Summary

 This plan identifies gaps in test coverage where the new gRPC fault injection framework (testcore.InjectRPCFault) could be used to test error handling and recovery paths that are currently
 untested or rely on slow timeout-based testing.

 High-Priority Test Opportunities

 1. Query Workflow - Sticky Fallback Testing

 Location: service/history/api/queryworkflow/api.go:343-387

 Current Gap: No tests for matching service failures during sticky query dispatch.

 Proposed Test:
 func TestQueryWorkflow_StickyMatchingUnavailable_FallbackToNormal() {
     // Inject fault on first matching.QueryWorkflow call (sticky queue)
     // Verify stickiness is cleared and retry on normal queue succeeds
 }

 Error Types to Test:
 - serviceerror.NewUnavailable - Matching temporarily down
 - serviceerror.NewDeadlineExceeded - Sticky context timeout
 - StickyWorkerUnavailable - No sticky pollers

 ---
 2. Activity Task Started - Deployment Version Query Failures

 Location: service/history/api/recordactivitytaskstarted/api.go:281-307

 Current Gap: No tests for GetTaskQueueUserData failures during activity task dispatch with deployment versioning.

 Proposed Test:
 func TestRecordActivityTaskStarted_DeploymentVersionQueryFails() {
     // Inject fault on matching.GetTaskQueueUserData
     // Verify activity task is properly rejected and can retry
 }

 ---
 3. Matching Poll - ResourceExhausted Handling by Cause

 Location: service/matching/matching_engine.go:755-762 (workflow), 991-998 (activity)

 Current Gap: Different behavior for BUSY_WORKFLOW cause vs other causes, not tested.

 Proposed Test:
 func TestPollWorkflowTask_ResourceExhausted_BusyWorkflow() {
     // Inject ResourceExhausted with BUSY_WORKFLOW cause
     // Verify special handling vs generic ResourceExhausted
 }

 ---
 4. Forwarder Error Handling

 Location: service/matching/forwarder.go:337-342

 Current Gap: Only ResourceExhausted is specially handled; other errors pass through untested.

 Proposed Tests:
 func TestForwarder_UnavailableError_PropagatesDirectly() {
     // Inject Unavailable on parent partition forward
     // Verify error propagates (no special handling)
 }

 func TestForwarder_ResourceExhausted_ReturnsSlowDown() {
     // Inject ResourceExhausted on forward
     // Verify errForwarderSlowDown is returned
 }

 ---
 5. ActivityStartDuringTransition Error

 Location: service/matching/matching_engine.go:974-990

 Current Gap: Unique error only for activity tasks during deployment transitions, no test coverage.

 Proposed Test:
 func TestPollActivityTask_StartDuringTransition() {
     // Set up deployment transition state
     // Inject ActivityStartDuringTransition error
     // Verify proper error handling and retry behavior
 }

 ---
 6. Speculative WT - Double Failure Scenario

 Location: service/history/api/updateworkflow/api.go:223-248

 Current Gap: Tests first failure with sticky fallback, but not second failure on normal queue.

 Proposed Test:
 func TestSpeculativeWT_BothStickyAndNormalFail() {
     // Inject StickyWorkerUnavailable on first AddWorkflowTask
     // Inject another error on second AddWorkflowTask (normal queue)
     // Verify 5-second timeout recovery triggers normal WT scheduling
 }

 ---
 Medium-Priority Test Opportunities

 7. Namespace Handover State

 Location: service/matching/matching_engine.go:765-769

 Gap: Special handling for namespace handover not tested.

 8. TaskAlreadyStarted Scenarios

 Location: service/matching/matching_engine.go:721-723

 Gap: Duplicate task dispatch scenario needs deterministic testing.

 9. ObsoleteDispatchBuildId / ObsoleteMatchingTask

 Location: service/matching/matching_engine.go:724-754

 Gap: Versioning-specific errors during deployment transitions untested.

 10. Record Task Started Timeout

 Location: service/matching/matching_engine.go:3102-3112

 Gap: 1s sync match timeout and 10s default timeout behavior untested.

 ---
 Implementation Approach

 Test File Locations

 - tests/query_workflow_test.go - Add sticky fallback fault injection tests
 - tests/update_workflow_test.go - Add speculative WT failure chain tests
 - tests/activity_test.go or new file - Add deployment version query tests
 - tests/matching_fault_injection_test.go (new) - Matching-specific error handling

 Common Pattern

 func TestXxx_ErrorScenario(s *TestSuite) {
     var faultInjected atomic.Bool
     namespaceID := s.NamespaceID().String()

     testcore.InjectRPCFault(s.T(), s.GetTestCluster(),
         func(req, resp any, _ error) error {
             r, ok := req.(*matchingservice.XxxRequest)
             if ok && resp == nil && r.GetNamespaceId() == namespaceID &&
                faultInjected.CompareAndSwap(false, true) {
                 return serviceerror.NewXxx("injected fault")
             }
             return nil
         })

     // Execute operation that triggers the RPC
     // Verify error handling / recovery behavior
 }

 ---
 Benefits Over Current Approach
 ┌────────────────────────────────────┬─────────────────────────────┐
 │          Current Approach          │  Fault Injection Approach   │
 ├────────────────────────────────────┼─────────────────────────────┤
 │ time.Sleep(10s) for sticky timeout │ Immediate fault, ~5s test   │
 ├────────────────────────────────────┼─────────────────────────────┤
 │ Cannot test matching unavailable   │ Direct error injection      │
 ├────────────────────────────────────┼─────────────────────────────┤
 │ Flaky timing-dependent tests       │ Deterministic fault trigger │
 ├────────────────────────────────────┼─────────────────────────────┤
 │ Cannot test error chains           │ Multiple faults in sequence │
 └────────────────────────────────────┴─────────────────────────────┘
 ---
 Verification

 For each new test:
 1. Verify fault injection fires (log message appears)
 2. Verify expected error handling path is taken
 3. Verify recovery/fallback behavior works correctly
 4. Check relevant metrics are recorded
 5. Ensure test completes in reasonable time (<10s ideally)
