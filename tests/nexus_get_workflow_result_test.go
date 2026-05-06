package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
)

type NexusGetWorkflowResultTestSuite struct {
	parallelsuite.Suite[*NexusGetWorkflowResultTestSuite]
}

func TestNexusGetWorkflowResultTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusGetWorkflowResultTestSuite{})
}

// =============================================================================
// API-level rejection tests (the handler never reaches a target workflow).
// =============================================================================

// TestGetResult_FeatureFlagDisabled: with the feature flag off, the API rejects the
// call with PermissionDenied; the handler propagates this as a Nexus operation error.
func (s *NexusGetWorkflowResultTestSuite) TestGetResult_FeatureFlagDisabled() {
	// Feature flag intentionally NOT enabled here.
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(callbacks.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
	)
	cfg := newGetResultNexusConfig(s.T())

	// Any wfID works - the API rejects on the feature flag before looking up the workflow.
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), makeGetResultHandlerFor(env, cfg.targetWfID, ""))

	w := worker.New(env.SdkClient(), cfg.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(callerWF, workflow.RegisterOptions{Name: callerWFName})
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWFName, endpointName)
	s.NoError(err)

	err = run.Get(env.Context(), nil)
	s.Error(err)
	s.Contains(err.Error(), "GetWorkflowExecutionResult operation is disabled")

	callerHist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID()})
	s.RequireHistoryEvent(callerHist, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED)
}

// TestGetResult_InvalidExecution: handler invokes the API with an empty workflow ID,
// which triggers InvalidArgument; the handler propagates this as a Nexus error.
func (s *NexusGetWorkflowResultTestSuite) TestGetResult_InvalidExecution() {
	env := newGetResultNexusEnv(s.T())
	cfg := newGetResultNexusConfig(s.T())

	// Empty wfID baked into the handler - the API rejects with InvalidArgument.
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), makeGetResultHandlerFor(env, "", ""))

	w := worker.New(env.SdkClient(), cfg.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(callerWF, workflow.RegisterOptions{Name: callerWFName})
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWFName, endpointName)
	s.NoError(err)

	err = run.Get(env.Context(), nil)
	s.Error(err)
	s.Contains(err.Error(), "WorkflowId is not set")

	callerHist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID()})
	s.RequireHistoryEvent(callerHist, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED)
}

// =============================================================================
// Parameterized tests:
// tests all of (CAN|NonCAN) * (Sync|Async) * (Success|Failure) combinations
// Each test runs both WorkflowWrapped and Standalone subtests.
// =============================================================================

func (s *NexusGetWorkflowResultTestSuite) TestGetResult() {
	testcaseNameFn := func(spec *targetWFSpec) string {
		CAN := "NonCAN"
		if spec.DoCAN {
			CAN = "CAN"
		}
		sync := "Sync"
		if spec.BlockOnSignal {
			sync = "Async"
		}
		outcome := "Success"
		if spec.Fail {
			outcome = "Failure"
		}
		return fmt.Sprintf("%s_%s_%s", CAN, sync, outcome)
	}

	bools := []bool{true, false}
	for _, doCAN := range bools {
		for _, shouldBlockOnSignal := range bools {
			for _, shouldFail := range bools {
				spec := &targetWFSpec{Value: "input", DoCAN: doCAN, BlockOnSignal: shouldBlockOnSignal, Fail: shouldFail}
				s.Run(testcaseNameFn(spec), func(s *NexusGetWorkflowResultTestSuite) {
					s.Run("ViaCallerWF", func(s *NexusGetWorkflowResultTestSuite) { runViaCallerWF(s, spec) })
					s.Run("ViaStandalone", func(s *NexusGetWorkflowResultTestSuite) { runViaStandalone(s, spec) })
				})
			}
		}
	}
}

// =============================================================================
// Handler and env setup boilerplate
// =============================================================================

// getResultNexusConfig holds randomized identifiers for one test run.
type getResultNexusConfig struct {
	taskQueue  string
	targetWfID string
}

func newGetResultNexusConfig(t *testing.T) *getResultNexusConfig {
	return &getResultNexusConfig{
		taskQueue:  testcore.RandomizeStr(t.Name()),
		targetWfID: testcore.RandomizeStr("target-workflow-id"),
	}
}

// makeGetResultHandlerFor returns a Nexus handler that calls GetWorkflowExecutionResult
// on the (workflowID, runID) provided by the user.
func makeGetResultHandlerFor(env *NexusTestEnv, workflowID, runID string) nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			// Call to GetWorkflowExecutionResult with callback attached.
			resp, err := env.FrontendClient().GetWorkflowExecutionResult(
				ctx, &workflowservice.GetWorkflowExecutionResultRequest{
					Namespace: env.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
					RequestId: uuid.NewString(),
					Identity:  "test",
					Callbacks: []*commonpb.Callback{
						{
							Variant: &commonpb.Callback_Nexus_{
								Nexus: &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
							},
						},
					},
				},
			)
			if err != nil {
				return nil, &nexus.OperationError{State: nexus.OperationStateFailed, Message: err.Error()}
			}

			// Sync completion: workflow was already terminal at API call time.
			if completed := resp.GetCompleted(); completed != nil {
				if failure := completed.GetFailure(); failure != nil {
					return nil, &nexus.OperationError{State: nexus.OperationStateFailed, Message: failure.GetMessage()}
				}
				if result := completed.GetResult(); result != nil && len(result.GetPayloads()) > 0 {
					var s string
					if err := json.Unmarshal(result.GetPayloads()[0].GetData(), &s); err == nil {
						return &nexus.HandlerStartOperationResultSync[any]{Value: s}, nil
					}
				}
				return &nexus.HandlerStartOperationResultSync[any]{Value: nil}, nil
			}

			// Async: callback registered, will fire when target completes.
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "get-result-async"}, nil
		},
	}
}

func newGetResultNexusEnv(t *testing.T, opts ...testcore.TestOption) *NexusTestEnv {
	getWorkflowResultFlagsOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableGetWorkflowExecutionResult, true),
		testcore.WithDynamicConfig(
			callbacks.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
	}
	allOpts := append(getWorkflowResultFlagsOpts, opts...)
	return newNexusTestEnv(t, true, allOpts...)
}

func newStandaloneEnv(t *testing.T) *NexusTestEnv {
	return newGetResultNexusEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
	)
}

// =============================================================================
// Workflow boilerplate
// =============================================================================

const (
	targetWFName          = "get-result-target-wf"
	intentionalFailureMsg = "intentional target workflow failure"
)

// targetWFSpec parameterizes the target workflow's behavior so we can parameterize
// tests to exercise permutations of cases on the target workflow.
type targetWFSpec struct {
	Value         string `json:"value"`
	DoCAN         bool   `json:"doCAN"`         // if true, target workflow continues-as-new once
	BlockOnSignal bool   `json:"blockOnSignal"` // if true, target workflow waits for a "complete" signal before finishing
	Fail          bool   `json:"fail"`          // if true, the target workflow returns an error
}

func targetWF(ctx workflow.Context, in targetWFSpec) (string, error) {
	// Check the ContinuedExecutionRunID() so we don't CAN indefinitely.
	info := workflow.GetInfo(ctx)
	if in.DoCAN && info.ContinuedExecutionRunID == "" {
		return "", workflow.NewContinueAsNewError(ctx, info.WorkflowType.Name, in)
	}

	if in.BlockOnSignal {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
	}
	if in.Fail {
		return "", errors.New(intentionalFailureMsg)
	}
	return in.Value, nil
}

const callerWFName = "get-result-caller-wf"

// callerWF is a nexus-operation-wrapped workflow that simply executes the operation
// that maps to the GetWorkflowExecutionResult API in the target namespace (via the handler).
//
// Assertions regarding sync/async-ness and whether callbacks get attached are done via
// tests that invoke this caller workflow.
func callerWF(ctx workflow.Context, endpointName string) (string, error) {
	nexusClient := workflow.NewNexusClient(endpointName, "test")
	fut := nexusClient.ExecuteOperation(ctx, "get-result", nil, workflow.NexusOperationOptions{})
	var result string
	err := fut.Get(ctx, &result)
	return result, err
}

// =============================================================================
// Scenario runners
// =============================================================================

// setupTargetWFAndWaitReady starts a worker (registering both target and caller
// workflows), spawns the target via the SDK, and waits for the target to be in the
// state the spec expects before returning the original RunID.
//
// Returning the original RunID here ensures that tests that later queries GetWorkflowExecutionResult
// will have to walk the entire CAN chain.
func setupTargetWFAndWaitReady(
	s *NexusGetWorkflowResultTestSuite, env *NexusTestEnv, cfg *getResultNexusConfig, spec *targetWFSpec,
) string {
	w := worker.New(env.SdkClient(), cfg.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(targetWF, workflow.RegisterOptions{Name: targetWFName})
	w.RegisterWorkflowWithOptions(callerWF, workflow.RegisterOptions{Name: callerWFName})
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		ID:                       cfg.targetWfID,
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, targetWFName, spec)
	s.NoError(err)
	origRunID := run.GetRunID()

	if !spec.BlockOnSignal {
		// If we don't block on signal, ensure the target workflow is complete to exercise
		// the sync Nexus GetWorkflowExecutionResult path.
		_ = run.Get(env.Context(), nil)
	} else {
		// Otherwise wait until the workflow execution is running to exercise the async
		// Nexus GetWorkflowExecutionResult path (callbacks attaching to this run).
		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), cfg.targetWfID, "")
			require.NoError(t, err)
			currentRunID := desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
			if spec.DoCAN {
				require.NotEqual(t, origRunID, currentRunID, "workflow continue-as-new hasn't happened")
			}
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.GetWorkflowExecutionInfo().GetStatus())
			hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: cfg.targetWfID, RunId: currentRunID})
			hasCompletedWFT := false
			for _, e := range hist {
				if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
					hasCompletedWFT = true
					break
				}
			}
			require.True(t, hasCompletedWFT, "target's first WFT not yet completed")
		}, 10*time.Second, 100*time.Millisecond)
	}
	return origRunID
}

// runViaCallerWF runs the scenario via a caller workflow that uses
// workflow.NewNexusClient to invoke the operation. The caller workflow itself is
// opaque about what target it's querying; that's wired into the handler.
func runViaCallerWF(s *NexusGetWorkflowResultTestSuite, spec *targetWFSpec) {
	env := newGetResultNexusEnv(s.T())
	cfg := newGetResultNexusConfig(s.T())
	origRunID := setupTargetWFAndWaitReady(s, env, cfg, spec)

	var pinRunID string
	if spec.DoCAN {
		pinRunID = origRunID
	}
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(),
		makeGetResultHandlerFor(env, cfg.targetWfID, pinRunID))

	callerRun, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWFName, endpointName)
	s.NoError(err)

	if spec.BlockOnSignal {
		// Wait for the API to register the callback on the target, then allow the target workflow to complete
		// via the completion signal.
		s.EventuallyWithT(func(t *assert.CollectT) {
			hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: cfg.targetWfID})
			for _, e := range hist {
				if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
					return
				}
			}
			require.Fail(t, "OPTIONS_UPDATED not yet appended to target history")
		}, 10*time.Second, 100*time.Millisecond)
		s.NoError(env.SdkClient().SignalWorkflow(env.Context(), cfg.targetWfID, "", "complete", nil))
	}

	var result string
	err = callerRun.Get(env.Context(), &result)
	if spec.Fail {
		s.Error(err)
		s.Contains(err.Error(), "intentional target workflow failure")
	} else {
		s.NoError(err)
		s.Equal(spec.Value, result)
	}
	s.assertTargetWFHistory(env, cfg.targetWfID, spec)
}

// runViaStandalone runs the scenario via StartNexusOperationExecution from the test
// goroutine (no caller workflow).
func runViaStandalone(s *NexusGetWorkflowResultTestSuite, spec *targetWFSpec) {
	env := newStandaloneEnv(s.T())
	cfg := newGetResultNexusConfig(s.T())

	origRunID := setupTargetWFAndWaitReady(s, env, cfg, spec)

	var pinRunID string
	if spec.DoCAN {
		pinRunID = origRunID
	}
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(),
		makeGetResultHandlerFor(env, cfg.targetWfID, pinRunID))

	opID := testcore.RandomizeStr("op")
	resp, err := env.startStandaloneNexusOp(&workflowservice.StartNexusOperationExecutionRequest{
		OperationId: opID,
		Endpoint:    endpointName,
	})
	s.NoError(err)

	if spec.BlockOnSignal {
		// Wait until the handler returned async (callback registered), then allow the target workflow to complete
		// via the completion signal.
		_, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: opID,
			RunId:       resp.GetRunId(),
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
		})
		s.NoError(err)
		s.NoError(env.SdkClient().SignalWorkflow(env.Context(), cfg.targetWfID, "", "complete", nil))
	}

	pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: opID,
		RunId:       resp.GetRunId(),
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
	})
	s.NoError(err)

	if spec.Fail {
		// If we spec-ed the target workflow to intentionally fail, expect the base cause of the failure to
		// be the intetional failure.
		s.NotNil(pollResp.GetFailure(), "expected operation to close with a failure")
		leafErr := pollResp.GetFailure()
		for leafErr.GetCause() != nil {
			leafErr = leafErr.GetCause()
		}
		protorequire.ProtoEqual(s.T(),
			&failurepb.Failure{Message: intentionalFailureMsg},
			leafErr,
			protorequire.IgnoreFields("source", "stack_trace", "encoded_attributes", "application_failure_info"),
		)
	} else {
		var result string
		s.NoError(payload.Decode(pollResp.GetResult(), &result))
		s.Equal(spec.Value, result)
	}
	s.assertTargetWFHistory(env, cfg.targetWfID, spec)
}

func (s *NexusGetWorkflowResultTestSuite) assertTargetWFHistory(
	env *NexusTestEnv, wfID string, spec *targetWFSpec,
) {
	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: wfID})
	if !spec.BlockOnSignal {
		// If the target workflow completed before the Nexus GetWorkflowExecutionResult op, then expect
		// no callbacks to be atttached.
		s.RequireNoHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)
	} else {
		// Otherwise, expect callbacks to be attached before we signaled the target workflow.
		optionsUpdatedEvent := s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)
		signaledEvent := s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
		s.Less(optionsUpdatedEvent.EventId, signaledEvent.EventId)
	}

	// The outcome of the workflow (via history event) should match what our spec dictated.
	if spec.Fail {
		s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
	} else {
		s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
	}
}
