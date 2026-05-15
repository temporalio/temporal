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

// =============================================================================
// API-level rejection tests (the handler never reaches a target workflow).
// =============================================================================

type NexusGetWorkflowResultFailuresSuite struct {
	parallelsuite.Suite[*NexusGetWorkflowResultFailuresSuite]
}

func TestNexusGetWorkflowResultFailuresSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusGetWorkflowResultFailuresSuite{})
}

// TestGetResult_FeatureFlagDisabled: with the feature flag off, the API rejects the
// call with PermissionDenied; the handler propagates this as a Nexus operation error.
func (s *NexusGetWorkflowResultFailuresSuite) TestGetResult_FeatureFlagDisabled() {
	// Feature flag intentionally NOT enabled here.
	env := newNexusTestEnv(
		s.T(),
		true,
		testcore.WithDynamicConfig(callbacks.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
	)
	cfg := newNexusGetWorkflowResultTestConfig(s.T())

	// Any wfID works - the API rejects on the feature flag before looking up the workflow.
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			resp, err := env.FrontendClient().GetWorkflowExecutionResult(ctx, &workflowservice.GetWorkflowExecutionResultRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: cfg.targetWfID},
				RequestId: uuid.NewString(),
				Identity:  "test",
				CompletionCallbacks: []*commonpb.Callback{{Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
				}}},
			})
			if err != nil {
				return nil, &nexus.OperationError{State: nexus.OperationStateFailed, Message: err.Error()}
			}
			return nil, &nexus.OperationError{State: nexus.OperationStateFailed, Message: fmt.Sprintf("unexpected response %v", resp)}
		},
	})

	// Register nexus-wrapped workflow on the caller namespace.
	w := worker.New(env.SdkClient(), cfg.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, endpointName string) (string, error) {
			nexusClient := workflow.NewNexusClient(endpointName, "test")
			fut := nexusClient.ExecuteOperation(ctx, "get-result", nil, workflow.NexusOperationOptions{})
			var result string
			err := fut.Get(ctx, &result)
			return result, err
		},
		workflow.RegisterOptions{Name: cfg.callerWFName},
	)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, cfg.callerWFName, endpointName)
	s.NoError(err)

	err = run.Get(env.Context(), nil)
	s.Error(err)
	s.Contains(err.Error(), "GetWorkflowExecutionResult operation is disabled")

	callerHist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID()})
	s.RequireHistoryEvent(callerHist, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED)
}

// TestGetResult_InvalidExecution: handler invokes the API with an empty workflow ID,
// which triggers InvalidArgument; the handler propagates this as a Nexus error.
func (s *NexusGetWorkflowResultFailuresSuite) TestGetResult_InvalidExecution() {
	env := newNexusTestEnv(
		s.T(),
		true,
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableGetWorkflowExecutionResult, true),
		testcore.WithDynamicConfig(callbacks.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
	)
	cfg := newNexusGetWorkflowResultTestConfig(s.T())

	// Empty wfID baked into the handler - the API rejects with InvalidArgument.
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			_, err := env.FrontendClient().GetWorkflowExecutionResult(ctx, &workflowservice.GetWorkflowExecutionResultRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: ""},
				RequestId: uuid.NewString(),
				Identity:  "test",
				CompletionCallbacks: []*commonpb.Callback{{Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
				}}},
			})
			if err != nil {
				return nil, &nexus.OperationError{State: nexus.OperationStateFailed, Message: err.Error()}
			}
			return nil, &nexus.OperationError{State: nexus.OperationStateFailed, Message: "expected error"}
		},
	})

	// Register workflow in caller's namespace.
	w := worker.New(env.SdkClient(), cfg.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, endpointName string) (string, error) {
			nexusClient := workflow.NewNexusClient(endpointName, "test")
			fut := nexusClient.ExecuteOperation(ctx, "get-result", nil, workflow.NexusOperationOptions{})
			var result string
			err := fut.Get(ctx, &result)
			return result, err
		},
		workflow.RegisterOptions{Name: cfg.callerWFName},
	)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, cfg.callerWFName, endpointName)
	s.NoError(err)

	err = run.Get(env.Context(), nil)
	s.Error(err)
	s.Contains(err.Error(), "WorkflowId is not set")

	callerHist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID()})
	s.RequireHistoryEvent(callerHist, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED)
}

// =============================================================================
// Parameterized tests:
// tests all of (CAN|NonCAN) * (Sync|Async) * (Success|Failure) combinations,
// each via both ViaCallerWF and ViaStandalone.
// =============================================================================

type NexusGetWorkflowResultSpecSuite struct {
	parallelsuite.Suite[*NexusGetWorkflowResultSpecSuite]
}

func TestNexusGetWorkflowResultSpecSuite(t *testing.T) {
	bools := []bool{true, false}
	for _, doCAN := range bools {
		for _, blockOnSignal := range bools {
			for _, fail := range bools {
				spec := &targetWFSpec{Value: "input", DoCAN: doCAN, BlockOnSignal: blockOnSignal, Fail: fail}
				t.Run(spec.name(), func(t *testing.T) {
					parallelsuite.Run(t, &NexusGetWorkflowResultSpecSuite{}, spec)
				})
			}
		}
	}
}

func (s *NexusGetWorkflowResultSpecSuite) newTestEnv(opts ...testcore.TestOption) *NexusTestEnv {
	allOpts := append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableGetWorkflowExecutionResult, true),
		testcore.WithDynamicConfig(callbacks.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
	}, opts...)
	return newNexusTestEnv(s.T(), true, allOpts...)
}

func (s *NexusGetWorkflowResultSpecSuite) makeGetResultHandler(env *NexusTestEnv, workflowID string) nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			resp, err := env.FrontendClient().GetWorkflowExecutionResult(
				ctx, &workflowservice.GetWorkflowExecutionResultRequest{
					Namespace: env.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
					RequestId: uuid.NewString(),
					Identity:  "test",
					CompletionCallbacks: []*commonpb.Callback{
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
			if running := resp.GetRunning(); running != nil {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "get-result-async"}, nil
			}
			if success := resp.GetSuccess(); success != nil {
				result := success.GetResult()
				var s string
				if err := json.Unmarshal(result.GetPayloads()[0].GetData(), &s); err == nil {
					return &nexus.HandlerStartOperationResultSync[any]{Value: s}, nil
				}
			}
			if failed := resp.GetFailed(); failed != nil {
				return nil, &nexus.OperationError{
					State: nexus.OperationStateFailed, Message: failed.GetFailure().GetMessage(),
				}
			}
			return nil, &nexus.OperationError{
				State: nexus.OperationStateFailed, Message: fmt.Sprintf("Unknown response %v", resp),
			}
		},
	}
}

func (s *NexusGetWorkflowResultSpecSuite) TestViaCallerWF(spec *targetWFSpec) {
	env := s.newTestEnv()
	cfg := newNexusGetWorkflowResultTestConfig(s.T())
	endpointName := env.createRandomExternalNexusServer(
		env.Context(), s.T(), s.makeGetResultHandler(env, cfg.targetWfID),
	)

	s.setupTargetWFAndWaitReady(env, cfg, spec, false)
	callerRun, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, cfg.callerWFName, endpointName)
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

func (s *NexusGetWorkflowResultSpecSuite) TestViaStandalone(spec *targetWFSpec) {
	env := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
	)
	cfg := newNexusGetWorkflowResultTestConfig(s.T())
	endpointName := env.createRandomExternalNexusServer(
		env.Context(), s.T(), s.makeGetResultHandler(env, cfg.targetWfID),
	)

	s.setupTargetWFAndWaitReady(env, cfg, spec, true)
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
		// be the intentional failure.
		s.NotNil(pollResp.GetFailure(), "expected operation to close with a failure")
		leafErr := pollResp.GetFailure()
		for leafErr.GetCause() != nil {
			leafErr = leafErr.GetCause()
		}
		protorequire.ProtoEqual(s.T(),
			&failurepb.Failure{Message: "intentional target workflow failure"},
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

func (s *NexusGetWorkflowResultSpecSuite) setupTargetWFAndWaitReady(
	env *NexusTestEnv,
	cfg *nexusGetWorkflowResultConfig,
	spec *targetWFSpec,
	isStandaloneNexusOp bool,
) {
	w := worker.New(env.SdkClient(), cfg.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(spec.targetWFFromSpec(), workflow.RegisterOptions{Name: cfg.targetWFName})

	// We only need to register the caller's nexus-wrapped workflow if this is not a standalone nexus op test.
	if !isStandaloneNexusOp {
		w.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, endpointName string) (string, error) {
				nexusClient := workflow.NewNexusClient(endpointName, "test")
				fut := nexusClient.ExecuteOperation(ctx, "get-result", nil, workflow.NexusOperationOptions{})
				var result string
				err := fut.Get(ctx, &result)
				return result, err
			},
			workflow.RegisterOptions{Name: cfg.callerWFName},
		)
	}
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		ID:                       cfg.targetWfID,
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, cfg.targetWFName, spec.Value)
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
				// If we want to exercise a CAN-ed workflow, wait until the workflow continues-as-new
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
}

func (s *NexusGetWorkflowResultSpecSuite) assertTargetWFHistory(
	env *NexusTestEnv, wfID string, spec *targetWFSpec,
) {
	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: wfID})
	if !spec.BlockOnSignal {
		// If the target workflow completed before the Nexus GetWorkflowExecutionResult op, then expect
		// no callbacks to be attached.
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

// =============================================================================
// Shared boilerplate
// =============================================================================

// nexusGetWorkflowResultConfig holds randomized identifiers for one test run.
type nexusGetWorkflowResultConfig struct {
	taskQueue  string
	targetWfID string

	// These consts are used by any tests that have these configs.
	targetWFName string
	callerWFName string
}

func newNexusGetWorkflowResultTestConfig(t *testing.T) *nexusGetWorkflowResultConfig {
	return &nexusGetWorkflowResultConfig{
		taskQueue:  testcore.RandomizeStr(t.Name()),
		targetWfID: testcore.RandomizeStr("target-workflow-id"),

		targetWFName: "get-result-target-wf",
		callerWFName: "get-result-caller-wf",
	}
}

// targetWFSpec parameterizes the target workflow's behavior so we can parameterize
// tests to exercise permutations of cases on the target workflow.
type targetWFSpec struct {
	Value         string `json:"value"`
	DoCAN         bool   `json:"doCAN"`         // if true, target workflow continues-as-new once
	BlockOnSignal bool   `json:"blockOnSignal"` // if true, target workflow waits for a "complete" signal before finishing
	Fail          bool   `json:"fail"`          // if true, the target workflow returns an error
}

func (spec *targetWFSpec) name() string {
	can := "NonCAN"
	if spec.DoCAN {
		can = "CAN"
	}
	sync := "Sync"
	if spec.BlockOnSignal {
		sync = "Async"
	}
	outcome := "Success"
	if spec.Fail {
		outcome = "Failure"
	}
	return fmt.Sprintf("%s_%s_%s", can, sync, outcome)
}

// targetWFFromSpec returns a target workflow based on conditionals from targetWFSpec.
func (spec *targetWFSpec) targetWFFromSpec() func(workflow.Context, string) (string, error) {
	return func(ctx workflow.Context, input string) (string, error) {
		// Check the ContinuedExecutionRunID() so we don't CAN indefinitely.
		info := workflow.GetInfo(ctx)
		if spec.DoCAN && info.ContinuedExecutionRunID == "" {
			return "", workflow.NewContinueAsNewError(ctx, info.WorkflowType.Name, input)
		}

		if spec.BlockOnSignal {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		}
		if spec.Fail {
			return "", errors.New("intentional target workflow failure")
		}
		return input, nil
	}
}
