package kitchensink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"
)

type ClientActionsExecutor struct {
	Client          client.Client
	Namespace       string
	WorkflowOptions client.StartWorkflowOptions
	WorkflowType    string
	WorkflowInput   *WorkflowInput
	Handle          client.WorkflowRun
	runID           string
}

func (e *ClientActionsExecutor) Start(
	ctx context.Context,
	withStartAction *WithStartClientAction,
) error {
	var err error
	if withStartAction == nil {
		e.Handle, err = e.Client.ExecuteWorkflow(ctx, e.WorkflowOptions, e.WorkflowType, e.WorkflowInput)
	} else if sig := withStartAction.GetDoSignal(); sig != nil {
		e.Handle, err = e.executeSignalAction(ctx, sig)
	} else if upd := withStartAction.GetDoUpdate(); upd != nil {
		e.Handle, err = e.executeUpdateAction(ctx, upd)
	} else {
		return fmt.Errorf("unsupported with_start_action: %v", withStartAction.String())
	}
	if err != nil {
		return fmt.Errorf("failed to start kitchen sink workflow: %w", err)
	}
	if e.Handle != nil { // can be nil if FailureExpected is set
		e.runID = e.Handle.GetRunID()
	}
	return nil
}

func (e *ClientActionsExecutor) ExecuteClientSequence(ctx context.Context, clientSeq *ClientSequence) error {
	for _, actionSet := range clientSeq.ActionSets {
		if err := e.executeClientActionSet(ctx, actionSet); err != nil {
			return err
		}
	}
	return nil
}

func (e *ClientActionsExecutor) executeClientActionSet(ctx context.Context, actionSet *ClientActionSet) error {
	errs, errGroupCtx := errgroup.WithContext(ctx)
	for _, action := range actionSet.Actions {
		if actionSet.Concurrent {
			action := action
			errs.Go(func() error {
				err := e.executeClientAction(errGroupCtx, action)
				if err != nil {
					return fmt.Errorf("failed to execute concurrent client action %v: %w", action, err)
				}
				return nil
			})
		} else {
			if err := e.executeClientAction(ctx, action); err != nil {
				return fmt.Errorf("failed to execute client action %v: %w", action, err)
			}
		}
	}
	if actionSet.Concurrent {
		if err := errs.Wait(); err != nil {
			return err
		}
		if actionSet.WaitAtEnd != nil {
			select {
			case <-time.After(actionSet.WaitAtEnd.AsDuration()):
			case <-ctx.Done():
				return fmt.Errorf("context done while waiting for end %w", ctx.Err())
			}
		}
	}
	if actionSet.GetWaitForCurrentRunToFinishAtEnd() {
		err := e.Client.GetWorkflow(ctx, e.WorkflowOptions.ID, e.runID).
			GetWithOptions(ctx, nil, client.WorkflowRunGetOptions{DisableFollowingRuns: true})
		var canErr *workflow.ContinueAsNewError
		if err != nil && !errors.As(err, &canErr) {
			return err
		}
		e.runID = e.Client.GetWorkflow(ctx, e.WorkflowOptions.ID, "").GetRunID()
	}
	return nil
}

// Run a specific client action -
func (e *ClientActionsExecutor) executeClientAction(ctx context.Context, action *ClientAction) error {
	if action.Variant == nil {
		return fmt.Errorf("client action variant must be set")
	}

	var err error
	if sig := action.GetDoSignal(); sig != nil {
		_, err = e.executeSignalAction(ctx, sig)
		return err
	} else if update := action.GetDoUpdate(); update != nil {
		_, err = e.executeUpdateAction(ctx, update)
		return err
	} else if query := action.GetDoQuery(); query != nil {
		if query.GetReportState() != nil {
			// TODO: Use args
			_, err = e.Client.QueryWorkflow(ctx, e.WorkflowOptions.ID, "", "report_state", nil)
		} else if handler := query.GetCustom(); handler != nil {
			_, err = e.Client.QueryWorkflow(ctx, e.WorkflowOptions.ID, "", handler.Name, handler.Args)
		} else {
			return fmt.Errorf("do_query must recognizable variant")
		}
		if query.FailureExpected {
			err = nil
		}
		return err
	} else if action.GetDoDescribe() != nil {
		_, err = e.Client.DescribeWorkflowExecution(ctx, e.WorkflowOptions.ID, "")
		return err
	} else if action.GetNestedActions() != nil {
		err = e.executeClientActionSet(ctx, action.GetNestedActions())
		return err
	} else if sano := action.GetDoStandaloneNexusOperation(); sano != nil {
		return e.executeStandaloneNexusOperation(ctx, sano)
	} else if sa := action.GetDoStandaloneActivity(); sa != nil {
		return e.executeStandaloneActivity(ctx, sa)
	} else {
		return fmt.Errorf("client action must be set")
	}
}

func (e *ClientActionsExecutor) executeSignalAction(ctx context.Context, sig *DoSignal) (client.WorkflowRun, error) {
	var signalName string
	var signalArgs any
	if sigActions := sig.GetDoSignalActions(); sigActions != nil {
		signalName = "do_actions_signal"
		signalArgs = sigActions
	} else if handler := sig.GetCustom(); handler != nil {
		signalName = handler.Name
		signalArgs = handler.Args
	} else {
		return nil, fmt.Errorf("do_signal must recognizable variant")
	}

	if sig.WithStart {
		return e.Client.SignalWithStartWorkflow(
			ctx, e.WorkflowOptions.ID, signalName, signalArgs, e.WorkflowOptions, e.WorkflowType, e.WorkflowInput)
	}
	return nil, e.Client.SignalWorkflow(ctx, e.WorkflowOptions.ID, "", signalName, signalArgs)
}

func (e *ClientActionsExecutor) executeUpdateAction(ctx context.Context, upd *DoUpdate) (run client.WorkflowRun, err error) {
	var updateOpts client.UpdateWorkflowOptions
	if actionsUpdate := upd.GetDoActions(); actionsUpdate != nil {
		updateOpts = client.UpdateWorkflowOptions{
			WorkflowID:   e.WorkflowOptions.ID,
			UpdateName:   "do_actions_update",
			WaitForStage: client.WorkflowUpdateStageCompleted,
			Args:         []any{actionsUpdate},
		}
	} else if handler := upd.GetCustom(); handler != nil {
		updateOpts = client.UpdateWorkflowOptions{
			WorkflowID:   e.WorkflowOptions.ID,
			UpdateName:   handler.Name,
			WaitForStage: client.WorkflowUpdateStageCompleted,
			Args:         []any{handler.Args},
		}
	} else {
		return nil, fmt.Errorf("do_update must recognizable variant")
	}

	var handle client.WorkflowUpdateHandle
	if upd.WithStart {
		workflowOpts := e.WorkflowOptions
		workflowOpts.WorkflowIDConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
		op := e.Client.NewWithStartWorkflowOperation(workflowOpts, e.WorkflowType, e.WorkflowInput)
		handle, err = e.Client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
			StartWorkflowOperation: op,
			UpdateOptions:          updateOpts,
		})
	} else {
		handle, err = e.Client.UpdateWorkflow(ctx, updateOpts)
	}

	if err == nil {
		err = handle.Get(ctx, nil)
		if upd.WithStart {
			run = e.Client.GetWorkflow(ctx, handle.WorkflowID(), handle.RunID())
		}
	}
	if upd.FailureExpected {
		err = nil
	}
	return run, err
}

func (e *ClientActionsExecutor) executeStandaloneNexusOperation(_ context.Context, _ *DoStandaloneNexusOperation) error {
	// Adjusted for the temporal-server SDK (v1.41.1), which predates the client-side
	// Nexus operation API (NewNexusClient/ExecuteOperation). Client-driven standalone
	// Nexus operations are unsupported here; the Nexus operation *lifecycle* is still
	// exercised via workflow-scheduled operations (the NexusOperation entity/model).
	return fmt.Errorf("kitchensink: client-side standalone Nexus operations unsupported on this SDK version")
}

func (e *ClientActionsExecutor) executeStandaloneActivity(ctx context.Context, sa *DoStandaloneActivity) error {
	act := sa.GetActivity()
	if act == nil {
		return fmt.Errorf("DoStandaloneActivity.activity is required")
	}

	actType, args := ActivityNameAndArgs(act)

	handle, err := e.Client.ExecuteActivity(ctx, client.StartActivityOptions{
		ID:                     fmt.Sprintf("standalone-activity-%s-%s", e.WorkflowOptions.ID, uuid.NewString()),
		TaskQueue:              act.TaskQueue,
		ScheduleToCloseTimeout: act.ScheduleToCloseTimeout.AsDuration(),
		ScheduleToStartTimeout: act.ScheduleToStartTimeout.AsDuration(),
		StartToCloseTimeout:    act.StartToCloseTimeout.AsDuration(),
		HeartbeatTimeout:       act.HeartbeatTimeout.AsDuration(),
		RetryPolicy:            ConvertFromPBRetryPolicy(act.RetryPolicy),
	}, actType, args...)
	if err != nil {
		return fmt.Errorf("failed to start standalone activity: %w", err)
	}
	return handle.Get(ctx, nil)
}
