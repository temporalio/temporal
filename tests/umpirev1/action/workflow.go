package action

import (
	"context"
	"fmt"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpirev1/model"
	"go.temporal.io/server/tests/umpirev1/planner"
)

// Workflow is the second entity driven by the actions model — proof the framework/registration
// split generalizes past NexusOperation. Nothing in the generic layer (Drive/Reconcile/planner) or
// the entity-agnostic planEdge changes; a second entity is just its lifecycle (already modelled by
// the Monitor) plus an actionFor registry and realizers. The Workflow lifecycle is minimal
// (created→started→completed); richer edges (fail/cancel/terminate/timeout) are a follow-up on both
// the model and these actions.

// completableWorkflow blocks until a "finish" signal, then returns — so a workflow's start and its
// completion are two separately drivable transitions (WorkflowStart, WorkflowComplete), matching
// the two model edges.
func completableWorkflow(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return nil
}

type startWorkflow struct{}

func (startWorkflow) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (startWorkflow) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	wfID := fmt.Sprintf("umpire-action-wf-%d", c.Iter)
	c.Env.SdkWorker().RegisterWorkflow(completableWorkflow)
	if _, err := c.Env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: c.Env.WorkerTaskQueue(),
	}, completableWorkflow); err != nil {
		return err
	}
	bindFresh(rc, a, wfID) // the workflow entity is keyed by its own id
	return nil
}

type completeWorkflow struct{}

func (completeWorkflow) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (completeWorkflow) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	wfID, ok := rc.Binding("wf")
	if !ok {
		return fmt.Errorf("completeWorkflow: workflow not bound")
	}
	return c.Env.SdkClient().SignalWorkflow(ctx, wfID, "", "finish", nil)
}

// selfCompletingWorkflow returns immediately; its completion is the single transition the Monitor
// observes for a run — the completion span carries the RunID, so it grounds the run-precise
// WorkflowRun entity (created→completed). A run-level `started` awaits observing WorkflowStart (the
// frontend StartWorkflowExecution reaches the Monitor, but the WorkflowStarted fact decodes the
// history request the frontend interceptor does not see; see UMPIRE_ACTIONS.md).
func selfCompletingWorkflow(workflow.Context) error { return nil }

type runWorkflow struct{}

func (runWorkflow) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (runWorkflow) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	wfID := fmt.Sprintf("umpire-action-wf-%d", c.Iter)
	c.Env.SdkWorker().RegisterWorkflow(selfCompletingWorkflow)
	run, err := c.Env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: c.Env.WorkerTaskQueue(),
	}, selfCompletingWorkflow)
	if err != nil {
		return err
	}
	bindFresh(rc, a, run.GetRunID()) // the run entity is keyed by RunID
	return nil
}

func runRef(fresh bool) umpire.Ref {
	return runVarRef("run", fresh, "")
}

func runVarRef(varName string, fresh bool, linkedFrom string) umpire.Ref {
	return umpire.Ref{Type: model.WorkflowRunType, Var: varName, Fresh: fresh, LinkedFrom: linkedFrom}
}

// RunWorkflow starts a self-completing workflow execution; the run-precise WorkflowRun entity is
// observed through its full lifecycle — created→started (start span) →completed (completion span).
var RunWorkflow = umpire.Action{
	Name: "StartWorkflowExecution(self-completing)", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
	Effects: []umpire.Effect{
		{Ref: runRef(true), Event: model.WorkflowRunStart},
		{Ref: runRef(false), Event: model.WorkflowRunComplete},
	},
	Entry:   []string{"StartWorkflowExecution"},
	Realize: runWorkflow{},
}

// WorkflowRunPlan is the named plan that drives a workflow execution to completed.
func WorkflowRunPlan() []umpire.Action { return []umpire.Action{RunWorkflow} }

// continueAsNewOnceWorkflow continues-as-new once, then completes — one WorkflowID, two runs.
func continueAsNewOnceWorkflow(ctx workflow.Context, done bool) error {
	if done {
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, continueAsNewOnceWorkflow, true)
}

type runContinueAsNew struct{}

func (runContinueAsNew) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (runContinueAsNew) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	wfID := fmt.Sprintf("umpire-action-can-%d", c.Iter)
	c.Env.SdkWorker().RegisterWorkflow(continueAsNewOnceWorkflow)
	run, err := c.Env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: c.Env.WorkerTaskQueue(),
	}, continueAsNewOnceWorkflow, false)
	if err != nil {
		return err
	}
	bindFresh(rc, a, run.GetRunID()) // binds the predecessor "run"; the successor "run2" is bound by observation
	return nil
}

// RunContinueAsNew drives a continue-as-new chain and reconciles both runs: the predecessor
// (created→started→continued_as_new) and its successor (created→started→completed), the latter's
// ref bound by observation — the driver never supplies the server-minted successor RunID.
var RunContinueAsNew = umpire.Action{
	Name: "StartWorkflowExecution(continue-as-new)", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
	Effects: []umpire.Effect{
		{Ref: runVarRef("run", true, ""), Event: model.WorkflowRunStart},
		{Ref: runVarRef("run", false, ""), Event: model.WorkflowRunContinueAsNew},
		{Ref: runVarRef("run2", true, "run"), Event: model.WorkflowRunStart},
		{Ref: runVarRef("run2", false, "run"), Event: model.WorkflowRunComplete},
	},
	Entry:   []string{"StartWorkflowExecution"},
	Realize: runContinueAsNew{},
}

// WorkflowContinueAsNewPlan drives and reconciles a continue-as-new run graph (predecessor +
// successor).
func WorkflowContinueAsNewPlan() []umpire.Action { return []umpire.Action{RunContinueAsNew} }

func wfRef(fresh bool) umpire.Ref {
	return umpire.Ref{Type: model.WorkflowType, Var: "wf", Fresh: fresh}
}

var (
	// StartWorkflow starts the completable workflow (created→started).
	StartWorkflow = umpire.Action{
		Name: "StartWorkflowExecution", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
		Effects: []umpire.Effect{{Ref: wfRef(true), Event: model.WorkflowStart}},
		Entry:   []string{"StartWorkflowExecution"},
		Realize: startWorkflow{},
	}
	// CompleteWorkflow signals the workflow to finish (started→completed).
	CompleteWorkflow = umpire.Action{
		Name: "SignalWorkflowExecution(finish)", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
		Requires: []umpire.Pre{{Ref: wfRef(false), State: model.WorkflowStarted}},
		Effects:  []umpire.Effect{{Ref: wfRef(false), Event: model.WorkflowComplete}},
		Entry:    []string{"SignalWorkflowExecution"},
		Realize:  completeWorkflow{},
	}
)

// workflowActionFor is the Workflow model's event→action registry. Hosting is irrelevant (a
// workflow is top-level, driven directly), so it is ignored.
func workflowActionFor(from, event string, _ umpire.Hosting) (umpire.Action, bool) {
	switch event {
	case model.WorkflowStart:
		return StartWorkflow, true
	case model.WorkflowComplete:
		return CompleteWorkflow, true
	}
	return umpire.Action{}, false
}

func workflowLifecycle() (*umpire.Lifecycle, bool) {
	return planner.DefaultModels().Lifecycle(string(model.WorkflowType))
}

// WorkflowPlanEdge is planEdge for the Workflow model — the same entity-agnostic planner the Nexus
// PlanEdge uses, with the Workflow lifecycle and registry.
func WorkflowPlanEdge(from, event string) ([]umpire.Action, error) {
	lc, ok := workflowLifecycle()
	if !ok {
		return nil, fmt.Errorf("no Workflow lifecycle")
	}
	return planEdge(lc, workflowActionFor, from, event, umpire.Standalone)
}

// WorkflowAutoCoverPlans computes one plan per settling edge of the Workflow model, exactly as
// AutoCoverPlans does for NexusOperation — from the shared settlingEdgesFor core.
func WorkflowAutoCoverPlans() [][]umpire.Action {
	lc, ok := workflowLifecycle()
	if !ok {
		return nil
	}
	var plans [][]umpire.Action
	for _, e := range settlingEdgesFor(lc, workflowActionFor, umpire.Standalone) {
		if seq, err := planEdge(lc, workflowActionFor, e.from, e.event, e.hosting); err == nil {
			plans = append(plans, seq)
		}
	}
	return plans
}
