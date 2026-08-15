package action

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

func observeWorkflowRunID(
	atom coreregress.CompletedAtom,
	bindings coreregress.Bindings,
	response *workflowservice.DescribeWorkflowExecutionResponse,
) bool {
	if len(atom.Arguments) != 2 || response.GetWorkflowExecutionInfo().GetExecution() == nil {
		return false
	}
	runID := response.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	if runID == "" {
		return false
	}
	symbol := atom.Arguments[1].SymbolName
	if observed, exists := bindings[symbol]; exists {
		return observed == runID
	}
	bindings[symbol] = runID
	return true
}

func (p *regressionPath) describeActivity(ctx context.Context, symbol coreregress.Argument, bindings coreregress.Bindings) (*workflowservice.DescribeActivityExecutionResponse, bool) {
	activityID, ok := bindingString(bindings, symbol.SymbolName)
	if !ok {
		activityID, ok = p.context.Binding(symbol.SymbolName)
		if !ok {
			return nil, false
		}
		bindings[symbol.SymbolName] = activityID
	}
	p.mu.RLock()
	runID := p.activityRuns[activityID]
	p.mu.RUnlock()
	response, err := p.environment.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  p.environment.Namespace().String(),
		ActivityId: activityID,
		RunId:      runID,
	})
	return response, err == nil
}

func (p *regressionPath) activityStateSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	response, ok := p.describeActivity(ctx, atom.Arguments[0], bindings)
	return ok && fmt.Sprint(atom.Arguments[1].Value) == "completed" && response.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
}

func (p *regressionPath) activityLinkSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	response, ok := p.describeActivity(ctx, atom.Arguments[0], bindings)
	if !ok {
		return false
	}
	operationID, ok := bindingString(bindings, atom.Arguments[1].SymbolName)
	if !ok {
		return false
	}
	for _, link := range response.GetInfo().GetLinks() {
		operation := link.GetNexusOperation()
		if operation.GetNamespace() == p.environment.Namespace().String() && operation.GetOperationId() == operationID {
			return true
		}
	}
	return false
}

func (p *regressionPath) nexusActivityLinkSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	operationID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	activityID, ok := bindingString(bindings, atom.Arguments[1].SymbolName)
	if !ok {
		activityID, ok = p.context.Binding(atom.Arguments[1].SymbolName)
		if !ok {
			return false
		}
		bindings[atom.Arguments[1].SymbolName] = activityID
	}
	describe, err := p.environment.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
		Namespace:   p.environment.Namespace().String(),
		OperationId: operationID,
		RunId:       p.context.RunID,
	})
	if err == nil {
		for _, link := range describe.GetInfo().GetLinks() {
			activity := link.GetActivity()
			if activity.GetNamespace() == p.environment.Namespace().String() && activity.GetActivityId() == activityID {
				return true
			}
		}
	}
	response, err := p.environment.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: p.environment.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: operationID},
	})
	if err != nil {
		return false
	}
	for _, event := range response.GetHistory().GetEvents() {
		for _, link := range event.GetLinks() {
			activity := link.GetActivity()
			if activity.GetNamespace() == p.environment.Namespace().String() && activity.GetActivityId() == activityID {
				return true
			}
		}
	}
	return false
}
