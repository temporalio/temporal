package action

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

func (p *regressionPath) atomSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings, historical bool) bool {
	if atom.Predicate == "nexus.callback_reference_consistent" && len(atom.Arguments) == 2 {
		return p.callbackReferenceSatisfied(ctx, atom, bindings)
	}
	if atom.Predicate == "nexus.late_start_response_accepted" && len(atom.Arguments) == 1 {
		return p.lateStartResponseSatisfied(atom, bindings)
	}
	if atom.Predicate == "nexus.result_digest" && len(atom.Arguments) == 2 {
		return p.terminalObservationSatisfied(ctx, atom, bindings)
	}
	if atom.Predicate == "nexus.link_endpoint" && len(atom.Arguments) == 2 {
		return p.terminalObservationSatisfied(ctx, atom, bindings)
	}
	if atom.Predicate == "workflow.nexus_storage_absent" && len(atom.Arguments) == 2 {
		return p.nexusStorageAbsent(ctx, atom, bindings)
	}
	if atom.Predicate == "nexus.handler_workflow" && len(atom.Arguments) == 2 {
		return p.handlerWorkflowRelationSatisfied(atom, bindings)
	}
	if atom.Predicate == "nexus.cancel_request_failed" && len(atom.Arguments) == 1 {
		operationID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
		return ok && (p.standaloneCancellationFailed(ctx, operationID) || p.historyContainsNexusEvent(ctx, operationID, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED))
	}
	if atom.Predicate == "workflow.state" && len(atom.Arguments) == 2 {
		return p.workflowStateSatisfied(ctx, atom, bindings, historical)
	}
	if atom.Predicate == "workflow.run_id" && len(atom.Arguments) == 2 {
		workflowID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
		if !ok {
			return false
		}
		response, err := p.environment.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: p.environment.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		return err == nil && observeWorkflowRunID(atom, bindings, response)
	}
	if atom.Predicate == "activity.state" && len(atom.Arguments) == 2 {
		return p.activityStateSatisfied(ctx, atom, bindings)
	}
	if atom.Predicate == "activity.linked_to_nexus_operation" && len(atom.Arguments) == 2 {
		return p.activityLinkSatisfied(ctx, atom, bindings)
	}
	if atom.Predicate == "nexus.linked_to_activity" && len(atom.Arguments) == 2 {
		return p.nexusActivityLinkSatisfied(ctx, atom, bindings)
	}
	if atom.Predicate != "nexus.state" || len(atom.Arguments) != 2 {
		return p.localFacts[semanticAtomKey(atom)]
	}
	symbol := atom.Arguments[0].SymbolName
	identity, ok := bindingString(bindings, symbol)
	if !ok {
		return false
	}
	state := fmt.Sprint(atom.Arguments[1].Value)
	if p.localFacts[p.semanticFactKey(atom.Predicate, atom.Arguments[0], state)] {
		return true
	}
	if p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), umpirefw.ObservationQuery{
		Predicate: atom.Predicate, Arguments: []string{identity, state}, Historical: historical,
	}) {
		return true
	}
	return p.historyNexusStateReached(ctx, identity, state)
}
