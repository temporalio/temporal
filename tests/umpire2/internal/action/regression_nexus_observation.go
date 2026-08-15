package action

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

func (p *regressionPath) callbackReferenceSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	operationID, operationBound := bindingString(bindings, atom.Arguments[0].SymbolName)
	handlerID, handlerBound := bindingString(bindings, atom.Arguments[1].SymbolName)
	if !operationBound || !handlerBound {
		return false
	}
	for _, workflowID := range []string{operationID, handlerID} {
		if _, err := p.environment.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: p.environment.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		}); err != nil {
			return false
		}
	}
	return p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), umpirefw.ObservationQuery{
		Predicate: atom.Predicate,
		Arguments: []string{operationID, handlerID},
	})
}

func (p *regressionPath) lateStartResponseSatisfied(atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	operationID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	return p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), umpirefw.ObservationQuery{
		Predicate: atom.Predicate,
		Arguments: []string{operationID},
	})
}

func (p *regressionPath) terminalObservationSatisfied(
	ctx context.Context,
	atom coreregress.CompletedAtom,
	bindings coreregress.Bindings,
) bool {
	workflowID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	query := umpirefw.ObservationQuery{
		Predicate: atom.Predicate,
		Arguments: []string{workflowID, fmt.Sprint(atom.Arguments[1].Value)},
	}
	if p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), query) {
		return true
	}
	_, err := p.environment.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: p.environment.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	return err == nil && p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), query)
}

func (p *regressionPath) nexusStorageAbsent(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	workflowID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	if p.findEmptyNexusStorage(workflowID) {
		return true
	}
	response, err := p.environment.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: p.environment.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Archetype: chasm.WorkflowArchetype,
	})
	if err != nil || response.GetDatabaseMutableState() == nil {
		return false
	}
	observed := fact.NewWorkflowNexusStorageSnapshot(
		p.environment.NamespaceID().String(),
		workflowID,
		response.GetDatabaseMutableState(),
	)
	if observer, ok := p.environment.GetMonitor().(factObserver); ok {
		if err := observer.ObserveFact(ctx, observed); err != nil {
			return false
		}
	}
	return len(observed.OperationIDs) == 0
}

func (p *regressionPath) findEmptyNexusStorage(workflowID string) bool {
	return p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), umpirefw.ObservationQuery{
		Predicate: "workflow.nexus_storage_absent",
		Arguments: []string{workflowID},
	})
}

func (p *regressionPath) handlerWorkflowRelationSatisfied(atom coreregress.CompletedAtom, bindings coreregress.Bindings) bool {
	operationID, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	handlerID, ok := bindingString(bindings, atom.Arguments[1].SymbolName)
	if !ok {
		return false
	}
	return p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), umpirefw.ObservationQuery{
		Predicate: atom.Predicate,
		Arguments: []string{operationID, handlerID},
	})
}

func (p *regressionPath) standaloneCancellationFailed(ctx context.Context, operationID string) bool {
	response, err := p.environment.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
		Namespace:   p.environment.Namespace().String(),
		OperationId: operationID,
		RunId:       p.context.RunID,
	})
	return err == nil && response.GetInfo().GetCancellationInfo().GetState() == enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED
}

func (p *regressionPath) historyContainsNexusEvent(ctx context.Context, workflowID string, eventType enumspb.EventType) bool {
	response, err := p.environment.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: p.environment.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	if err != nil {
		return false
	}
	for _, event := range response.GetHistory().GetEvents() {
		if event.GetEventType() == eventType {
			return true
		}
	}
	return false
}
