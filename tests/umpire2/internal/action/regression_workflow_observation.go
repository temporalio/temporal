package action

import (
	"context"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	regressnexus "go.temporal.io/server/tests/umpire2/regress/nexus"
)

func (p *regressionPath) workflowStateSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings, historical bool) bool {
	identity, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	want := fmt.Sprint(atom.Arguments[1].Value)
	if p.environment.GetMonitor().Observed(p.environment.NamespaceID().String(), umpirefw.ObservationQuery{
		Predicate: atom.Predicate, Arguments: []string{identity, want}, Historical: historical,
	}) {
		return true
	}
	response, err := p.environment.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: p.environment.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: identity},
	})
	if err != nil {
		return false
	}
	closed := response.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	if want == "completed" {
		return closed
	}
	return historical || !closed || p.usesEmbeddedCaller()
}

func (p *regressionPath) historyNexusStateReached(ctx context.Context, workflowID, state string) bool {
	response, err := p.environment.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: p.environment.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	if err != nil {
		return false
	}
	wanted := map[string]enumspb.EventType{
		string(regressnexus.Scheduled): enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		string(regressnexus.Started):   enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		string(regressnexus.Completed): enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		string(regressnexus.Canceled):  enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		string(regressnexus.TimedOut):  enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
	}[state]
	for _, event := range response.GetHistory().GetEvents() {
		if event.GetEventType() == wanted {
			return true
		}
	}
	return false
}

func bindingString(bindings coreregress.Bindings, symbol string) (string, bool) {
	value, ok := bindings[symbol]
	if !ok {
		return "", false
	}
	identity, ok := value.(string)
	return identity, ok
}

func semanticAtomKey(atom coreregress.CompletedAtom) string {
	parts := make([]string, len(atom.Arguments))
	for index, argument := range atom.Arguments {
		if argument.Literal {
			parts[index] = fmt.Sprint(argument.Value)
		} else {
			parts[index] = "$" + argument.SymbolName
		}
	}
	return atom.Predicate + "(" + strings.Join(parts, ",") + ")"
}

func (p *regressionPath) semanticFactKey(predicate string, subject coreregress.Argument, value any) string {
	return semanticAtomKey(coreregress.CompletedAtom{Predicate: predicate, Arguments: []coreregress.Argument{subject, coreregress.Literal(value)}})
}
