package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2/model"
	regressnexus "go.temporal.io/server/tests/umpire2/regress/nexus"
)

func (p *regressionPath) Reconcile(ctx context.Context, step coreregress.CompletedStep, bindings coreregress.Bindings) error {
	return p.awaitAtoms(ctx, step.Effects, bindings, true)
}

func (p *regressionPath) Observe(ctx context.Context, milestone coreregress.CompletedMilestone, bindings coreregress.Bindings) error {
	arguments := append([]coreregress.Argument(nil), milestone.Arguments...)
	if milestone.Kind == coreregress.BindingKind {
		arguments = append(arguments, coreregress.Symbol(milestone.Binding))
	}
	return p.awaitAtoms(ctx, []coreregress.CompletedAtom{{Predicate: milestone.Name, Arguments: arguments}}, bindings, true)
}

func (p *regressionPath) CheckSafety(ctx context.Context, _ coreregress.Checkpoint) error {
	if err := p.refreshLinkedExecutions(ctx); err != nil {
		return err
	}
	return violationsError(p.environment.GetMonitor().CheckNamespaceSafety(ctx, p.environment.NamespaceID().String()))
}

func (p *regressionPath) ArtifactFacts(_ context.Context) ([]json.RawMessage, error) {
	root := umpirefw.NewEntityID(model.NamespaceType, p.environment.NamespaceID().String())
	facts := p.environment.GetMonitor().FactLog().QueryByID(root)
	result := make([]json.RawMessage, 0, len(facts))
	for _, observed := range facts {
		payload, err := json.Marshal(observed)
		if err != nil {
			return nil, fmt.Errorf("encode observed fact %s: %w", observed.Name(), err)
		}
		encoded, err := json.Marshal(struct {
			Name    string               `json:"name"`
			Target  *umpirefw.EntityPath `json:"target,omitempty"`
			Payload json.RawMessage      `json:"payload"`
		}{
			Name:    observed.Name(),
			Target:  observed.TargetEntity(),
			Payload: payload,
		})
		if err != nil {
			return nil, fmt.Errorf("encode observed fact artifact %s: %w", observed.Name(), err)
		}
		result = append(result, encoded)
	}
	return result, nil
}

func (p *regressionPath) refreshLinkedExecutions(ctx context.Context) error {
	p.mu.RLock()
	pairs := make(map[string]string, len(p.activityOps))
	runs := make(map[string]string, len(p.activityRuns))
	for activityID, operationID := range p.activityOps {
		pairs[activityID] = operationID
		runs[activityID] = p.activityRuns[activityID]
	}
	p.mu.RUnlock()
	for activityID, operationID := range pairs {
		if _, err := p.environment.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  p.environment.Namespace().String(),
			ActivityId: activityID,
			RunId:      runs[activityID],
		}); err != nil {
			return fmt.Errorf("refresh linked activity %s: %w", activityID, err)
		}
		if _, err := p.environment.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   p.environment.Namespace().String(),
			OperationId: operationID,
			RunId:       p.context.RunID,
		}); err != nil {
			return fmt.Errorf("refresh linked Nexus operation %s: %w", operationID, err)
		}
	}
	return nil
}

func (p *regressionPath) Quiesce(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	generation := p.environment.GetMonitor().ModelState().Generation()
	stable := 0
	for stable < 3 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			current := p.environment.GetMonitor().ModelState().Generation()
			if current == generation {
				stable++
			} else {
				generation = current
				stable = 0
			}
		}
	}
	return nil
}

func (p *regressionPath) ResolveLiveness(ctx context.Context) error {
	return violationsError(p.environment.GetMonitor().CheckNamespace(ctx, p.environment.NamespaceID().String()))
}

func (p *regressionPath) Close(ctx context.Context) error {
	p.context.Cleanup()
	p.environment.GetMonitor().PurgeNamespace(p.environment.NamespaceID().String())
	if p.cleanup != nil {
		return p.cleanup(ctx)
	}
	return nil
}

func violationsError(violations []umpirefw.Violation) error {
	var result []error
	for _, violation := range violations {
		result = append(result, fmt.Errorf("%s: %s", violation.Rule, violation.Message))
	}
	return errors.Join(result...)
}

func (p *regressionPath) awaitAtoms(ctx context.Context, atoms []coreregress.CompletedAtom, bindings coreregress.Bindings, historical bool) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		allSatisfied := true
		var missing []string
		for _, atom := range atoms {
			if !p.atomSatisfied(ctx, atom, bindings, historical) {
				allSatisfied = false
				missing = append(missing, semanticAtomKey(atom))
			}
		}
		if err := p.CheckSafety(ctx, coreregress.ObservationCheckpoint); err != nil {
			return fmt.Errorf("monitor safety during observation: %w", err)
		}
		if allSatisfied {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w waiting for %s", ctx.Err(), strings.Join(missing, ", "))
		case <-ticker.C:
		}
	}
}

func (p *regressionPath) atomSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings, historical bool) bool {
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
	root := umpirefw.NewEntityID(model.NamespaceType, p.environment.NamespaceID().String())
	for _, entry := range p.environment.GetMonitor().ModelState().QueryEntities(model.NexusOperationType, 0, &root) {
		operation, ok := entry.Entity.(*model.NexusOperation)
		if !ok || operation.WorkflowID != identity {
			continue
		}
		if semanticNexusStateReached(operation, state, historical) {
			return true
		}
	}
	return p.historyNexusStateReached(ctx, identity, state)
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

func (p *regressionPath) workflowStateSatisfied(ctx context.Context, atom coreregress.CompletedAtom, bindings coreregress.Bindings, historical bool) bool {
	identity, ok := bindingString(bindings, atom.Arguments[0].SymbolName)
	if !ok {
		return false
	}
	want := fmt.Sprint(atom.Arguments[1].Value)
	root := umpirefw.NewEntityID(model.NamespaceType, p.environment.NamespaceID().String())
	for _, entry := range p.environment.GetMonitor().ModelState().QueryEntities(model.WorkflowType, 0, &root) {
		entity, ok := entry.Entity.(*model.Workflow)
		if !ok || entity.WorkflowID != identity {
			continue
		}
		actual := model.WorkflowStarted
		if want == "completed" {
			actual = model.WorkflowCompleted
		}
		if historical {
			_, reached := entity.FSM.EnteredAt(actual)
			if reached {
				return true
			}
			continue
		}
		if entity.FSM.Current() == actual {
			return true
		}
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

func semanticNexusStateReached(operation *model.NexusOperation, state string, historical bool) bool {
	actual := ""
	switch state {
	case string(regressnexus.Scheduled):
		actual = model.NexusScheduled
	case string(regressnexus.Started):
		actual = model.NexusStarted
	case string(regressnexus.Completed):
		actual = model.NexusSucceeded
	case string(regressnexus.Canceled):
		actual = model.NexusCanceled
	case string(regressnexus.TimedOut):
		actual = model.NexusTimedOut
	default:
		return false
	}
	if historical {
		_, reached := operation.FSM.EnteredAt(actual)
		return reached
	}
	return operation.FSM.Current() == actual
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
