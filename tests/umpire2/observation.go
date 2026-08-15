package umpire2

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// Observed reports whether a protocol-level semantic observation occurred in one namespace.
func (u *Monitor) Observed(namespaceID string, query umpirefw.ObservationQuery) bool {
	if len(query.Arguments) == 0 {
		return false
	}
	root := u.namespaceRoot(namespaceID)
	view := u.evidence.runtime.View(root)
	switch query.Predicate {
	case "workflow.state":
		if len(query.Arguments) != 2 {
			return false
		}
		for _, entry := range view.Entities(model.WorkflowType, 0) {
			workflow, ok := entry.Entity.(*model.Workflow)
			if !ok || workflow.WorkflowID != query.Arguments[0] {
				continue
			}
			if query.Historical {
				_, reached := workflow.FSM.EnteredAt(query.Arguments[1])
				return reached
			}
			return workflow.FSM.Current() == query.Arguments[1]
		}
	case "nexus.state":
		if len(query.Arguments) != 2 {
			return false
		}
		state, ok := nexusModelState(query.Arguments[1])
		if !ok {
			return false
		}
		for _, entry := range view.Entities(model.NexusOperationType, 0) {
			operation, ok := entry.Entity.(*model.NexusOperation)
			if !ok || operation.WorkflowID != query.Arguments[0] {
				continue
			}
			if query.Historical {
				_, reached := operation.FSM.EnteredAt(state)
				return reached
			}
			return operation.FSM.Current() == state
		}
	case "nexus.callback_reference_consistent":
		return len(query.Arguments) == 2 && u.callbackReferenceObserved(view, namespaceID, query.Arguments[0], query.Arguments[1])
	case "nexus.late_start_response_accepted":
		return len(query.Arguments) == 1 && u.lateStartResponseObserved(view, query.Arguments[0])
	case "nexus.result_digest":
		return len(query.Arguments) == 2 && u.terminalFactObserved(view, query.Arguments[0], func(observed *fact.NexusOperationTerminal) bool {
			return observed.ResultDigest == query.Arguments[1]
		})
	case "nexus.link_endpoint":
		return len(query.Arguments) == 2 && u.terminalFactObserved(view, query.Arguments[0], func(observed *fact.NexusOperationTerminal) bool {
			return slices.Contains(observed.LinkEndpoints, query.Arguments[1])
		})
	case "workflow.nexus_storage_absent":
		return len(query.Arguments) >= 1 && u.emptyNexusStorageObserved(view, query.Arguments[0])
	case "nexus.handler_workflow":
		return len(query.Arguments) == 2 && u.handlerWorkflowObserved(namespaceID, query.Arguments[0], query.Arguments[1])
	}
	return false
}

// ArtifactFacts returns normalized JSON evidence for the facts observed in one namespace.
func (u *Monitor) ArtifactFacts(namespaceID string) ([]json.RawMessage, error) {
	return u.evidence.runtime.ArtifactFacts(u.namespaceRoot(namespaceID))
}

// ObservationSummary returns a compact diagnostic of callback lineage evidence.
func (u *Monitor) ObservationSummary(namespaceID string) string {
	root := u.namespaceRoot(namespaceID)
	view := u.evidence.runtime.View(root)
	var callbackFacts []string
	seenFacts := make(map[string]struct{})
	for _, observed := range view.Facts() {
		var summary string
		switch value := observed.(type) {
		case *fact.NexusCallbackObservation:
			summary = fmt.Sprintf("NexusCallback(callback=%s operation=%s malformed=%t class=%s)", value.CallbackID, value.OperationID, value.Malformed, value.ErrorClass)
		case *fact.WorkflowCallbackAttachment:
			summary = fmt.Sprintf("WorkflowCallback(callback=%s run=%s malformed=%t class=%s)", value.CallbackID, value.HandlerRunID, value.Malformed, value.ErrorClass)
		default:
			continue
		}
		if _, exists := seenFacts[summary]; exists {
			continue
		}
		seenFacts[summary] = struct{}{}
		callbackFacts = append(callbackFacts, summary)
	}
	var states []string
	for _, entry := range view.Entities(model.CallbackType, 0) {
		if callback, ok := entry.Entity.(*model.Callback); ok {
			states = append(states, fmt.Sprintf("Callback(id=%s handler=%s/%s start=%s ref=%s:%s/%s)", callback.CallbackID, callback.HandlerWorkflowID, callback.HandlerRunID, callback.HandlerWorkflowStartTime, callback.ReferenceKind, callback.ReferenceValue, callback.ReferencedEventType))
		}
	}
	for _, entry := range view.Entities(model.NexusOperationType, 0) {
		if operation, ok := entry.Entity.(*model.NexusOperation); ok {
			states = append(states, fmt.Sprintf("NexusOperation(workflow=%s handler=%s/%s start=%s ref=%s:%s/%s)", operation.WorkflowID, operation.HandlerWorkflowID, operation.HandlerRunID, operation.StartHistoryEventTime, operation.StartReferenceKind, operation.StartReferenceValue, operation.StartReferencedEventType))
		}
	}
	relations := strings.ReplaceAll(fmt.Sprint(view.Relations()), "\x00", "/")
	return fmt.Sprintf("facts=%v states=%v relations=%s", callbackFacts, states, relations)
}

func nexusModelState(state string) (string, bool) {
	states := map[string]string{
		"scheduled": model.NexusScheduled,
		"started":   model.NexusStarted,
		"completed": model.NexusSucceeded,
		"canceled":  model.NexusCanceled,
		"timed_out": model.NexusTimedOut,
	}
	result, ok := states[state]
	return result, ok
}

func (u *Monitor) callbackReferenceObserved(view umpirefw.RuntimeView, namespaceID, operationID, handlerID string) bool {
	var operation *model.NexusOperation
	for _, entry := range view.Entities(model.NexusOperationType, 0) {
		candidate, ok := entry.Entity.(*model.NexusOperation)
		if ok && candidate.WorkflowID == operationID {
			operation = candidate
			break
		}
	}
	if operation == nil || operation.StartReferenceKind == "" || operation.StartedAt().IsZero() {
		return false
	}
	for _, entry := range view.Entities(model.CallbackType, 0) {
		callback, ok := entry.Entity.(*model.Callback)
		if !ok || callback.OperationID != operationID || callback.HandlerWorkflowID != handlerID || callback.ReferenceKind == "" {
			continue
		}
		callbackID := umpirefw.NewEntityID(model.CallbackType, namespaceID+"\x00"+callback.CallbackID)
		handlerRun := umpirefw.NewEntityID(model.WorkflowRunType, namespaceID+"\x00"+callback.HandlerRunID)
		if !slices.Contains(view.RelationTargets(model.CallbackHandlerRunRelation, callbackID), handlerRun) {
			continue
		}
		referencedAt := callback.HandlerWorkflowStartTime
		if callback.ReferenceKind == "request" {
			referencedAt = callback.AttachmentEventTime
		}
		return operation.HandlerWorkflowID == callback.HandlerWorkflowID &&
			operation.HandlerRunID == callback.HandlerRunID &&
			operation.StartReferenceKind == callback.ReferenceKind &&
			operation.StartReferenceValue == callback.ReferenceValue &&
			operation.StartReferencedEventType == callback.ReferencedEventType &&
			!referencedAt.IsZero() && !operation.StartHistoryEventTime.Before(referencedAt)
	}
	return false
}

func (u *Monitor) lateStartResponseObserved(view umpirefw.RuntimeView, operationID string) bool {
	var settledAt time.Time
	for _, entry := range view.Entities(model.NexusOperationType, 0) {
		operation, ok := entry.Entity.(*model.NexusOperation)
		if !ok || operation.WorkflowID != operationID {
			continue
		}
		if observedAt, settled := operation.SettledAt(); settled {
			settledAt = observedAt
			break
		}
	}
	if settledAt.IsZero() {
		return false
	}
	for _, entry := range view.Entities(model.CallbackType, 0) {
		callback, ok := entry.Entity.(*model.Callback)
		if ok && callback.OperationID == operationID && callback.ResponseKind == "async_success" &&
			len(callback.DeliveryResponses) > 0 && len(callback.ConflictingResponses) == 0 && callback.FirstResponseTime.After(settledAt) {
			return true
		}
	}
	return false
}

func (u *Monitor) terminalFactObserved(view umpirefw.RuntimeView, workflowID string, matches func(*fact.NexusOperationTerminal) bool) bool {
	for _, observed := range view.FactsByType((&fact.NexusOperationTerminal{}).Name()) {
		terminal, ok := observed.(*fact.NexusOperationTerminal)
		if ok && terminal.WorkflowID == workflowID && matches(terminal) {
			return true
		}
	}
	return false
}

func (u *Monitor) emptyNexusStorageObserved(view umpirefw.RuntimeView, workflowID string) bool {
	observed := view.FactsByType((&fact.WorkflowNexusStorageSnapshot{}).Name())
	for i := len(observed) - 1; i >= 0; i-- {
		snapshot, ok := observed[i].(*fact.WorkflowNexusStorageSnapshot)
		if ok && snapshot.WorkflowID == workflowID {
			return len(snapshot.OperationIDs) == 0
		}
	}
	return false
}

func (u *Monitor) handlerWorkflowObserved(namespaceID, operationID, handlerID string) bool {
	view := u.evidence.runtime.View(u.namespaceRoot(namespaceID))
	operation := umpirefw.NewEntityID(model.NexusOperationType, namespaceID+"\x00"+operationID)
	handler := umpirefw.NewEntityID(model.WorkflowType, namespaceID+"\x00"+handlerID)
	handlerRuns := view.RelationTargets(model.WorkflowRunsRelation, handler)
	for _, callback := range view.RelationSources(model.CallbackOperationRelation, operation) {
		for _, run := range view.RelationTargets(model.CallbackHandlerRunRelation, callback) {
			if slices.Contains(handlerRuns, run) {
				return true
			}
		}
	}
	return false
}
