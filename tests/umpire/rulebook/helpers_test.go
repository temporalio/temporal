package rulebook

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	historyv1 "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatev1 "go.temporal.io/api/update/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
	"go.temporal.io/server/tests/umpire/fact"
)

func newTestRegistry() *umpire.Registry {
	r := umpire.NewRegistry()
	entity.RegisterDefaultEntities(r)
	return r
}

func checkSafetyRule(reg *umpire.Registry, m umpire.SafetyRule) []umpire.Violation {
	return umpire.CheckSafetyRule(context.Background(), m, reg, log.NewNoopLogger(), umpire.RuleConfig{})
}

func checkLivenessRule(reg *umpire.Registry, m umpire.LivenessRule) []umpire.Violation {
	return umpire.CheckLivenessRule(context.Background(), m, reg, log.NewNoopLogger(), umpire.RuleConfig{})
}

func routeFact(t *testing.T, r *umpire.Registry, m umpire.Fact) {
	t.Helper()
	if err := r.RouteFacts(context.Background(), []umpire.Fact{m}); err != nil {
		t.Fatalf("RouteFacts failed: %v", err)
	}
}

func makeWorkflowTaskAdded(taskQueue, workflowID, runID string) *fact.WorkflowTaskAdded {
	wtID := umpire.NewEntityID(entity.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	tqID := umpire.NewEntityID(entity.TaskQueueType, taskQueue)
	return &fact.WorkflowTaskAdded{
		Request: &matchingservice.AddWorkflowTaskRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		},
		Identity: &umpire.Identity{EntityID: wtID, ParentID: &tqID},
	}
}

func makeWorkflowTaskStored(taskQueue, workflowID, runID string) *fact.WorkflowTaskStored {
	wtID := umpire.NewEntityID(entity.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	tqID := umpire.NewEntityID(entity.TaskQueueType, taskQueue)
	return &fact.WorkflowTaskStored{
		TaskQueue:  taskQueue,
		WorkflowID: workflowID,
		RunID:      runID,
		Identity:   &umpire.Identity{EntityID: wtID, ParentID: &tqID},
	}
}

func makeSpecWorkflowTaskStored(taskQueue, workflowID, runID string) *fact.WorkflowTaskStored {
	wtID := umpire.NewEntityID(entity.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	return &fact.WorkflowTaskStored{
		TaskQueue:  taskQueue,
		WorkflowID: workflowID,
		RunID:      runID,
		Identity:   &umpire.Identity{EntityID: wtID, ParentID: &wfID},
	}
}

func makeSpeculativeScheduled(taskQueue, workflowID, runID string) *fact.SpeculativeWorkflowTaskScheduled {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	wtID := umpire.NewEntityID(entity.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	return &fact.SpeculativeWorkflowTaskScheduled{
		WorkflowID: workflowID,
		RunID:      runID,
		TaskQueue:  taskQueue,
		Identity:   &umpire.Identity{EntityID: wtID, ParentID: &wfID},
	}
}

func makeWorkflowTaskPolled(taskQueue, workflowID, runID string, taskReturned bool) *fact.WorkflowTaskPolled {
	wtID := umpire.NewEntityID(entity.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	tqID := umpire.NewEntityID(entity.TaskQueueType, taskQueue)
	return &fact.WorkflowTaskPolled{
		Request: &matchingservice.PollWorkflowTaskQueueRequest{
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			},
		},
		Identity:     &umpire.Identity{EntityID: wtID, ParentID: &tqID},
		TaskReturned: taskReturned,
	}
}

func makeSpecWorkflowTaskPolled(taskQueue, workflowID, runID string, taskReturned bool) *fact.WorkflowTaskPolled {
	wtID := umpire.NewEntityID(entity.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	return &fact.WorkflowTaskPolled{
		Request: &matchingservice.PollWorkflowTaskQueueRequest{
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			},
		},
		Identity:     &umpire.Identity{EntityID: wtID, ParentID: &wfID},
		TaskReturned: taskReturned,
	}
}

func makeWorkflowUpdateAdmitted(workflowID, updateID string) *fact.WorkflowUpdateAdmitted {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	updID := umpire.NewEntityID(entity.WorkflowUpdateType, updateID)
	return &fact.WorkflowUpdateAdmitted{
		Attributes: &historyv1.WorkflowExecutionUpdateAdmittedEventAttributes{
			Request: &updatev1.Request{
				Meta:  &updatev1.Meta{UpdateId: updateID},
				Input: &updatev1.Input{Name: "handler"},
			},
		},
		Identity: &umpire.Identity{EntityID: updID, ParentID: &wfID},
	}
}

func makeWorkflowUpdateAccepted(workflowID, updateID string) *fact.WorkflowUpdateAccepted {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	updID := umpire.NewEntityID(entity.WorkflowUpdateType, updateID)
	return &fact.WorkflowUpdateAccepted{
		Attributes: &historyv1.WorkflowExecutionUpdateAcceptedEventAttributes{},
		Identity:   &umpire.Identity{EntityID: updID, ParentID: &wfID},
	}
}

func makeWorkflowUpdateCompleted(workflowID, updateID string) *fact.WorkflowUpdateCompleted {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	updID := umpire.NewEntityID(entity.WorkflowUpdateType, updateID)
	return &fact.WorkflowUpdateCompleted{
		Attributes: &historyv1.WorkflowExecutionUpdateCompletedEventAttributes{
			Meta:    &updatev1.Meta{UpdateId: updateID},
			Outcome: &updatev1.Outcome{Value: &updatev1.Outcome_Success{}},
		},
		Identity: &umpire.Identity{EntityID: updID, ParentID: &wfID},
	}
}

func makeWorkflowUpdateRejected(workflowID, updateID string) *fact.WorkflowUpdateRejected {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	updID := umpire.NewEntityID(entity.WorkflowUpdateType, updateID)
	return &fact.WorkflowUpdateRejected{
		Attributes: &historyv1.WorkflowExecutionUpdateRejectedEventAttributes{
			RejectedRequest: &updatev1.Request{
				Meta: &updatev1.Meta{UpdateId: updateID},
			},
		},
		Identity: &umpire.Identity{EntityID: updID, ParentID: &wfID},
	}
}

func makeWorkflowUpdateRequested(workflowID, updateID string) *fact.WorkflowUpdateRequested {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	updID := umpire.NewEntityID(entity.WorkflowUpdateType, updateID)
	return &fact.WorkflowUpdateRequested{
		Request: &historyservice.UpdateWorkflowExecutionRequest{
			Request: &workflowservice.UpdateWorkflowExecutionRequest{
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
				Request: &updatev1.Request{
					Meta:  &updatev1.Meta{UpdateId: updateID},
					Input: &updatev1.Input{Name: "handler"},
				},
			},
		},
		Identity: &umpire.Identity{EntityID: updID, ParentID: &wfID},
	}
}

func makeWorkflowStarted(workflowID string) *fact.WorkflowStarted {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	return &fact.WorkflowStarted{
		Request: &historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId: workflowID,
			},
		},
		Identity: &umpire.Identity{EntityID: wfID},
	}
}

func makeWorkflowCompleted(workflowID string) *fact.WorkflowTaskCompleted {
	wfID := umpire.NewEntityID(entity.WorkflowType, workflowID)
	return &fact.WorkflowTaskCompleted{
		Identity: &umpire.Identity{EntityID: wfID},
	}
}
