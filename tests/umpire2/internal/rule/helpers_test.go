package rule

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

func newTestModelState() *umpire.ModelState {
	r := umpire.NewModelState()
	model.RegisterDefaultEntities(r)
	return r
}

func checkSafetyRule(reg *umpire.ModelState, m umpire.SafetyRule) []umpire.Violation {
	return umpire.CheckSafetyRule(context.Background(), m, reg, umpire.RuleConfig{})
}

func checkLivenessRule(reg *umpire.ModelState, m umpire.LivenessRule) []umpire.Violation {
	return umpire.CheckLivenessRule(context.Background(), m, reg, umpire.RuleConfig{})
}

func routeFact(t *testing.T, r *umpire.ModelState, m umpire.Fact) {
	t.Helper()
	if err := r.RouteFacts(context.Background(), []umpire.Fact{m}); err != nil {
		t.Fatalf("RouteFacts failed: %v", err)
	}
}

func makeWorkflowTaskAdded(taskQueue, workflowID, runID string) *fact.WorkflowTaskAdded {
	wtID := umpire.NewEntityID(model.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	tqID := umpire.NewEntityID(model.TaskQueueType, taskQueue)
	return &fact.WorkflowTaskAdded{
		Request: &matchingservice.AddWorkflowTaskRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		},
		EntityPath: &umpire.EntityPath{EntityID: wtID, Ancestors: []umpire.EntityID{tqID}},
	}
}

func makeWorkflowTaskStored(taskQueue, workflowID, runID string) *fact.WorkflowTaskStored {
	wtID := umpire.NewEntityID(model.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	tqID := umpire.NewEntityID(model.TaskQueueType, taskQueue)
	return &fact.WorkflowTaskStored{
		TaskQueue:  taskQueue,
		WorkflowID: workflowID,
		RunID:      runID,
		EntityPath: &umpire.EntityPath{EntityID: wtID, Ancestors: []umpire.EntityID{tqID}},
	}
}

func makeSpecWorkflowTaskStored(taskQueue, workflowID, runID string) *fact.WorkflowTaskStored {
	wtID := umpire.NewEntityID(model.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
	return &fact.WorkflowTaskStored{
		TaskQueue:  taskQueue,
		WorkflowID: workflowID,
		RunID:      runID,
		EntityPath: &umpire.EntityPath{EntityID: wtID, Ancestors: []umpire.EntityID{wfID}},
	}
}

func makeSpeculativeScheduled(taskQueue, workflowID, runID string) *fact.SpeculativeWorkflowTaskScheduled {
	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
	wtID := umpire.NewEntityID(model.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	return &fact.SpeculativeWorkflowTaskScheduled{
		WorkflowID: workflowID,
		RunID:      runID,
		TaskQueue:  taskQueue,
		EntityPath: &umpire.EntityPath{EntityID: wtID, Ancestors: []umpire.EntityID{wfID}},
	}
}

func makeWorkflowTaskPolled(taskQueue, workflowID, runID string, taskReturned bool) *fact.WorkflowTaskPolled {
	wtID := umpire.NewEntityID(model.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	tqID := umpire.NewEntityID(model.TaskQueueType, taskQueue)
	return &fact.WorkflowTaskPolled{
		Request: &matchingservice.PollWorkflowTaskQueueRequest{
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			},
		},
		EntityPath:   &umpire.EntityPath{EntityID: wtID, Ancestors: []umpire.EntityID{tqID}},
		TaskReturned: taskReturned,
	}
}

func makeSpecWorkflowTaskPolled(taskQueue, workflowID, runID string, taskReturned bool) *fact.WorkflowTaskPolled {
	wtID := umpire.NewEntityID(model.WorkflowTaskType, taskQueue+":"+workflowID+":"+runID)
	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
	return &fact.WorkflowTaskPolled{
		Request: &matchingservice.PollWorkflowTaskQueueRequest{
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			},
		},
		EntityPath:   &umpire.EntityPath{EntityID: wtID, Ancestors: []umpire.EntityID{wfID}},
		TaskReturned: taskReturned,
	}
}

func makeWorkflowStarted(workflowID string) *fact.WorkflowStarted {
	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
	return &fact.WorkflowStarted{
		Request: &historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId: workflowID,
			},
		},
		EntityPath: &umpire.EntityPath{EntityID: wfID},
	}
}

func makeWorkflowCompleted(workflowID string) *fact.WorkflowExecutionCompleted {
	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
	return &fact.WorkflowExecutionCompleted{
		WorkflowID: workflowID,
		EntityPath: &umpire.EntityPath{EntityID: wfID},
	}
}
