package api

import (
	historyi "go.temporal.io/server/service/history/interfaces"
)

type WorkflowLease interface {
	GetContext() historyi.WorkflowContext
	GetMutableState() historyi.MutableState
	GetReleaseFn() historyi.ReleaseWorkflowContextFunc
}

type workflowLease struct {
	context      historyi.WorkflowContext
	mutableState historyi.MutableState
	releaseFn    historyi.ReleaseWorkflowContextFunc
}

type UpdateWorkflowAction struct {
	Noop               bool
	CreateWorkflowTask bool
	// Abort all "Workflow Updates" (not persistence updates) after persistence operation is succeeded but WF lock is not released.
	AbortUpdates bool
}

var (
	UpdateWorkflowWithNewWorkflowTask = &UpdateWorkflowAction{
		CreateWorkflowTask: true,
	}
	UpdateWorkflowWithoutWorkflowTask = &UpdateWorkflowAction{
		CreateWorkflowTask: false,
	}
	UpdateWorkflowTerminate = &UpdateWorkflowAction{
		CreateWorkflowTask: false,
		AbortUpdates:       true,
	}
)

type UpdateWorkflowActionFunc func(WorkflowLease) (*UpdateWorkflowAction, error)

var _ WorkflowLease = (*workflowLease)(nil)

func NewWorkflowLease(
	wfContext historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	mutableState historyi.MutableState,
) WorkflowLease {
	return &workflowLease{
		context:      wfContext,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func (w *workflowLease) GetContext() historyi.WorkflowContext {
	return w.context
}

func (w *workflowLease) GetMutableState() historyi.MutableState {
	return w.mutableState
}

func (w *workflowLease) GetReleaseFn() historyi.ReleaseWorkflowContextFunc {
	return w.releaseFn
}
