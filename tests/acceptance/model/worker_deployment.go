package model

import (
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkerDeployment struct {
		stamp.Model[*WorkerDeployment]
		stamp.Scope[*Namespace]
	}
	NewWorkerDeployment struct {
		Namespace *Namespace `validate:"required"`
		Name      stamp.ID
	}
)

func (w *WorkerDeployment) GetNamespace() *Namespace {
	return w.GetScope()
}

func (w *WorkerDeployment) OnSetWorkerDeploymentCurrentVersion(
	_ IncomingAction[*workflowservice.SetWorkerDeploymentCurrentVersionRequest],
) func(OutgoingAction[*workflowservice.SetWorkerDeploymentCurrentVersionResponse]) {
	return func(out OutgoingAction[*workflowservice.SetWorkerDeploymentCurrentVersionResponse]) {
		// TODO
	}
}

func (w *WorkerDeployment) Verify() {}
