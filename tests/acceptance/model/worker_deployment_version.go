package model

import (
	"errors"

	"go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkerDeploymentVersion struct {
		stamp.Model[*WorkerDeploymentVersion]
		stamp.Scope[*WorkerDeployment]

		IsCurrent stamp.Prop[bool]
	}
	NewWorkerDeploymentVersion struct {
		Deployment *WorkerDeployment `validate:"required"`
		Name       stamp.ID
	}
)

func (w *WorkerDeploymentVersion) GetDeployment() *WorkerDeployment {
	return w.GetScope()
}

func (w *WorkerDeploymentVersion) OnSetWorkerDeploymentCurrentVersion(
	_ IncomingAction[*workflowservice.SetWorkerDeploymentCurrentVersionRequest],
) func(OutgoingAction[*workflowservice.SetWorkerDeploymentCurrentVersionResponse]) {
	return func(out OutgoingAction[*workflowservice.SetWorkerDeploymentCurrentVersionResponse]) {
		var notFound *serviceerror.NotFound
		switch {
		case out.ResponseErr == nil:
			w.IsCurrent.Set(true)
		case errors.As(out.ResponseErr, &notFound):
			// TODO
		default:
			panic("unhandled error: " + out.ResponseErr.Error())
		}
	}
}

func (w *WorkerDeploymentVersion) Verify() {}

func (w *WorkerDeploymentVersion) Next(ctx stamp.GenContext) *deployment.WorkerDeploymentVersion {
	return &deployment.WorkerDeploymentVersion{
		DeploymentName: string(w.GetDeployment().GetID()),
		BuildId:        string(w.GetID()),
	}
}
