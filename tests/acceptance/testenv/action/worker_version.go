package action

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

var (
	GenVersioningBehavior = stamp.GenChoice("GenVersioningBehavior",
		enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED,
		enumspb.VERSIONING_BEHAVIOR_PINNED,
		enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
	)
)

type CreateWorkerDeployment struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.WorkerDeployment]
	Namespace *model.Namespace `validate:"required"`
	Name      stamp.Gen[stamp.ID]
}

func (t CreateWorkerDeployment) Next(ctx stamp.GenContext) model.NewWorkerDeployment {
	return model.NewWorkerDeployment{
		Namespace: t.Namespace,
		Name:      t.Name.Next(ctx.AllowRandom()),
	}
}

type CreateWorkerDeploymentVersion struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.WorkerDeploymentVersion]
	Deployment *model.WorkerDeployment `validate:"required"`
	Name       stamp.Gen[stamp.ID]
}

func (t CreateWorkerDeploymentVersion) Next(ctx stamp.GenContext) model.NewWorkerDeploymentVersion {
	return model.NewWorkerDeploymentVersion{
		Deployment: t.Deployment,
		Name:       t.Name.Next(ctx.AllowRandom()),
	}
}

type SetWorkerDeploymentCurrentVersion struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.WorkerDeploymentVersion]
	Version *model.WorkerDeploymentVersion `validate:"required"`
	// TODO: ...
}

func (w SetWorkerDeploymentCurrentVersion) Next(ctx stamp.GenContext) *workflowservice.SetWorkerDeploymentCurrentVersionRequest {
	return &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      string(w.Version.GetDeployment().GetNamespace().GetID()),
		DeploymentName: string(w.Version.GetDeployment().GetID()),
		BuildId:        string(w.Version.GetID()),
	}
}
