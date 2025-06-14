package action

import (
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

var (
	AnyVersionBehavior = stamp.GenEnum("AnyVersionBehavior",
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

type VersioningOverride struct {
	AutoUpgrade *stamp.Gen[VersioningOverride_AutoUpgrade] `validate:"required"`
	//PinnedVersion *model.WorkerDeploymentVersion             `validate:"required"`
}

func (v VersioningOverride) Next(ctx stamp.GenContext) *workflowpb.VersioningOverride {
	panic("implement me")

	//switch {
	//case v.PinnedVersion != nil:
	//	return &workflowpb.VersioningOverride{
	//		Override: &workflowpb.VersioningOverride_Pinned{
	//			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
	//				Behavior: 0, // TODO
	//				Version:  v.PinnedVersion.Next(ctx),
	//			},
	//		},
	//	}
	//case v.AutoUpgrade != nil:
	//	return &workflowpb.VersioningOverride{
	//		Override: &workflowpb.VersioningOverride_AutoUpgrade{
	//			AutoUpgrade: v.AutoUpgrade},
	//	}
	//default:
	//	panic("VersioningOverride must have either Pinned or AutoUpgrade set")
	//}
}

type VersioningOverride_AutoUpgrade struct {
	AutoUpgrade bool
}

func (v VersioningOverride_AutoUpgrade) Next(ctx stamp.GenContext) *workflowpb.VersioningOverride_AutoUpgrade {
	return &workflowpb.VersioningOverride_AutoUpgrade{
		AutoUpgrade: v.AutoUpgrade,
	}
}
