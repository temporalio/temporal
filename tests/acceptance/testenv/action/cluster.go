package action

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type ClusterConfig struct {
	Key dynamicconfig.GenericSetting `validate:"required"`
	Val any                          `validate:"required"`
}

type StartCluster struct {
	stamp.ActionActor[*stamp.Root]
	stamp.ActionTarget[*model.Cluster]
	Name    stamp.Gen[stamp.ID]
	Configs []ClusterConfig
}

func (t StartCluster) Next(ctx stamp.GenContext) model.ClusterStarted {
	return model.ClusterStarted{
		Name: t.Name.Next(ctx.AllowRandom()),
	}
}
