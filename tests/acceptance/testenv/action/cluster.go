package action

import (
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type StartCluster struct {
	stamp.ActionActor[*stamp.Root]
	stamp.ActionTarget[*model.Cluster]
	ClusterName model.ClusterName
}

func (t StartCluster) Next(_ stamp.GenContext) model.ClusterStarted {
	return model.ClusterStarted{ClusterName: t.ClusterName}
}
