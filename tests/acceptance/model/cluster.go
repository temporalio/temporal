package model

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	Cluster struct {
		stamp.Model[*Cluster]
		stamp.Scope[*stamp.Root]
	}
	ClusterName    string
	ClusterStarted struct {
		Name ClusterName
	}
	ClusterConfigChanged struct {
		Key  dynamicconfig.GenericSetting
		Vals []dynamicconfig.ConstrainedValue
	}
)

func (c *Cluster) Verify() {}
