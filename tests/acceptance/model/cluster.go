package model

import (
	"go.temporal.io/server/common/testing/stamp"
)

type (
	Cluster struct {
		stamp.Model[*Cluster]
		stamp.Scope[*stamp.Root]
	}
	ClusterName    string
	ClusterStarted struct {
		ClusterName ClusterName
	}
)

func (c *Cluster) Verify() {}
