package adapter

import (
	"github.com/temporalio/temporal-proto/enums"
	"github.com/temporalio/temporal-proto/workflowservice"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
)

func ToThriftRegisterDomainRequest(in *workflowservice.RegisterDomainRequest) *shared.RegisterDomainRequest {
	var clusters []*shared.ClusterReplicationConfiguration
	for _, cluster := range in.Clusters {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(cluster.ClusterName),
		})
	}

	return &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(in.Name),
		Description:                            common.StringPtr(in.Description),
		OwnerEmail:                             common.StringPtr(in.OwnerEmail),
		Data:                                   in.Data,
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(in.WorkflowExecutionRetentionPeriodInDays),
		EmitMetric:                             common.BoolPtr(in.EmitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(in.ActiveClusterName),
		SecurityToken:                          common.StringPtr(in.SecurityToken),
		HistoryArchivalStatus:                  toThriftArchivalStatus(in.HistoryArchivalStatus),
		HistoryArchivalURI:                     common.StringPtr(in.HistoryArchivalURI),
		VisibilityArchivalStatus:               toThriftArchivalStatus(in.VisibilityArchivalStatus),
		VisibilityArchivalURI:                  common.StringPtr(in.VisibilityArchivalURI),
		IsGlobalDomain:                         common.BoolPtr(in.IsGlobalDomain),
	}
}

func toThriftArchivalStatus(in enums.ArchivalStatus) *shared.ArchivalStatus {
	switch in {
	case enums.ArchivalStatusDisabled:
		return common.ArchivalStatusPtr(shared.ArchivalStatusDisabled)
	case enums.ArchivalStatusEnabled:
		return common.ArchivalStatusPtr(shared.ArchivalStatusEnabled)
	default:
		return nil
	}
}
