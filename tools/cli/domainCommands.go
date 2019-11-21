// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	"go.uber.org/yarpc/yarpcerrors"

	pbCommon "github.com/temporalio/temporal-proto/common"
	"github.com/temporalio/temporal-proto/enums"
	"github.com/temporalio/temporal-proto/workflowservice"
	"github.com/temporalio/temporal/.gen/go/shared"
	serviceFrontend "github.com/temporalio/temporal/.gen/go/temporal/workflowserviceclient"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

type (
	domainCLIImpl struct {
		// used when making RPC call to frontend service``
		frontendClient serviceFrontend.Interface

		frontendClientGRPC workflowservice.WorkflowServiceYARPCClient

		// act as admin to modify domain in DB directly
		domainHandler domain.Handler
	}
)

// newDomainCLI creates a domain CLI
func newDomainCLI(
	c *cli.Context,
	isAdminMode bool,
) *domainCLIImpl {

	var frontendClient serviceFrontend.Interface
	var frontendClientGRPC workflowservice.WorkflowServiceYARPCClient
	var domainHandler domain.Handler
	if !isAdminMode {
		frontendClient = initializeFrontendClient(c)
		frontendClientGRPC = initializeFrontendClientGRPC(c)
	} else {
		domainHandler = initializeAdminDomainHandler(c)
	}
	return &domainCLIImpl{
		frontendClient:     frontendClient,
		frontendClientGRPC: frontendClientGRPC,
		domainHandler:      domainHandler,
	}
}

// RegisterDomain register a domain
func (d *domainCLIImpl) RegisterDomain(c *cli.Context) {
	domainName := getRequiredGlobalOption(c, FlagDomain)

	description := c.String(FlagDescription)
	ownerEmail := c.String(FlagOwnerEmail)
	retentionDays := defaultDomainRetentionDays

	if c.IsSet(FlagRetentionDays) {
		retentionDays = c.Int(FlagRetentionDays)
	}
	securityToken := c.String(FlagSecurityToken)
	emitMetric := false
	var err error
	if c.IsSet(FlagEmitMetric) {
		emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagEmitMetric), err)
		}
	}
	var isGlobalDomain bool
	if c.IsSet(FlagIsGlobalDomain) {
		isGlobalDomain, err = strconv.ParseBool(c.String(FlagIsGlobalDomain))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagIsGlobalDomain), err)
		}
	}

	domainData := map[string]string{}
	if c.IsSet(FlagDomainData) {
		domainDataStr := getRequiredOption(c, FlagDomainData)
		domainData, err = parseDomainDataKVs(domainDataStr)
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagDomainData), err)
		}
	}
	if len(requiredDomainDataKeys) > 0 {
		err = checkRequiredDomainDataKVs(domainData)
		if err != nil {
			ErrorAndExit("Domain data missed required data.", err)
		}
	}

	var activeClusterName string
	if c.IsSet(FlagActiveClusterName) {
		activeClusterName = c.String(FlagActiveClusterName)
	}

	var clusters []*pbCommon.ClusterReplicationConfiguration
	if c.IsSet(FlagClusters) {
		clusterStr := c.String(FlagClusters)
		clusters = append(clusters, &pbCommon.ClusterReplicationConfiguration{
			ClusterName: clusterStr,
		})
		for _, clusterStr := range c.Args() {
			clusters = append(clusters, &pbCommon.ClusterReplicationConfiguration{
				ClusterName: clusterStr,
			})
		}
	}

	request := &workflowservice.RegisterDomainRequest{
		Name:                                   domainName,
		Description:                            description,
		OwnerEmail:                             ownerEmail,
		Data:                                   domainData,
		WorkflowExecutionRetentionPeriodInDays: int32(retentionDays),
		EmitMetric:                             emitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      activeClusterName,
		SecurityToken:                          securityToken,
		HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
		HistoryArchivalURI:                     c.String(FlagHistoryArchivalURI),
		VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
		VisibilityArchivalURI:                  c.String(FlagVisibilityArchivalURI),
		IsGlobalDomain:                         isGlobalDomain,
	}

	ctx, cancel := newContext(c)
	defer cancel()
	err = d.registerDomain(ctx, request, c.IsSet(FlagGRPC))
	if err != nil {
		switch er := err.(type) {
		case *shared.DomainAlreadyExistsError:
			ErrorAndExit(fmt.Sprintf("Domain %s already registered.", domainName), er)
		case *yarpcerrors.Status:
			ErrorAndExit(er.Message(), er)
		default:
			ErrorAndExit("Operation RegisterDomain failed.", er)
		}
	} else {
		fmt.Printf("Domain %s successfully registered.\n", domainName)
	}
}

// UpdateDomain updates a domain
func (d *domainCLIImpl) UpdateDomain(c *cli.Context) {
	domainName := getRequiredGlobalOption(c, FlagDomain)

	var updateRequest *workflowservice.UpdateDomainRequest
	ctx, cancel := newContext(c)
	defer cancel()

	if c.IsSet(FlagActiveClusterName) {
		activeCluster := c.String(FlagActiveClusterName)
		fmt.Printf("Will set active cluster name to: %s, other flag will be omitted.\n", activeCluster)
		replicationConfig := &pbCommon.DomainReplicationConfiguration{
			ActiveClusterName: activeCluster,
		}
		updateRequest = &workflowservice.UpdateDomainRequest{
			Name:                     domainName,
			ReplicationConfiguration: replicationConfig,
		}
	} else {
		resp, err := d.describeDomain(ctx, &workflowservice.DescribeDomainRequest{
			Name: domainName,
		}, c.IsSet(FlagGRPC))
		if err != nil {
			switch er := err.(type) {
			case *shared.EntityNotExistsError:
				ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), er)
			case *yarpcerrors.Status:
				ErrorAndExit(er.Message(), er)
			default:
				ErrorAndExit("Operation UpdateDomain failed.", err)
			}
			return
		}

		description := resp.DomainInfo.GetDescription()
		ownerEmail := resp.DomainInfo.GetOwnerEmail()
		retentionDays := resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays()
		emitMetric := resp.Configuration.GetEmitMetric().GetValue()
		var clusters []*pbCommon.ClusterReplicationConfiguration

		if c.IsSet(FlagDescription) {
			description = c.String(FlagDescription)
		}
		if c.IsSet(FlagOwnerEmail) {
			ownerEmail = c.String(FlagOwnerEmail)
		}
		domainData := map[string]string{}
		if c.IsSet(FlagDomainData) {
			domainDataStr := c.String(FlagDomainData)
			domainData, err = parseDomainDataKVs(domainDataStr)
			if err != nil {
				ErrorAndExit("Domain data format is invalid.", err)
			}
		}
		if c.IsSet(FlagRetentionDays) {
			retentionDays = int32(c.Int(FlagRetentionDays))
		}
		if c.IsSet(FlagEmitMetric) {
			emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
			if err != nil {
				ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagEmitMetric), err)
			}
		}
		if c.IsSet(FlagClusters) {
			clusterStr := c.String(FlagClusters)
			clusters = append(clusters, &pbCommon.ClusterReplicationConfiguration{
				ClusterName: clusterStr,
			})
			for _, clusterStr := range c.Args() {
				clusters = append(clusters, &pbCommon.ClusterReplicationConfiguration{
					ClusterName: clusterStr,
				})
			}
		}

		var binBinaries *pbCommon.BadBinaries
		if c.IsSet(FlagAddBadBinary) {
			if !c.IsSet(FlagReason) {
				ErrorAndExit("Must provide a reason.", nil)
			}
			binChecksum := c.String(FlagAddBadBinary)
			reason := c.String(FlagReason)
			operator := getCurrentUserFromEnv()
			binBinaries = &pbCommon.BadBinaries{
				Binaries: map[string]*pbCommon.BadBinaryInfo{
					binChecksum: {
						Reason:   reason,
						Operator: operator,
					},
				},
			}
		}

		var badBinaryToDelete string
		if c.IsSet(FlagRemoveBadBinary) {
			badBinaryToDelete = c.String(FlagRemoveBadBinary)
		}

		updateInfo := &pbCommon.UpdateDomainInfo{
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        domainData,
		}
		updateConfig := &pbCommon.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retentionDays,
			EmitMetric:                             &pbCommon.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
			HistoryArchivalURI:                     c.String(FlagHistoryArchivalURI),
			VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
			VisibilityArchivalURI:                  c.String(FlagVisibilityArchivalURI),
			BadBinaries:                            binBinaries,
		}
		replicationConfig := &pbCommon.DomainReplicationConfiguration{
			Clusters: clusters,
		}
		updateRequest = &workflowservice.UpdateDomainRequest{
			Name:                     domainName,
			UpdatedInfo:              updateInfo,
			Configuration:            updateConfig,
			ReplicationConfiguration: replicationConfig,
			DeleteBadBinary:          badBinaryToDelete,
		}
	}

	securityToken := c.String(FlagSecurityToken)
	updateRequest.SecurityToken = securityToken
	err := d.updateDomain(ctx, updateRequest, c.IsSet(FlagGRPC))
	if err != nil {
		switch er := err.(type) {
		case *shared.EntityNotExistsError:
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), er)
		case *yarpcerrors.Status:
			ErrorAndExit(er.Message(), er)
		default:
			ErrorAndExit("Operation UpdateDomain failed.", err)
		}
	} else {
		fmt.Printf("Domain %s successfully updated.\n", domainName)
	}
}

// DescribeDomain updates a domain
func (d *domainCLIImpl) DescribeDomain(c *cli.Context) {
	domainName := c.GlobalString(FlagDomain)
	domainID := c.String(FlagDomainID)

	if domainID == "" && domainName == "" {
		ErrorAndExit("At least domainID or domainName must be provided.", nil)
	}
	ctx, cancel := newContext(c)
	defer cancel()
	resp, err := d.describeDomain(ctx, &workflowservice.DescribeDomainRequest{
		Name: domainName,
		Uuid: domainID,
	}, c.IsSet(FlagGRPC))
	if err != nil {
		switch er := err.(type) {
		case *shared.EntityNotExistsError:
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), er)
		case *yarpcerrors.Status:
			ErrorAndExit(er.Message(), er)
		default:
			ErrorAndExit("Operation DescribeDomain failed.", err)
		}
		return
	}

	var formatStr = "Name: %v\nUUID: %v\nDescription: %v\nOwnerEmail: %v\nDomainData: %#v\nStatus: %v\nRetentionInDays: %v\n" +
		"EmitMetrics: %v\nActiveClusterName: %v\nClusters: %v\nHistoryArchivalStatus: %v\n"
	descValues := []interface{}{
		resp.DomainInfo.GetName(),
		resp.DomainInfo.GetUuid(),
		resp.DomainInfo.GetDescription(),
		resp.DomainInfo.GetOwnerEmail(),
		resp.DomainInfo.Data,
		resp.DomainInfo.GetStatus(),
		resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays(),
		resp.Configuration.GetEmitMetric().GetValue(),
		resp.ReplicationConfiguration.GetActiveClusterName(),
		clustersToString(resp.ReplicationConfiguration.Clusters),
		resp.Configuration.GetHistoryArchivalStatus().String(),
	}
	if resp.Configuration.GetHistoryArchivalURI() != "" {
		formatStr = formatStr + "HistoryArchivalURI: %v\n"
		descValues = append(descValues, resp.Configuration.GetHistoryArchivalURI())
	}
	formatStr = formatStr + "VisibilityArchivalStatus: %v\n"
	descValues = append(descValues, resp.Configuration.GetVisibilityArchivalStatus().String())
	if resp.Configuration.GetVisibilityArchivalURI() != "" {
		formatStr = formatStr + "VisibilityArchivalURI: %v\n"
		descValues = append(descValues, resp.Configuration.GetVisibilityArchivalURI())
	}
	fmt.Printf(formatStr, descValues...)
	if resp.Configuration.BadBinaries != nil {
		fmt.Println("Bad binaries to reset:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetBorder(true)
		table.SetColumnSeparator("|")
		header := []string{"Binary Checksum", "Operator", "Start Time", "Reason"}
		headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
		table.SetHeader(header)
		table.SetHeaderColor(headerColor...)
		for cs, bin := range resp.Configuration.BadBinaries.Binaries {
			row := []string{cs}
			row = append(row, bin.GetOperator())
			row = append(row, time.Unix(0, bin.GetCreatedTimeNano()).String())
			row = append(row, bin.GetReason())
			table.Append(row)
		}
		table.Render()
	}
}

func (d *domainCLIImpl) registerDomain(
	ctx context.Context,
	request *workflowservice.RegisterDomainRequest,
	useGRPC bool,
) error {

	if useGRPC && d.frontendClientGRPC != nil {
		_, err := d.frontendClientGRPC.RegisterDomain(ctx, request)
		return err
	}

	if !useGRPC && d.frontendClient != nil {
		return d.frontendClient.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(request))
	}

	return d.domainHandler.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(request))
}

func (d *domainCLIImpl) updateDomain(
	ctx context.Context,
	request *workflowservice.UpdateDomainRequest,
	useGRPC bool,
) error {
	if useGRPC && d.frontendClientGRPC != nil {
		_, err := d.frontendClientGRPC.UpdateDomain(ctx, request)
		return err
	}

	if !useGRPC && d.frontendClient != nil {
		_, err := d.frontendClient.UpdateDomain(ctx, adapter.ToThriftUpdateDomainRequest(request))
		return err
	}

	_, err := d.domainHandler.UpdateDomain(ctx, adapter.ToThriftUpdateDomainRequest(request))
	return err
}

func (d *domainCLIImpl) describeDomain(
	ctx context.Context,
	request *workflowservice.DescribeDomainRequest,
	useGRPC bool,
) (*workflowservice.DescribeDomainResponse, error) {
	if useGRPC && d.frontendClientGRPC != nil {
		return d.frontendClientGRPC.DescribeDomain(ctx, request)
	}

	if !useGRPC && d.frontendClient != nil {
		resp, err := d.frontendClient.DescribeDomain(ctx, adapter.ToThriftDescribeDomainRequest(request))
		return adapter.ToProtoDescribeDomainResponse(resp), err
	}

	resp, err := d.domainHandler.DescribeDomain(ctx, adapter.ToThriftDescribeDomainRequest(request))
	return adapter.ToProtoDescribeDomainResponse(resp), err
}

func clustersToString(clusters []*pbCommon.ClusterReplicationConfiguration) string {
	var res string
	for i, cluster := range clusters {
		if i == 0 {
			res = res + cluster.GetClusterName()
		} else {
			res = res + ", " + cluster.GetClusterName()
		}
	}
	return res
}

func archivalStatus(c *cli.Context, statusFlagName string) enums.ArchivalStatus {
	if c.IsSet(statusFlagName) {
		switch c.String(statusFlagName) {
		case "disabled":
			return enums.ArchivalStatusDisabled
		case "enabled":
			return enums.ArchivalStatusEnabled
		default:
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", statusFlagName), errors.New("invalid status, valid values are \"disabled\" and \"enabled\""))
		}
	}
	return enums.ArchivalStatusDefault
}
