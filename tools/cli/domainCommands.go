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

	"github.com/gogo/protobuf/types"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/domain"
)

type (
	domainCLIImpl struct {
		// used when making RPC call to frontend service``
		frontendClient workflowservice.WorkflowServiceClient

		// act as admin to modify domain in DB directly
		domainHandler domain.Handler
	}
)

// newDomainCLI creates a domain CLI
func newDomainCLI(
	c *cli.Context,
	isAdminMode bool,
) *domainCLIImpl {

	var frontendClient workflowservice.WorkflowServiceClient
	var domainHandler domain.Handler
	if !isAdminMode {
		frontendClient = initializeFrontendClient(c)
	} else {
		domainHandler = initializeAdminDomainHandler(c)
	}
	return &domainCLIImpl{
		frontendClient: frontendClient,
		domainHandler:  domainHandler,
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
	var err error

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

	var clusters []*commonproto.ClusterReplicationConfiguration
	if c.IsSet(FlagClusters) {
		clusterStr := c.String(FlagClusters)
		clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
			ClusterName: clusterStr,
		})
		for _, clusterStr := range c.Args() {
			clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
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
	err = d.registerDomain(ctx, request)
	if err != nil {
		if _, ok := err.(*serviceerror.DomainAlreadyExists); !ok {
			ErrorAndExit("Register Domain operation failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s already registered.", domainName), err)
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
		replicationConfig := &commonproto.DomainReplicationConfiguration{
			ActiveClusterName: activeCluster,
		}
		updateRequest = &workflowservice.UpdateDomainRequest{
			Name:                     domainName,
			ReplicationConfiguration: replicationConfig,
		}
	} else {
		resp, err := d.describeDomain(ctx, &workflowservice.DescribeDomainRequest{
			Name: domainName,
		})
		if err != nil {
			if _, ok := err.(*serviceerror.NotFound); !ok {
				ErrorAndExit("Operation UpdateDomain failed.", err)
			} else {
				ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), err)
			}
			return
		}

		description := resp.DomainInfo.GetDescription()
		ownerEmail := resp.DomainInfo.GetOwnerEmail()
		retentionDays := resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays()
		emitMetric := resp.Configuration.GetEmitMetric().GetValue()
		var clusters []*commonproto.ClusterReplicationConfiguration

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
		if c.IsSet(FlagClusters) {
			clusterStr := c.String(FlagClusters)
			clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
				ClusterName: clusterStr,
			})
			for _, clusterStr := range c.Args() {
				clusters = append(clusters, &commonproto.ClusterReplicationConfiguration{
					ClusterName: clusterStr,
				})
			}
		}

		var binBinaries *commonproto.BadBinaries
		if c.IsSet(FlagAddBadBinary) {
			if !c.IsSet(FlagReason) {
				ErrorAndExit("Must provide a reason.", nil)
			}
			binChecksum := c.String(FlagAddBadBinary)
			reason := c.String(FlagReason)
			operator := getCurrentUserFromEnv()
			binBinaries = &commonproto.BadBinaries{
				Binaries: map[string]*commonproto.BadBinaryInfo{
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

		updateInfo := &commonproto.UpdateDomainInfo{
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        domainData,
		}
		updateConfig := &commonproto.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: retentionDays,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
			HistoryArchivalURI:                     c.String(FlagHistoryArchivalURI),
			VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
			VisibilityArchivalURI:                  c.String(FlagVisibilityArchivalURI),
			BadBinaries:                            binBinaries,
		}
		replicationConfig := &commonproto.DomainReplicationConfiguration{
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
	err := d.updateDomain(ctx, updateRequest)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			ErrorAndExit("Operation UpdateDomain failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), err)
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
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			ErrorAndExit("Operation DescribeDomain failed.", err)
		}
		ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), err)
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
) error {
	if d.frontendClient != nil {
		_, err := d.frontendClient.RegisterDomain(ctx, request)
		return err
	}

	_, err := d.domainHandler.RegisterDomain(ctx, request)
	return err
}

func (d *domainCLIImpl) updateDomain(
	ctx context.Context,
	request *workflowservice.UpdateDomainRequest,
) error {
	if d.frontendClient != nil {
		_, err := d.frontendClient.UpdateDomain(ctx, request)
		return err
	}

	_, err := d.domainHandler.UpdateDomain(ctx, request)
	return err
}

func (d *domainCLIImpl) describeDomain(
	ctx context.Context,
	request *workflowservice.DescribeDomainRequest,
) (*workflowservice.DescribeDomainResponse, error) {

	if d.frontendClient != nil {
		return d.frontendClient.DescribeDomain(ctx, request)
	}

	resp, err := d.domainHandler.DescribeDomain(ctx, request)
	return resp, err
}

func clustersToString(clusters []*commonproto.ClusterReplicationConfiguration) string {
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
