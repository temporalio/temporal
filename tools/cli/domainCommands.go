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
	serviceFrontend "github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/domain"
	"github.com/urfave/cli"
	s "go.uber.org/cadence/.gen/go/shared"
)

type (
	domainCLIImpl struct {
		// used when making RPC call to frontend service
		frontendClient serviceFrontend.Interface

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
	emitMetric := false
	var err error
	if c.IsSet(FlagEmitMetric) {
		emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagEmitMetric), err)
		}
	}
	var isGlobalDomainPtr *bool
	if c.IsSet(FlagIsGlobalDomain) {
		isGlobalDomain, err := strconv.ParseBool(c.String(FlagIsGlobalDomain))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagIsGlobalDomain), err)
		}
		isGlobalDomainPtr = common.BoolPtr(isGlobalDomain)
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

	var activeClusterName *string
	if c.IsSet(FlagActiveClusterName) {
		activeClusterName = common.StringPtr(c.String(FlagActiveClusterName))
	}

	var clusters []*shared.ClusterReplicationConfiguration
	if c.IsSet(FlagClusters) {
		clusterStr := c.String(FlagClusters)
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterStr),
		})
		for _, clusterStr := range c.Args() {
			clusters = append(clusters, &shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterStr),
			})
		}
	}

	request := &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(ownerEmail),
		Data:                                   domainData,
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      activeClusterName,
		SecurityToken:                          common.StringPtr(securityToken),
		HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
		HistoryArchivalURI:                     common.StringPtr(c.String(FlagHistoryArchivalURI)),
		VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
		VisibilityArchivalURI:                  common.StringPtr(c.String(FlagVisibilityArchivalURI)),
		IsGlobalDomain:                         isGlobalDomainPtr,
	}

	ctx, cancel := newContext(c)
	defer cancel()
	err = d.registerDomain(ctx, request)
	if err != nil {
		if _, ok := err.(*s.DomainAlreadyExistsError); !ok {
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

	var updateRequest *shared.UpdateDomainRequest
	ctx, cancel := newContext(c)
	defer cancel()

	if c.IsSet(FlagActiveClusterName) {
		activeCluster := c.String(FlagActiveClusterName)
		fmt.Printf("Will set active cluster name to: %s, other flag will be omitted.\n", activeCluster)
		replicationConfig := &shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(activeCluster),
		}
		updateRequest = &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domainName),
			ReplicationConfiguration: replicationConfig,
		}
	} else {
		resp, err := d.describeDomain(ctx, &shared.DescribeDomainRequest{
			Name: common.StringPtr(domainName),
		})
		if err != nil {
			if _, ok := err.(*shared.EntityNotExistsError); !ok {
				ErrorAndExit("Operation UpdateDomain failed.", err)
			} else {
				ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), err)
			}
			return
		}

		description := resp.DomainInfo.GetDescription()
		ownerEmail := resp.DomainInfo.GetOwnerEmail()
		retentionDays := resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays()
		emitMetric := resp.Configuration.GetEmitMetric()
		var clusters []*shared.ClusterReplicationConfiguration

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
			clusters = append(clusters, &shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterStr),
			})
			for _, clusterStr := range c.Args() {
				clusters = append(clusters, &shared.ClusterReplicationConfiguration{
					ClusterName: common.StringPtr(clusterStr),
				})
			}
		}

		var binBinaries *shared.BadBinaries
		if c.IsSet(FlagAddBadBinary) {
			if !c.IsSet(FlagReason) {
				ErrorAndExit("Must provide a reason.", nil)
			}
			binChecksum := c.String(FlagAddBadBinary)
			reason := c.String(FlagReason)
			operator := getCurrentUserFromEnv()
			binBinaries = &shared.BadBinaries{
				Binaries: map[string]*shared.BadBinaryInfo{
					binChecksum: {
						Reason:   common.StringPtr(reason),
						Operator: common.StringPtr(operator),
					},
				},
			}
		}

		var badBinaryToDelete *string
		if c.IsSet(FlagRemoveBadBinary) {
			badBinaryToDelete = common.StringPtr(c.String(FlagRemoveBadBinary))
		}

		updateInfo := &shared.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(ownerEmail),
			Data:        domainData,
		}
		updateConfig := &shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
			EmitMetric:                             common.BoolPtr(emitMetric),
			HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
			HistoryArchivalURI:                     common.StringPtr(c.String(FlagHistoryArchivalURI)),
			VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
			VisibilityArchivalURI:                  common.StringPtr(c.String(FlagVisibilityArchivalURI)),
			BadBinaries:                            binBinaries,
		}
		replicationConfig := &shared.DomainReplicationConfiguration{
			Clusters: clusters,
		}
		updateRequest = &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domainName),
			UpdatedInfo:              updateInfo,
			Configuration:            updateConfig,
			ReplicationConfiguration: replicationConfig,
			DeleteBadBinary:          badBinaryToDelete,
		}
	}

	securityToken := c.String(FlagSecurityToken)
	updateRequest.SecurityToken = common.StringPtr(securityToken)
	_, err := d.updateDomain(ctx, updateRequest)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
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
	resp, err := d.describeDomain(ctx, &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
		UUID: common.StringPtr(domainID),
	})
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			ErrorAndExit("Operation DescribeDomain failed.", err)
		}
		ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), err)
	}

	var formatStr = "Name: %v\nUUID: %v\nDescription: %v\nOwnerEmail: %v\nDomainData: %v\nStatus: %v\nRetentionInDays: %v\n" +
		"EmitMetrics: %v\nActiveClusterName: %v\nClusters: %v\nHistoryArchivalStatus: %v\n"
	descValues := []interface{}{
		resp.DomainInfo.GetName(),
		resp.DomainInfo.GetUUID(),
		resp.DomainInfo.GetDescription(),
		resp.DomainInfo.GetOwnerEmail(),
		resp.DomainInfo.Data,
		resp.DomainInfo.GetStatus(),
		resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays(),
		resp.Configuration.GetEmitMetric(),
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
	request *shared.RegisterDomainRequest,
) error {

	if d.frontendClient != nil {
		return d.frontendClient.RegisterDomain(ctx, request)
	}

	return d.domainHandler.RegisterDomain(ctx, request)
}

func (d *domainCLIImpl) updateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
) (*shared.UpdateDomainResponse, error) {

	if d.frontendClient != nil {
		return d.frontendClient.UpdateDomain(ctx, request)
	}

	return d.domainHandler.UpdateDomain(ctx, request)
}

func (d *domainCLIImpl) describeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
) (*shared.DescribeDomainResponse, error) {

	if d.frontendClient != nil {
		return d.frontendClient.DescribeDomain(ctx, request)
	}

	return d.domainHandler.DescribeDomain(ctx, request)
}

func archivalStatus(c *cli.Context, statusFlagName string) *shared.ArchivalStatus {
	if c.IsSet(statusFlagName) {
		switch c.String(statusFlagName) {
		case "disabled":
			return common.ArchivalStatusPtr(shared.ArchivalStatusDisabled)
		case "enabled":
			return common.ArchivalStatusPtr(shared.ArchivalStatusEnabled)
		default:
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", statusFlagName), errors.New("invalid status, valid values are \"disabled\" and \"enabled\""))
		}
	}
	return nil
}

func clustersToString(clusters []*shared.ClusterReplicationConfiguration) string {
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
