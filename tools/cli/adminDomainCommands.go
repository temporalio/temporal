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
	"errors"
	"fmt"
	"strconv"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/urfave/cli"
)

/**
TODO: Move changes in this file to commands.go and delete this file once archival is done

This file contains a copy of the domain level commands in commands.go.
These commands additionally provide options to interact with domain archival config.
Once archival is finished this file should be deleted and archival config changes should be ported to commands.go.
*/

// AdminRegisterDomain register a domain
func AdminRegisterDomain(c *cli.Context) {
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

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
		Name:                                   common.StringPtr(domain),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(ownerEmail),
		Data:                                   domainData,
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
		SecurityToken:                          common.StringPtr(securityToken),
		ArchivalStatus:                         archivalStatus(c),
		ArchivalBucketName:                     common.StringPtr(c.String(FlagArchivalBucketName)),
	}

	ctx, cancel := newContext()
	defer cancel()
	err = frontendClient.RegisterDomain(ctx, request)
	if err != nil {
		if _, ok := err.(*shared.DomainAlreadyExistsError); !ok {
			ErrorAndExit("Register Domain operation failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s already registered.", domain), err)
		}
	} else {
		fmt.Printf("Domain %s successfully registered.\n", domain)
	}
}

// AdminUpdateDomain updates a domain
func AdminUpdateDomain(c *cli.Context) {
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	var updateRequest *shared.UpdateDomainRequest
	ctx, cancel := newContext()
	defer cancel()

	if c.IsSet(FlagActiveClusterName) {
		activeCluster := c.String(FlagActiveClusterName)
		fmt.Printf("Will set active cluster name to: %s, other flag will be omitted.\n", activeCluster)
		replicationConfig := &shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(activeCluster),
		}
		updateRequest = &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			ReplicationConfiguration: replicationConfig,
		}
	} else {
		req := &shared.DescribeDomainRequest{
			Name: common.StringPtr(domain),
		}
		resp, err := frontendClient.DescribeDomain(ctx, req)
		if err != nil {
			if _, ok := err.(*shared.EntityNotExistsError); !ok {
				ErrorAndExit("Operation UpdateDomain failed.", err)
			} else {
				ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domain), err)
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

		updateInfo := &shared.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(ownerEmail),
			Data:        domainData,
		}
		updateConfig := &shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
			EmitMetric:                             common.BoolPtr(emitMetric),
			ArchivalStatus:                         archivalStatus(c),
			ArchivalBucketName:                     common.StringPtr(c.String(FlagArchivalBucketName)),
		}
		replicationConfig := &shared.DomainReplicationConfiguration{
			Clusters: clusters,
		}
		updateRequest = &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			UpdatedInfo:              updateInfo,
			Configuration:            updateConfig,
			ReplicationConfiguration: replicationConfig,
		}
	}

	securityToken := c.String(FlagSecurityToken)
	updateRequest.SecurityToken = common.StringPtr(securityToken)
	_, err := frontendClient.UpdateDomain(ctx, updateRequest)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			ErrorAndExit("Operation UpdateDomain failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domain), err)
		}
	} else {
		fmt.Printf("Domain %s successfully updated.\n", domain)
	}
}

// AdminDescribeDomain updates a domain
func AdminDescribeDomain(c *cli.Context) {
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	ctx, cancel := newContext()
	defer cancel()
	req := &shared.DescribeDomainRequest{
		Name: common.StringPtr(domain),
	}
	resp, err := frontendClient.DescribeDomain(ctx, req)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			ErrorAndExit("Operation DescribeDomain failed.", err)
		}
		ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domain), err)
	}

	archivalStatus := resp.Configuration.GetArchivalStatus()
	var formatStr = "Name: %v\nDescription: %v\nOwnerEmail: %v\nDomainData: %v\nStatus: %v\nRetentionInDays: %v\n" +
		"EmitMetrics: %v\nActiveClusterName: %v\nClusters: %v\nArchivalStatus: %v\n"
	descValues := []interface{}{
		resp.DomainInfo.GetName(),
		resp.DomainInfo.GetDescription(),
		resp.DomainInfo.GetOwnerEmail(),
		resp.DomainInfo.Data,
		resp.DomainInfo.GetStatus(),
		resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays(),
		resp.Configuration.GetEmitMetric(),
		resp.ReplicationConfiguration.GetActiveClusterName(),
		serverClustersToString(resp.ReplicationConfiguration.Clusters),
		archivalStatus.String(),
	}
	if resp.Configuration.GetArchivalBucketName() != "" {
		formatStr = formatStr + "BucketName: %v\n"
		descValues = append(descValues, resp.Configuration.GetArchivalBucketName())
	}
	if resp.Configuration.GetArchivalRetentionPeriodInDays() != 0 {
		formatStr = formatStr + "ArchivalRetentionInDays: %v\n"
		descValues = append(descValues, resp.Configuration.GetArchivalRetentionPeriodInDays())
	}
	if resp.Configuration.GetArchivalBucketOwner() != "" {
		formatStr = formatStr + "BucketOwner: %v\n"
		descValues = append(descValues, resp.Configuration.GetArchivalBucketOwner())
	}
	fmt.Printf(formatStr, descValues...)
}

func serverClustersToString(clusters []*shared.ClusterReplicationConfiguration) string {
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

func archivalStatus(c *cli.Context) *shared.ArchivalStatus {
	if c.IsSet(FlagArchivalStatus) {
		switch c.String(FlagArchivalStatus) {
		case "disabled":
			return common.ArchivalStatusPtr(shared.ArchivalStatusDisabled)
		case "enabled":
			return common.ArchivalStatusPtr(shared.ArchivalStatusEnabled)
		default:
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagArchivalStatus), errors.New("invalid status, options are {never_enabled, disabled, enabled}"))
		}
	}
	return nil
}
