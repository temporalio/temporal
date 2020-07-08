// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	namespacepb "go.temporal.io/temporal-proto/namespace/v1"
	replicationpb "go.temporal.io/temporal-proto/replication/v1"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	"go.temporal.io/server/common/namespace"
)

type (
	namespaceCLIImpl struct {
		// used when making RPC call to frontend service``
		frontendClient workflowservice.WorkflowServiceClient

		// act as admin to modify namespace in DB directly
		namespaceHandler namespace.Handler
	}
)

// newNamespaceCLI creates a namespace CLI
func newNamespaceCLI(
	c *cli.Context,
	isAdminMode bool,
) *namespaceCLIImpl {

	var frontendClient workflowservice.WorkflowServiceClient
	var namespaceHandler namespace.Handler
	if !isAdminMode {
		frontendClient = initializeFrontendClient(c)
	} else {
		namespaceHandler = initializeAdminNamespaceHandler(c)
	}
	return &namespaceCLIImpl{
		frontendClient:   frontendClient,
		namespaceHandler: namespaceHandler,
	}
}

// RegisterNamespace register a namespace
func (d *namespaceCLIImpl) RegisterNamespace(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)

	description := c.String(FlagDescription)
	ownerEmail := c.String(FlagOwnerEmail)
	retentionDays := defaultNamespaceRetentionDays

	if c.IsSet(FlagRetentionDays) {
		retentionDays = c.Int(FlagRetentionDays)
	}
	securityToken := c.String(FlagSecurityToken)
	var err error

	var isGlobalNamespace bool
	if c.IsSet(FlagIsGlobalNamespace) {
		isGlobalNamespace, err = strconv.ParseBool(c.String(FlagIsGlobalNamespace))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagIsGlobalNamespace), err)
		}
	}

	namespaceData := map[string]string{}
	if c.IsSet(FlagNamespaceData) {
		namespaceDataStr := getRequiredOption(c, FlagNamespaceData)
		namespaceData, err = parseNamespaceDataKVs(namespaceDataStr)
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagNamespaceData), err)
		}
	}
	if len(requiredNamespaceDataKeys) > 0 {
		err = checkRequiredNamespaceDataKVs(namespaceData)
		if err != nil {
			ErrorAndExit("Namespace data missed required data.", err)
		}
	}

	var activeClusterName string
	if c.IsSet(FlagActiveClusterName) {
		activeClusterName = c.String(FlagActiveClusterName)
	}

	var clusters []*replicationpb.ClusterReplicationConfig
	if c.IsSet(FlagClusters) {
		clusterStr := c.String(FlagClusters)
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterStr,
		})
		for _, clusterStr := range c.Args() {
			clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
				ClusterName: clusterStr,
			})
		}
	}

	request := &workflowservice.RegisterNamespaceRequest{
		Name:                                   namespace,
		Description:                            description,
		OwnerEmail:                             ownerEmail,
		Data:                                   namespaceData,
		WorkflowExecutionRetentionPeriodInDays: int32(retentionDays),
		Clusters:                               clusters,
		ActiveClusterName:                      activeClusterName,
		SecurityToken:                          securityToken,
		HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
		HistoryArchivalUri:                     c.String(FlagHistoryArchivalURI),
		VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
		VisibilityArchivalUri:                  c.String(FlagVisibilityArchivalURI),
		IsGlobalNamespace:                      isGlobalNamespace,
	}

	ctx, cancel := newContext(c)
	defer cancel()
	err = d.registerNamespace(ctx, request)
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			ErrorAndExit("Register namespace operation failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Namespace %s already registered.", namespace), err)
		}
	} else {
		fmt.Printf("Namespace %s successfully registered.\n", namespace)
	}
}

// UpdateNamespace updates a namespace
func (d *namespaceCLIImpl) UpdateNamespace(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)

	var updateRequest *workflowservice.UpdateNamespaceRequest
	ctx, cancel := newContext(c)
	defer cancel()

	if c.IsSet(FlagActiveClusterName) {
		activeCluster := c.String(FlagActiveClusterName)
		fmt.Printf("Will set active cluster name to: %s, other flag will be omitted.\n", activeCluster)
		replicationConfig := &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
		}
		updateRequest = &workflowservice.UpdateNamespaceRequest{
			Name:              namespace,
			ReplicationConfig: replicationConfig,
		}
	} else {
		resp, err := d.describeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Name: namespace,
		})
		if err != nil {
			if _, ok := err.(*serviceerror.NotFound); !ok {
				ErrorAndExit("Operation UpdateNamespace failed.", err)
			} else {
				ErrorAndExit(fmt.Sprintf("Namespace %s does not exist.", namespace), err)
			}
			return
		}

		description := resp.NamespaceInfo.GetDescription()
		ownerEmail := resp.NamespaceInfo.GetOwnerEmail()
		retentionDays := resp.Config.GetWorkflowExecutionRetentionPeriodInDays()
		emitMetric := resp.Config.GetEmitMetric().GetValue()
		var clusters []*replicationpb.ClusterReplicationConfig

		if c.IsSet(FlagDescription) {
			description = c.String(FlagDescription)
		}
		if c.IsSet(FlagOwnerEmail) {
			ownerEmail = c.String(FlagOwnerEmail)
		}
		namespaceData := map[string]string{}
		if c.IsSet(FlagNamespaceData) {
			namespaceDataStr := c.String(FlagNamespaceData)
			namespaceData, err = parseNamespaceDataKVs(namespaceDataStr)
			if err != nil {
				ErrorAndExit("Namespace data format is invalid.", err)
			}
		}
		if c.IsSet(FlagRetentionDays) {
			retentionDays = int32(c.Int(FlagRetentionDays))
		}
		if c.IsSet(FlagClusters) {
			clusterStr := c.String(FlagClusters)
			clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
				ClusterName: clusterStr,
			})
			for _, clusterStr := range c.Args() {
				clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
					ClusterName: clusterStr,
				})
			}
		}

		var binBinaries *namespacepb.BadBinaries
		if c.IsSet(FlagAddBadBinary) {
			if !c.IsSet(FlagReason) {
				ErrorAndExit("Must provide a reason.", nil)
			}
			binChecksum := c.String(FlagAddBadBinary)
			reason := c.String(FlagReason)
			operator := getCurrentUserFromEnv()
			binBinaries = &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{
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

		updateInfo := &namespacepb.UpdateNamespaceInfo{
			Description: description,
			OwnerEmail:  ownerEmail,
			Data:        namespaceData,
		}
		updateConfig := &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionPeriodInDays: retentionDays,
			EmitMetric:                             &types.BoolValue{Value: emitMetric},
			HistoryArchivalStatus:                  archivalStatus(c, FlagHistoryArchivalStatus),
			HistoryArchivalUri:                     c.String(FlagHistoryArchivalURI),
			VisibilityArchivalStatus:               archivalStatus(c, FlagVisibilityArchivalStatus),
			VisibilityArchivalUri:                  c.String(FlagVisibilityArchivalURI),
			BadBinaries:                            binBinaries,
		}
		replicationConfig := &replicationpb.NamespaceReplicationConfig{
			Clusters: clusters,
		}
		updateRequest = &workflowservice.UpdateNamespaceRequest{
			Name:              namespace,
			UpdateInfo:        updateInfo,
			Config:            updateConfig,
			ReplicationConfig: replicationConfig,
			DeleteBadBinary:   badBinaryToDelete,
		}
	}

	securityToken := c.String(FlagSecurityToken)
	updateRequest.SecurityToken = securityToken
	err := d.updateNamespace(ctx, updateRequest)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			ErrorAndExit("Operation UpdateNamespace failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Namespace %s does not exist.", namespace), err)
		}
	} else {
		fmt.Printf("Namespace %s successfully updated.\n", namespace)
	}
}

// DescribeNamespace updates a namespace
func (d *namespaceCLIImpl) DescribeNamespace(c *cli.Context) {
	namespace := c.GlobalString(FlagNamespace)
	namespaceID := c.String(FlagNamespaceID)

	if namespaceID == "" && namespace == "" {
		ErrorAndExit("At least namespaceId or namespace must be provided.", nil)
	}
	ctx, cancel := newContext(c)
	defer cancel()
	resp, err := d.describeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
		Id:   namespaceID,
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			ErrorAndExit("Operation DescribeNamespace failed.", err)
		}
		ErrorAndExit(fmt.Sprintf("Namespace %s does not exist.", namespace), err)
	}

	printNamespace(resp)
}

func printNamespace(resp *workflowservice.DescribeNamespaceResponse) {
	var formatStr = "Name: %v\nId: %v\nDescription: %v\nOwnerEmail: %v\nNamespaceData: %#v\nStatus: %v\nRetentionInDays: %v\n" +
		"EmitMetrics: %v\nActiveClusterName: %v\nClusters: %v\nHistoryArchivalStatus: %v\n"
	descValues := []interface{}{
		resp.NamespaceInfo.GetName(),
		resp.NamespaceInfo.GetId(),
		resp.NamespaceInfo.GetDescription(),
		resp.NamespaceInfo.GetOwnerEmail(),
		resp.NamespaceInfo.Data,
		resp.NamespaceInfo.GetStatus(),
		resp.Config.GetWorkflowExecutionRetentionPeriodInDays(),
		resp.Config.GetEmitMetric().GetValue(),
		resp.ReplicationConfig.GetActiveClusterName(),
		clustersToString(resp.ReplicationConfig.Clusters),
		resp.Config.GetHistoryArchivalStatus().String(),
	}
	if resp.Config.GetHistoryArchivalUri() != "" {
		formatStr = formatStr + "HistoryArchivalURI: %v\n"
		descValues = append(descValues, resp.Config.GetHistoryArchivalUri())
	}
	formatStr = formatStr + "VisibilityArchivalStatus: %v\n"
	descValues = append(descValues, resp.Config.GetVisibilityArchivalStatus().String())
	if resp.Config.GetVisibilityArchivalUri() != "" {
		formatStr = formatStr + "VisibilityArchivalURI: %v\n"
		descValues = append(descValues, resp.Config.GetVisibilityArchivalUri())
	}
	fmt.Printf(formatStr, descValues...)
	if resp.Config.BadBinaries != nil {
		fmt.Println("Bad binaries to reset:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetBorder(true)
		table.SetColumnSeparator("|")
		header := []string{"Binary Checksum", "Operator", "Start Time", "Reason"}
		headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
		table.SetHeader(header)
		table.SetHeaderColor(headerColor...)
		for cs, bin := range resp.Config.BadBinaries.Binaries {
			row := []string{cs}
			row = append(row, bin.GetOperator())
			row = append(row, time.Unix(0, bin.GetCreateTimeNano()).String())
			row = append(row, bin.GetReason())
			table.Append(row)
		}
		table.Render()
	}
}

// ListNamespaces list all namespaces
func (d *namespaceCLIImpl) ListNamespaces(c *cli.Context) {
	for _, ns := range d.getAllNamespaces(c) {
		printNamespace(ns)
	}
}

func (d *namespaceCLIImpl) getAllNamespaces(c *cli.Context) []*workflowservice.DescribeNamespaceResponse {
	var res []*workflowservice.DescribeNamespaceResponse
	pagesize := int32(200)
	var token []byte
	ctx, cancel := newContext(c)
	defer cancel()
	for more := true; more; more = len(token) > 0 {
		listRequest := &workflowservice.ListNamespacesRequest{
			PageSize:      pagesize,
			NextPageToken: token,
		}
		listResp, err := d.listNamespaces(ctx, listRequest)
		if err != nil {
			ErrorAndExit("Error when list namespaces info", err)
		}
		token = listResp.GetNextPageToken()
		res = append(res, listResp.GetNamespaces()...)
	}
	return res
}

func (d *namespaceCLIImpl) listNamespaces(
	ctx context.Context,
	request *workflowservice.ListNamespacesRequest,
) (*workflowservice.ListNamespacesResponse, error) {

	if d.frontendClient != nil {
		return d.frontendClient.ListNamespaces(ctx, request)
	}

	return d.namespaceHandler.ListNamespaces(ctx, request)
}

func (d *namespaceCLIImpl) registerNamespace(
	ctx context.Context,
	request *workflowservice.RegisterNamespaceRequest,
) error {
	if d.frontendClient != nil {
		_, err := d.frontendClient.RegisterNamespace(ctx, request)
		return err
	}

	_, err := d.namespaceHandler.RegisterNamespace(ctx, request)
	return err
}

func (d *namespaceCLIImpl) updateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
) error {
	if d.frontendClient != nil {
		_, err := d.frontendClient.UpdateNamespace(ctx, request)
		return err
	}

	_, err := d.namespaceHandler.UpdateNamespace(ctx, request)
	return err
}

func (d *namespaceCLIImpl) describeNamespace(
	ctx context.Context,
	request *workflowservice.DescribeNamespaceRequest,
) (*workflowservice.DescribeNamespaceResponse, error) {

	if d.frontendClient != nil {
		return d.frontendClient.DescribeNamespace(ctx, request)
	}

	resp, err := d.namespaceHandler.DescribeNamespace(ctx, request)
	return resp, err
}

func clustersToString(clusters []*replicationpb.ClusterReplicationConfig) string {
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

func archivalStatus(c *cli.Context, statusFlagName string) enumspb.ArchivalStatus {
	if c.IsSet(statusFlagName) {
		switch c.String(statusFlagName) {
		case "disabled":
			return enumspb.ARCHIVAL_STATUS_DISABLED
		case "enabled":
			return enumspb.ARCHIVAL_STATUS_ENABLED
		default:
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", statusFlagName), errors.New("invalid status, valid values are \"disabled\" and \"enabled\""))
		}
	}
	return enumspb.ARCHIVAL_STATUS_UNSPECIFIED
}
