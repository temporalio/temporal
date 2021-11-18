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
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/adminservice/v1"
	clispb "go.temporal.io/server/api/cli/v1"
)

const (
	addSearchAttributesTimeout = 30 * time.Second
)

// AdminAddSearchAttributes to add search attributes
func AdminAddSearchAttributes(c *cli.Context) {
	names := getRequiredStringSliceOption(c, FlagName)
	typeStrs := getRequiredStringSliceOption(c, FlagType)

	if len(names) != len(typeStrs) {
		ErrorAndExit("Number of names and types options should be the same.", nil)
	}

	adminClient := cFactory.AdminClient(c)
	existingSearchAttributes, err := getSearchAttributes(c, adminClient)
	if err != nil {
		ErrorAndExit("Unable to get existing search attributes.", err)
	}

	searchAttributes := make(map[string]enumspb.IndexedValueType, len(typeStrs))
	for i := 0; i < len(typeStrs); i++ {
		typeStr := typeStrs[i]

		// To support backwards compatibility "String" is an alias to "Text".
		// TODO: Remove this code in 1 year (after 10/15/22).
		if strings.EqualFold(typeStr, "String") {
			color.HiYellow("Search attribute %s: String type is deprecated, use Text instead.", names[i])
			typeStr = "Text"
		}

		typeInt, err := stringToEnum(typeStr, enumspb.IndexedValueType_value)
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Unable to parse search attribute type: %s", typeStr), err)
		}
		existingSearchAttributeType, searchAttributeExists := existingSearchAttributes.CustomAttributes[names[i]]
		if !searchAttributeExists {
			searchAttributes[names[i]] = enumspb.IndexedValueType(typeInt)
			continue
		}
		if existingSearchAttributeType != enumspb.IndexedValueType(typeInt) {
			ErrorAndExit(fmt.Sprintf("Search attribute %s already exists and has different type %s.", names[i], existingSearchAttributeType), nil)
		}
	}

	if len(searchAttributes) == 0 {
		color.HiYellow("Search attributes already exist.")
		return
	}

	if c.Bool(FlagSkipSchemaUpdate) {
		promptMsg := color.RedString("This command will only modify search attributes metadata. You need to modify Elasticsearch schema manually prior to running this command. Continue? Y/N")
		prompt(promptMsg, c.GlobalBool(FlagAutoConfirm))
	}

	// ask user for confirmation
	promptMsg := fmt.Sprintf(
		"You are about to add search attributes %s. Continue? Y/N",
		color.YellowString(strings.TrimLeft(fmt.Sprintf("%v", searchAttributes), "map")),
	)
	prompt(promptMsg, c.GlobalBool(FlagAutoConfirm))

	request := &adminservice.AddSearchAttributesRequest{
		SearchAttributes: searchAttributes,
		IndexName:        c.String(FlagElasticsearchIndex),
		SkipSchemaUpdate: c.Bool(FlagSkipSchemaUpdate),
	}

	ctx, cancel := newContextWithTimeout(c, addSearchAttributesTimeout)
	defer cancel()
	_, err = adminClient.AddSearchAttributes(ctx, request)
	if err != nil {
		ErrorAndExit("Unable to add search attributes.", err)
	}

	resp, err := getSearchAttributes(c, adminClient)
	if err != nil {
		ErrorAndExit("Search attributes have been added successfully but there was an error while reading them back.", err)
	}
	printSearchAttributesResponse(resp, c.String(FlagElasticsearchIndex))
	color.HiGreen("Search attributes have been added successfully.")
}

// AdminRemoveSearchAttributes to add search attributes
func AdminRemoveSearchAttributes(c *cli.Context) {
	names := getRequiredStringSliceOption(c, FlagName)

	// ask user for confirmation
	promptMsg := fmt.Sprintf(
		"You are about to remove search attributes %s. Continue? Y/N",
		color.YellowString(fmt.Sprintf("%v", names)),
	)
	prompt(promptMsg, c.GlobalBool(FlagAutoConfirm))

	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	request := &adminservice.RemoveSearchAttributesRequest{
		SearchAttributes: names,
		IndexName:        c.String(FlagElasticsearchIndex),
	}

	_, err := adminClient.RemoveSearchAttributes(ctx, request)
	if err != nil {
		ErrorAndExit("Unable to remove search attributes.", err)
	}

	resp, err := getSearchAttributes(c, adminClient)
	if err != nil {
		ErrorAndExit("Search attributes have been removed successfully but there was an error while reading them back.", err)
	}
	printSearchAttributesResponse(resp, c.String(FlagElasticsearchIndex))
	color.HiGreen("Search attributes have been removed successfully.")
}

// AdminGetSearchAttributes to print search attributes
func AdminGetSearchAttributes(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)
	resp, err := getSearchAttributes(c, adminClient)
	if err != nil {
		ErrorAndExit("Unable to get search attributes.", err)
	}
	if c.Bool(FlagPrintJSON) {
		printSearchAttributesResponseJSON(resp, c.String(FlagElasticsearchIndex))
		return
	}
	printSearchAttributesResponse(resp, c.String(FlagElasticsearchIndex))
}

func getSearchAttributes(c *cli.Context, adminClient adminservice.AdminServiceClient) (*adminservice.GetSearchAttributesResponse, error) {
	ctx, cancel := newContext(c)
	defer cancel()
	request := &adminservice.GetSearchAttributesRequest{
		IndexName: c.String(FlagElasticsearchIndex),
	}
	return adminClient.GetSearchAttributes(ctx, request)
}

func printSearchAttributesResponse(resp *adminservice.GetSearchAttributesResponse, indexName string) {
	if indexName != "" {
		indexName = fmt.Sprintf(" (%s)", indexName)
	}
	printSearchAttributes(resp.GetCustomAttributes(), fmt.Sprintf("Custom search attributes%s", indexName))
	printSearchAttributes(resp.GetSystemAttributes(), "System search attributes")

	color.Cyan("Storage mappings%s:\n", indexName)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Column name", "Column type"})
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	var rows [][]string
	for colName, colType := range resp.GetMapping() {
		rows = append(rows, []string{
			colName,
			colType,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0] < rows[j][0]
	})
	table.AppendBulk(rows)
	table.Render()

	color.Cyan("Workflow info:\n")
	prettyPrintJSONObject(&clispb.WorkflowExecutionInfo{
		Execution: resp.GetAddWorkflowExecutionInfo().GetExecution(),
		StartTime: resp.GetAddWorkflowExecutionInfo().GetStartTime(),
		CloseTime: resp.GetAddWorkflowExecutionInfo().GetCloseTime(),
		Status:    resp.GetAddWorkflowExecutionInfo().GetStatus(),
	})
}

func printSearchAttributes(searchAttributes map[string]enumspb.IndexedValueType, header string) {
	var rows [][]string
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Type"})
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)

	color.Cyan("%s:\n", header)
	for saName, saType := range searchAttributes {
		rows = append(rows,
			[]string{
				saName,
				saType.String(),
			})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0] < rows[j][0]
	})
	table.AppendBulk(rows)
	table.Render()
}

func printSearchAttributesResponseJSON(resp *adminservice.GetSearchAttributesResponse, indexName string) {
	json := &clispb.AddSearchAttributesResponse{
		IndexName:              indexName,
		CustomSearchAttributes: make(map[string]string, len(resp.CustomAttributes)),
		SystemSearchAttributes: make(map[string]string, len(resp.SystemAttributes)),
		Mapping:                resp.GetMapping(),
		AddWorkflowExecutionInfo: &clispb.WorkflowExecutionInfo{
			Execution: resp.GetAddWorkflowExecutionInfo().GetExecution(),
			StartTime: resp.GetAddWorkflowExecutionInfo().GetStartTime(),
			CloseTime: resp.GetAddWorkflowExecutionInfo().GetCloseTime(),
			Status:    resp.GetAddWorkflowExecutionInfo().GetStatus(),
		},
	}

	for name, value := range resp.GetCustomAttributes() {
		json.CustomSearchAttributes[name] = value.String()
	}

	for name, value := range resp.GetSystemAttributes() {
		json.SystemSearchAttributes[name] = value.String()
	}

	prettyPrintJSONObject(json)
}
